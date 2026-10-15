"""TMDB Incremental Ingestion - Consumer/Worker Lambda.

Processes movie IDs from SQS, pulls detailed metadata, and writes directly to S3.
"""

from datetime import datetime, timezone
import json
import logging
import os
import time
import boto3
from botocore.exceptions import ClientError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


# ===========================================================================
# STEP 0: RUNTIME CONFIGURATION & SHARED CLIENT INITIALIZATION
# ===========================================================================

# Configure structured runtime logging for AWS CloudWatch Ingestion and Metric Filters
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Extract core execution parameters from AWS Lambda environment variables
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

# Resilience Strategy:
# Implement connection pooling with exponential backoff and jitter to mitigate
# transient network drops and upstream 5xx errors from TMDB without overwhelming gateways.
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Performance Optimization:
# Initialize AWS S3 client outside the invocation handler to reuse TCP connection pool
# and IAM credential cache across warm Lambda container invocations.
s3_client = boto3.client("s3")

# Target API endpoints and authentication headers
DETAILS_URL = "https://api.themoviedb.org/3/movie/{movie_id}"
HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY}",
    "accept": "application/json",
}


def fetch_movie_detail(movie_id: int):
    """Retrieve full movie specifications, credits, and keywords for a movie ID.

    Leverages TMDB 'append_to_response' capability to consolidate three separate API calls
    (movie detail, cast/crew credits, and thematic keywords) into a single atomic HTTP GET,
    significantly reducing network round-trips and compute runtime.

    Args:
        movie_id: Unique TMDB entity identifier.

    Returns:
        dict | None: Deserialized JSON payload containing enriched movie metadata,
            or None if the request failed or encountered an unhandled network error.

    Raises:
        requests.exceptions.RequestException: Handled internally; logged and returns None
            to ensure individual entity errors do not fail the entire batch.
    """
    # -----------------------------------------------------------------------
    # STEP 1.1: QUERY PARAMETER SPECIFICATION
    # -----------------------------------------------------------------------
    # Business requirement: Fetch complete entity attributes in one call
    params = {"append_to_response": "credits,keywords", "language": "en-US"}
    try:
        # Enforce strict 10s socket timeout to avoid hanging the Lambda container
        res = session.get(
            DETAILS_URL.format(movie_id=movie_id),
            params=params,
            headers=HEADERS,
            timeout=10,
        )
        if res.status_code == 200:
            # -------------------------------------------------------------------
            # SCHEMA: TMDB /movie/{id}?append_to_response=credits,keywords
            # {
            #   "id": 550,
            #   "title": "Fight Club",
            #   "adult": false,
            #   "video": false,
            #   "budget": 63000000,
            #   "revenue": 100853753,
            #   "release_date": "1999-10-15",
            #   "genres": [{"id": 18, "name": "Drama"}],
            #   "credits": {
            #     "cast": [
            #       {"id": 287, "name": "Brad Pitt", "character": "Tyler Durden"}
            #     ],
            #     "crew": [
            #       {"id": 7467, "name": "David Fincher", "job": "Director"}
            #     ]
            #   },
            #   "keywords": {
            #     "keywords": [{"id": 825, "name": "support group"}]
            #   }
            # }
            # -------------------------------------------------------------------
            return res.json()
    except requests.exceptions.RequestException as err:
        # Soft-fail: Log entity-level extraction failure to allow remaining items in batch to proceed
        logger.error("Error fetching movie %s: %s", movie_id, err)
    return None


def lambda_handler(event, context):
    """Processes batch of records triggered directly by SQS.

    Consumes messages dispatched via AWS SQS Event Source Mapping (ESM), extracts target
    movie IDs, enriches them against upstream TMDB API, and sinks the curated batch directly
    into an Amazon S3 Data Lake under Hive-compatible temporal partitions.

    Args:
        event: Standard AWS SQS Event Source Mapping payload containing batch records.
        context: AWS Lambda runtime context object.

    Returns:
        dict: Ingestion execution summary containing HTTP status code and processed count.

    Raises:
        json.JSONDecodeError: If an incoming SQS message body cannot be parsed.
        botocore.exceptions.ClientError: If S3 PutObject fails (bubbles up to trigger SQS
            retry policy and Dead-Letter Queue / DLQ redrive).
    """
    # -----------------------------------------------------------------------
    # STEP 1: PARSE SQS EVENT SOURCE MAPPING (ESM) PAYLOAD
    # -----------------------------------------------------------------------
    # -----------------------------------------------------------------------
    # SCHEMA: Incoming AWS SQS Event Record Structure
    # {
    #   "Records": [
    #     {
    #       "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
    #       "receiptHandle": "AQEBwJnKyr...",
    #       "body": "{\"movie_ids\": [550, 551, 552]}",
    #       "attributes": {"ApproximateReceiveCount": "1"},
    #       "eventSource": "aws:sqs",
    #       "awsRegion": "us-east-1"
    #     }
    #   ]
    # }
    # -----------------------------------------------------------------------
    records = event.get("Records", [])
    logger.info("Processing SQS batch of %s records", len(records))

    enriched_records = []
    # Anchor partition path timestamp in UTC to maintain consistency across global runs
    utc_now = datetime.now(timezone.utc)

    # -----------------------------------------------------------------------
    # STEP 2: ITERATE SQS MESSAGES & ENRICH ENTITY METADATA
    # -----------------------------------------------------------------------
    for item in records:
        body = json.loads(item["body"])
        movie_ids = body.get("movie_ids", [])

        for movie_id in movie_ids:
            detail = fetch_movie_detail(movie_id)
            if detail:
                # Business Compliance & Quality Filter:
                # 1. Exclude adult content to comply with downstream analytics governance.
                # 2. Exclude straight-to-video / promo entries ('video: true') to preserve
                #    catalog parity with historical box-office backfills.
                if not detail.get("adult", False) and not detail.get("video", False):
                    enriched_records.append(detail)

            # Upstream Rate Limiting Guard:
            # TMDB allows ~40-50 requests/second. Adding a 50ms pause per invocation thread
            # prevents 429 HTTP Too Many Requests when multiple Lambda concurrent workers spin up.
            time.sleep(0.05)

    # -----------------------------------------------------------------------
    # STEP 3: ATOMIC DATA LAKE SINK (AMAZON S3 RAW ZONE)
    # -----------------------------------------------------------------------
    if enriched_records:
        # Microsecond precision prevents collision between concurrent Lambda worker batches
        timestamp_str = utc_now.strftime("%Y%m%d_%H%M%S_%f")

        # Hive-compatible date partitioning allows AWS Athena / Glue Crawlers to perform partition pruning
        s3_key = (
            f"raw/daily/year={utc_now.year}/month={utc_now.month:02d}/day={utc_now.day:02d}/"
            f"movies_batch_{timestamp_str}.json"
        )
        try:
            # Direct multi-record JSON staging into S3 Data Lake
            # ---------------------------------------------------------------
            # SCHEMA: S3 Persisted Batch Payload (Array of Enriched Objects)
            # [
            #   {
            #     "id": 550,
            #     "title": "Fight Club",
            #     "adult": false,
            #     "credits": {...},
            #     "keywords": {...}
            #   },
            #   ...
            # ]
            # ---------------------------------------------------------------
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(enriched_records, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
            logger.info("Uploaded %s records to %s", len(enriched_records), s3_key)
        except ClientError as err:
            logger.error("Failed to upload batch to S3: %s", err)
            # Architectural Fault Tolerance:
            # Re-raise the exception to tell AWS Lambda that message processing failed.
            # This causes SQS not to delete the message, allowing automated SQS retries
            # and eventual forwarding to the Dead-Letter Queue (DLQ) if failures persist.
            raise err

    # -----------------------------------------------------------------------
    # SCHEMA: Consumer Lambda Output Envelope
    # {
    #   "statusCode": 200,
    #   "processed_count": 48
    # }
    # -----------------------------------------------------------------------
    return {"statusCode": 200, "processed_count": len(enriched_records)}
