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
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

load_dotenv()

# ---------------------------------------------------------------------------
# Global Logging & Environment Configuration
# ---------------------------------------------------------------------------
# Configure runtime logging for AWS CloudWatch ingestion
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Retrieve configuration parameters from Lambda environment variables
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

# Initialize persistent HTTP session with exponential backoff retry strategy
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Initialize AWS S3 client outside handler to reuse across warm invocations
s3_client = boto3.client("s3")

# Target API endpoints and authentication headers
DETAILS_URL = "https://api.themoviedb.org/3/movie/{movie_id}"
HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY}",
    "accept": "application/json",
}


def fetch_movie_detail(movie_id: int):
    """Retrieve full movie specifications, credits, and keywords for a movie ID.

    Args:
        movie_id: Unique TMDB identifier.

    Returns:
        Dictionary containing detailed movie metadata, or None if extraction failed.
    """
    # Append credits and keywords sub-resources to retrieve comprehensive entity data in one call
    params = {"append_to_response": "credits,keywords", "language": "en-US"}
    try:
        res = session.get(
            DETAILS_URL.format(movie_id=movie_id),
            params=params,
            headers=HEADERS,
            timeout=10,
        )
        if res.status_code == 200:
            return res.json()
    except requests.exceptions.RequestException as err:
        logger.error("Error fetching movie %s: %s", movie_id, err)
    return None


def lambda_handler(event, context):
    """Processes batch of records triggered directly by SQS."""
    # Extract incoming SQS batch records dispatched by the event source mapping
    records = event.get("Records", [])
    logger.info("Processing SQS batch of %s records", len(records))

    enriched_records = []
    utc_now = datetime.now(timezone.utc)

    # Iterate through each queued message in the received batch
    for item in records:
        # Deserialize message body payload to extract target movie identifier
        body = json.loads(item["body"])
        movie_id = body.get("movie_id")
        if not movie_id:
            continue

        detail = fetch_movie_detail(movie_id)
        if detail:
            # Enforce schema parity with historical backfill by filtering out adult and non-theatrical video entries
            if not detail.get("adult", False) and not detail.get("video", False):
                enriched_records.append(detail)

        # Enforce rate-limiting delay to prevent downstream API throttling
        time.sleep(0.05)

    # Persist batch payload to S3 Data Lake under date-partitioned paths
    if enriched_records:
        timestamp_str = utc_now.strftime("%Y%m%d_%H%M%S_%f")
        s3_key = (
            f"raw/daily/year={utc_now.year}/month={utc_now.month:02d}/day={utc_now.day:02d}/"
            f"movies_batch_{timestamp_str}.json"
        )
        try:
            # Stage serialized JSON batch directly into target S3 bucket partition
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(enriched_records, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
            logger.info("Uploaded %s records to %s", len(enriched_records), s3_key)
        except ClientError as err:
            logger.error("Failed to upload batch to S3: %s", err)
            # Re-raise exception to let SQS manage retry policy and route to DLQ if persistent
            raise err

    return {"statusCode": 200, "processed_count": len(enriched_records)}
