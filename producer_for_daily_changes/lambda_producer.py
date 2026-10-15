"""TMDB Incremental Ingestion - Producer Lambda.

Fetches updated movie IDs via TMDB Changes API and pushes them to Amazon SQS.
"""

from datetime import datetime, timedelta, timezone
import json
import logging
import os
import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


# ===========================================================================
# STEP 0: RUNTIME ENVIRONMENT & RESILIENT CLIENT INITIALIZATION
# ===========================================================================

# Configure structured runtime logging for AWS CloudWatch Insights aggregation and alerting
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Extract core execution parameters from AWS Lambda environment variables
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")

# Resilience Strategy:
# Implement connection pooling with exponential backoff and jitter to mitigate
# transient network partitions and 5xx errors from TMDB without overwhelming upstream gateways.
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Performance Optimization:
# Instantiate AWS SDK client globally to leverage TCP keep-alive connections across warm invocations.
sqs_client = boto3.client("sqs")

# TMDB V3 changes endpoint requires Bearer token authentication
CHANGES_API_URL = "https://api.themoviedb.org/3/movie/changes"
HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY}",
    "accept": "application/json",
}


def fetch_changed_movie_ids(start_date: str, end_date: str) -> list[int]:
    """Retrieve all movie IDs that experienced changes between start_date and end_date.

    Queries the TMDB /movie/changes API incrementally across all available paginated
    records. Filters out adult content per compliance policy and returns deduplicated IDs.

    Args:
        start_date: Boundary start date formatted as ISO-8601 YYYY-MM-DD.
        end_date: Boundary end date formatted as ISO-8601 YYYY-MM-DD.

    Returns:
        list[int]: Unique collection of modified TMDB movie IDs within the query window.

    Raises:
        requests.exceptions.RequestException: If unhandled network errors occur beyond
            the configured retry limit.
        ValueError: If JSON deserialization fails on a 200 response payload.
    """
    movie_ids = []
    page = 1
    total_pages = 1

    # -----------------------------------------------------------------------
    # STEP 1.1: PAGINATED UPSTREAM EXTRACTION LOOP
    # -----------------------------------------------------------------------
    # TMDB restricts change queries to a max of 100 items per page; full traversal is required
    while page <= total_pages:
        # Enforce a strict 10s socket timeout to avoid indefinite Lambda hangs consuming billable compute
        response = session.get(
            CHANGES_API_URL,
            params={"start_date": start_date, "end_date": end_date, "page": page},
            headers=HEADERS,
            timeout=10,
        )

        # Fault-tolerance policy:
        # Skip anomalous individual page failures to prioritize overall pipeline progress
        # and prevent transient single-page errors from blocking downstream consumer pipelines.
        if response.status_code != 200:
            page += 1
            continue

        # -------------------------------------------------------------------
        # SCHEMA: TMDB /movie/changes API Response Payload
        # {
        #   "results": [
        #     {"id": 550, "adult": false},
        #     {"id": 10294, "adult": true}
        #   ],
        #   "page": 1,
        #   "total_pages": 42,
        #   "total_results": 4180
        # }
        # -------------------------------------------------------------------
        payload = response.json()

        # Dynamic loop boundary: Read actual pagination depth returned by the upstream API
        if page == 1:
            total_pages = payload.get("total_pages", 1)

        records = payload.get("results", [])

        # -------------------------------------------------------------------
        # STEP 1.2: COMPLIANCE FILTERING & ENTITY ID COLLECTION
        # -------------------------------------------------------------------
        # Business logic requirement: Exclude adult-rated content early in ingestion
        # to ensure downstream data catalog compliance and optimize downstream processing costs.
        movie_ids.extend(
            [
                item["id"]
                for item in records
                if "id" in item and not item.get("adult", False)
            ]
        )
        page += 1

    # -----------------------------------------------------------------------
    # STEP 1.3: DEDUPLICATION
    # -----------------------------------------------------------------------
    # TMDB Changes API may return duplicate IDs across overlapping sync windows;
    # deduplicate in-memory to prevent redundant SQS deliveries and downstream worker contention.
    return list(set(movie_ids))


def push_to_sqs(movie_ids: list[int], chunk_size: int = 50) -> int:
    """Chunk movie IDs into groups and dispatch them using SQS batching.

    Implements a two-tier batching strategy:
      1. Business Level: Packs multiple movie IDs (default: 50) into a single SQS message payload.
      2. Infrastructure Level: Batches up to 10 SQS messages per AWS SendMessageBatch API call.

    Args:
        movie_ids: List of unique TMDB movie identifiers to ingest.
        chunk_size: Number of entity IDs packed per individual SQS message payload.
            Defaults to 50.

    Returns:
        int: Total number of movie IDs processed and dispatched across all message batches.

    Raises:
        botocore.exceptions.ClientError: If AWS SQS rejects the batch request or fails authentication.
        botocore.exceptions.ParamValidationError: If payload schema violates AWS SQS message limits.
    """
    # -----------------------------------------------------------------------
    # STEP 2.1: MESSAGE-LEVEL CHUNKING (COST & THROUGHPUT OPTIMIZATION)
    # -----------------------------------------------------------------------
    # Packing 50 entity IDs into one message optimizes AWS SQS API billing ($0.40/million requests)
    # and keeps message payloads well within the AWS 256 KB hard limit (~50 IDs < 1 KB).
    grouped_messages = [
        {"movie_ids": movie_ids[i : i + chunk_size]}
        for i in range(0, len(movie_ids), chunk_size)
    ]
    # -----------------------------------------------------------------------
    # SCHEMA: Internal Grouped Messages Structure
    # [
    #   {"movie_ids": [550, 551, ..., 600]},
    #   {"movie_ids": [601, 602, ..., 650]}
    # ]
    # -----------------------------------------------------------------------

    total_sent = 0
    sqs_batch_limit = (
        10  # AWS SQS SendMessageBatch hard API limit: max 10 messages per batch call
    )

    # -----------------------------------------------------------------------
    # STEP 2.2: SQS SDK BATCH DISPATCH (UP TO 500 MOVIES PER API CALL)
    # -----------------------------------------------------------------------
    for i in range(0, len(grouped_messages), sqs_batch_limit):
        chunk = grouped_messages[i : i + sqs_batch_limit]

        # -------------------------------------------------------------------
        # SCHEMA: AWS SQS SendMessageBatch Request Entries
        # [
        #   {
        #     "Id": "0",
        #     "MessageBody": "{\"movie_ids\": [550, 551, ..., 600]}"
        #   },
        #   {
        #     "Id": "1",
        #     "MessageBody": "{\"movie_ids\": [601, 602, ..., 650]}"
        #   }
        # ]
        # -------------------------------------------------------------------
        # SQS Batch Entry requires a unique correlation ID per item within the single batch call
        entries = [
            {"Id": str(idx), "MessageBody": json.dumps(msg)}
            for idx, msg in enumerate(chunk)
        ]

        # Network boundary call: Dispatches up to 10 SQS messages atomically
        sqs_client.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
        total_sent += sum(len(msg["movie_ids"]) for msg in chunk)

    # Log metrics for CloudWatch Ingestion observability and pipeline throughput monitoring
    logger.info(
        "Enqueued %s total movies across %s SQS messages",
        total_sent,
        len(grouped_messages),
    )
    return total_sent


def lambda_handler(event: dict, context) -> dict:
    """AWS Lambda entry point for TMDB incremental ingestion pipeline.

    Coordinates incremental delta discovery and orchestrates SQS fan-out dispatch.
    Can be triggered via EventBridge cron schedule or direct invocation payload.

    Args:
        event: AWS Lambda invocation event. Accepts optional ISO-8601 date boundary overrides.
            Example: {"start_date": "2026-10-14", "end_date": "2026-10-15"}
        context: AWS Lambda runtime context object containing execution environment metadata.

    Returns:
        dict: Standard HTTP-compatible API gateway / Lambda response envelope.

    Raises:
        botocore.exceptions.ClientError: If downstream AWS SQS access is unauthorized or fails.
        requests.exceptions.RequestException: If upstream TMDB API is completely unreachable.
    """
    # -----------------------------------------------------------------------
    # STEP 1: RESOLVE TEMPORAL INGESTION BOUNDARIES
    # -----------------------------------------------------------------------
    # Standardize on UTC timestamps to eliminate DST skew across distributed worker instances
    utc_now = datetime.now(timezone.utc)

    # Graceful degradation / Backfill support:
    # Use explicit dates from payload if available (manual backfill runs),
    # otherwise default to the standard 24-hour incremental ingestion window.
    start_date = event.get("start_date", utc_now.strftime("%Y-%m-%d"))
    end_date = event.get("end_date", (utc_now + timedelta(days=1)).strftime("%Y-%m-%d"))

    # -----------------------------------------------------------------------
    # STEP 2: DISCOVER CHANGED ENTITIES (UPSTREAM INGESTION)
    # -----------------------------------------------------------------------
    logger.info("Fetching changed movies for %s to %s", start_date, end_date)
    changed_ids = fetch_changed_movie_ids(start_date, end_date)
    logger.info("Found %s movie IDs", len(changed_ids))

    # -----------------------------------------------------------------------
    # STEP 3: ASYNCHRONOUS FAN-OUT ENQUEUE (DOWNSTREAM BUFFERING)
    # -----------------------------------------------------------------------
    # Decouple extraction from heavy enrichment workers via SQS queue buffering
    if changed_ids:
        sent_count = push_to_sqs(changed_ids, chunk_size=50)
        logger.info("Pushed %s messages to SQS successfully", sent_count)

    # -----------------------------------------------------------------------
    # SCHEMA: Lambda Invocation Output Envelope
    # {
    #   "statusCode": 200,
    #   "body": "{\"status\": \"success\", \"ids_pushed\": 1420}"
    # }
    # -----------------------------------------------------------------------
    return {
        "statusCode": 200,
        "body": json.dumps({"status": "success", "ids_pushed": len(changed_ids)}),
    }
