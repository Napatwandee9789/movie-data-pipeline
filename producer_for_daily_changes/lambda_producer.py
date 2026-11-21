"""TMDB Incremental & New Release Ingestion - Producer Lambda.

Discovers both newly released movies (via TMDB Discover API) and modified existing
movies (via TMDB Changes API) for a closed 24-hour UTC window (T-1), deduplicates
entity identifiers across both sources, and dispatches batched payloads to Amazon SQS.
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
# Implement connection pooling with exponential backoff to mitigate transient network
# partitions and 5xx errors from TMDB without overwhelming upstream gateways.
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Performance Optimization:
# Instantiate AWS SDK client globally to leverage TCP keep-alive connections across warm invocations.
sqs_client = boto3.client("sqs")

# TMDB V3 Endpoints requiring Bearer token authentication
DISCOVER_API_URL = "https://api.themoviedb.org/3/discover/movie"
CHANGES_API_URL = "https://api.themoviedb.org/3/movie/changes"

HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY}",
    "accept": "application/json",
}


def fetch_new_released_movie_ids(target_date: str) -> list[int]:
    """Retrieve all movie IDs newly released on the specified target_date.

    Queries the TMDB /discover/movie endpoint across all paginated results to capture
    net-new catalog additions for the Lakehouse. Note: TMDB enforces a hard pagination
    ceiling of 500 pages (10,000 items) on the Discover endpoint.

    Args:
        target_date: Closed UTC date boundary formatted as ISO-8601 YYYY-MM-DD.

    Returns:
        list[int]: Unique collection of newly released TMDB movie IDs.

    Raises:
        requests.exceptions.RequestException: If unhandled network errors occur beyond
            the configured retry limit.
        ValueError: If JSON deserialization fails on a 200 response payload.
    """
    movie_ids = []
    page = 1
    total_pages = 1

    # -----------------------------------------------------------------------
    # STEP 1A.1: PAGINATED NET-NEW CATALOG EXTRACTION LOOP
    # -----------------------------------------------------------------------
    # TMDB /changes only tracks mutations on existing records; /discover/movie is
    # required to capture brand-new titles entering the catalog on target_date.
    while page <= total_pages:
        # Enforce a strict 10s socket timeout to prevent indefinite Lambda hangs consuming billable compute
        response = session.get(
            DISCOVER_API_URL,
            params={
                "include_adult": "false",
                "include_video": "false",
                "primary_release_date.gte": target_date,
                "primary_release_date.lte": target_date,
                "page": page,
            },
            headers=HEADERS,
            timeout=10,
        )

        # Fault-tolerance policy:
        # Skip anomalous individual page failures (or TMDB's >500 page cap HTTP 400)
        # to prioritize overall pipeline progress without blocking downstream consumers.
        if response.status_code != 200:
            logger.warning(
                "Discover API returned status %s on page %s for %s; skipping page.",
                response.status_code,
                page,
                target_date,
            )
            page += 1
            continue

        # -------------------------------------------------------------------
        # SCHEMA: TMDB /3/discover/movie API Response Payload
        # {
        #   "page": 1,
        #   "results": [
        #     {
        #       "adult": false,
        #       "id": 124905,
        #       "original_title": "Godzilla",
        #       "release_date": "2026-11-20",
        #       "title": "Godzilla"
        #     }
        #   ],
        #   "total_pages": 12,
        #   "total_results": 235
        # }
        # -------------------------------------------------------------------
        payload = response.json()

        # Dynamic loop boundary: Read actual pagination depth returned by the upstream API.
        # Capped at 500 per TMDB Discover API specification to avoid guaranteed HTTP 400s.
        if page == 1:
            total_pages = min(payload.get("total_pages", 1), 500)

        records = payload.get("results", [])

        # -------------------------------------------------------------------
        # STEP 1A.2: ENTITY ID EXTRACTION & COMPLIANCE GUARDRAIL
        # -------------------------------------------------------------------
        # Double-enforce adult filtering in code as defense-in-depth alongside API query params
        movie_ids.extend(
            [
                item["id"]
                for item in records
                if "id" in item and not item.get("adult", False)
            ]
        )
        page += 1

    return list(set(movie_ids))


def fetch_changed_movie_ids(start_date: str, end_date: str) -> list[int]:
    """Retrieve all existing movie IDs that experienced metadata changes in the date window.

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
    # STEP 1B.1: PAGINATED DELTA MUTATION EXTRACTION LOOP
    # -----------------------------------------------------------------------
    # TMDB restricts change queries to a max of 100 items per page; full traversal is required
    while page <= total_pages:
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
            logger.warning(
                "Changes API returned status %s on page %s for %s..%s; skipping page.",
                response.status_code,
                page,
                start_date,
                end_date,
            )
            page += 1
            continue

        # -------------------------------------------------------------------
        # SCHEMA: TMDB /3/movie/changes API Response Payload
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
        # STEP 1B.2: COMPLIANCE FILTERING & ENTITY ID COLLECTION
        # -------------------------------------------------------------------
        # Business logic requirement: Exclude adult-rated content early in ingestion
        # to ensure downstream data catalog compliance and optimize processing costs.
        movie_ids.extend(
            [
                item["id"]
                for item in records
                if "id" in item and not item.get("adult", False)
            ]
        )
        page += 1

    # -----------------------------------------------------------------------
    # STEP 1B.3: INTRA-SOURCE DEDUPLICATION
    # -----------------------------------------------------------------------
    # TMDB Changes API may emit duplicate IDs across pages; deduplicate in-memory.
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
    # STEP 3.1: MESSAGE-LEVEL CHUNKING (COST & THROUGHPUT OPTIMIZATION)
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
    # STEP 3.2: SQS SDK BATCH DISPATCH (UP TO 500 MOVIES PER API CALL)
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
    """AWS Lambda entry point for TMDB daily new-release & incremental ingestion pipeline.

    Coordinates net-new catalog discovery (/discover/movie) and delta change extraction
    (/movie/changes) for a completed UTC day (T-1), performs cross-source deduplication,
    and orchestrates SQS fan-out dispatch without altering downstream worker contracts.

    Args:
        event: AWS Lambda invocation event. Accepts optional ISO-8601 date boundary overrides
            for manual backfills.
            Example (single day): {"target_date": "2026-11-20"}
            Example (range): {"start_date": "2026-11-18", "end_date": "2026-11-20"}
        context: AWS Lambda runtime context object containing execution environment metadata.

    Returns:
        dict: Standard HTTP-compatible API Gateway / Lambda response envelope summarizing
            extraction counts per source and total unique IDs pushed.

    Raises:
        botocore.exceptions.ClientError: If downstream AWS SQS access is unauthorized or fails.
        requests.exceptions.RequestException: If upstream TMDB API is completely unreachable.
    """
    # -----------------------------------------------------------------------
    # STEP 1: RESOLVE TEMPORAL INGESTION BOUNDARIES (T-1 CLOSED WINDOW)
    # -----------------------------------------------------------------------
    # Architectural Rationale:
    # Scheduled at ~00:30 after midnight so the previous UTC day (today - 1) has fully
    # closed. Querying a completed day prevents late-arriving upstream updates from being
    # missed compared to querying an in-progress day shortly after midnight.
    utc_yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    default_target_date = utc_yesterday.strftime("%Y-%m-%d")

    # Backfill / Override Support:
    # Allows explicit single-day ("target_date") or range ("start_date"/"end_date") overrides
    # via Lambda event payload while defaulting cleanly to T-1 for daily cron runs.
    target_date = event.get("target_date", default_target_date)
    start_date = event.get("start_date", target_date)
    end_date = event.get("end_date", target_date)

    # -----------------------------------------------------------------------
    # STEP 2: DUAL-SOURCE UPSTREAM EXTRACTION & CROSS-SOURCE DEDUPLICATION
    # -----------------------------------------------------------------------
    # 2.1 Fetch net-new movie releases entering the catalog on target_date
    logger.info("Fetching newly released movies for target_date=%s", target_date)
    new_release_ids = fetch_new_released_movie_ids(target_date)
    logger.info("Found %s newly released movie IDs", len(new_release_ids))

    # 2.2 Fetch existing movies that received metadata updates in [start_date, end_date]
    logger.info("Fetching changed movies for %s to %s", start_date, end_date)
    changed_ids = fetch_changed_movie_ids(start_date, end_date)
    logger.info("Found %s changed movie IDs", len(changed_ids))

    # 2.3 Cross-Source Deduplication:
    # A newly released movie frequently receives immediate metadata edits on its release day,
    # causing it to appear in BOTH /discover/movie and /movie/changes. Merging via set union
    # prevents duplicate SQS messages and redundant TMDB detail fetches in downstream workers.
    unified_movie_ids = list(set(new_release_ids) | set(changed_ids))
    logger.info(
        "Unified & deduplicated total: %s unique movie IDs (overlap removed: %s)",
        len(unified_movie_ids),
        (len(new_release_ids) + len(changed_ids)) - len(unified_movie_ids),
    )

    # -----------------------------------------------------------------------
    # STEP 3: ASYNCHRONOUS FAN-OUT ENQUEUE (DOWNSTREAM BUFFERING)
    # -----------------------------------------------------------------------
    # Decouple lightweight ID discovery from heavy detail-enrichment workers via SQS
    if unified_movie_ids:
        sent_count = push_to_sqs(unified_movie_ids, chunk_size=50)
        logger.info("Pushed %s movie IDs to SQS successfully", sent_count)

    # -----------------------------------------------------------------------
    # SCHEMA: Lambda Invocation Output Envelope
    # {
    #   "statusCode": 200,
    #   "body": "{\"status\": \"success\", \"target_date\": \"2026-11-20\", \"new_releases_found\": 145, \"changes_found\": 1310, \"ids_pushed\": 1420}"
    # }
    # -----------------------------------------------------------------------
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "status": "success",
                "target_date": target_date,
                "new_releases_found": len(new_release_ids),
                "changes_found": len(changed_ids),
                "ids_pushed": len(unified_movie_ids),
            }
        ),
    }


# ===========================================================================
# ARCHITECTURE & EXECUTION FLOW DIAGRAM
# ===========================================================================
#                      [Apache Airflow DAG (~00:30 Daily)]
#                                  │
#                                  ▼ (AWS Lambda Invoke API)
#   ┌───────────────────────────────────────────────────────────────────────┐
#   │ STEP 1: RESOLVE TEMPORAL BOUNDARY (lambda_handler)                    │
#   │  • Compute closed UTC window: target_date = (UTC_NOW - 1 day)         │
#   │  • Allow optional override via event["target_date" | "start_date"]    │
#   └─────────────────────────────────┬─────────────────────────────────────┘
#                                     │
#                   ┌─────────────────┴─────────────────┐
#                   ▼                                   ▼
#   ┌───────────────────────────────┐   ┌───────────────────────────────────┐
#   │ STEP 2A: NET-NEW CATALOG      │   │ STEP 2B: METADATA UPDATES         │
#   │ fetch_new_released_movie_ids()│   │ fetch_changed_movie_ids()         │
#   │                               │   │                                   │
#   │ GET /3/discover/movie         │   │ GET /3/movie/changes              │
#   │  • primary_release_date = T-1 │   │  • start_date = T-1               │
#   │  • include_adult = false      │   │  • end_date   = T-1               │
#   │  • Paginate page 1..N (<=500) │   │  • Paginate page 1..N             │
#   │  • Extract & dedup IDs        │   │  • Filter adult=false & dedup IDs │
#   └───────────────┬───────────────┘   └─────────────────┬─────────────────┘
#                   │                                     │
#                   └─────────────────┬───────────────────┘
#                                     ▼
#   ┌───────────────────────────────────────────────────────────────────────┐
#   │ STEP 2C: CROSS-SOURCE MERGE & DEDUPLICATION                           │
#   │  • unified_movie_ids = list(set(new_release_ids) | set(changed_ids))  │
#   │  • Eliminates overlap when a new movie is also edited on release day  │
#   └─────────────────────────────────┬─────────────────────────────────────┘
#                                     │
#                                     ▼ (If unified_movie_ids is non-empty)
#   ┌───────────────────────────────────────────────────────────────────────┐
#   │ STEP 3: TWO-TIER SQS BATCH DISPATCH (push_to_sqs)                     │
#   │                                                                       │
#   │  [Tier 1: Business Chunking]                                          │
#   │   • Group IDs into chunks of 50 -> {"movie_ids": [id1, ..., id50]}    │
#   │                                                                       │
#   │  [Tier 2: Infrastructure Batching]                                    │
#   │   • Group up to 10 messages per sqs.send_message_batch() API call     │
#   │   • Dispatches up to 500 movies per network round-trip                │
#   └─────────────────────────────────┬─────────────────────────────────────┘
#                                     │
#                                     ▼
#                      [Amazon SQS Queue] ──► [Downstream Worker Lambdas]
#                      (Contract unchanged: {"movie_ids": [...]})
#
# ===========================================================================
