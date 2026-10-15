"""TMDB Daily Snapshot Ingestion Pipeline.

Fetches daily metrics (popularity, vote_count, vote_average) for movies
released within the trailing 30-day window via TMDB Discover API. Splits the
30-day window into two discrete intervals (0-14 days and 15-30 days) to prevent
exceeding TMDB's 500-page limit, and uploads partitioned snapshot payloads
directly into AWS S3.
"""

from datetime import datetime, timedelta, timezone
import json
import logging
import os
import time
import boto3
from botocore.exceptions import ClientError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ---------------------------------------------------------------------------
# Global Logging & Client Configuration
# ---------------------------------------------------------------------------
# Configure execution logging for AWS CloudWatch ingestion
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Retrieve configuration parameters from environment variables
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

if not TMDB_API_KEY or not S3_BUCKET_NAME:
    raise ValueError(
        "Missing required environment variables: TMDB_API_KEY or S3_BUCKET_NAME."
    )

DISCOVER_API_URL = "https://api.themoviedb.org/3/discover/movie"
REQUEST_TIMEOUT = 10  # HTTP request timeout in seconds
RATE_LIMIT_PAUSE = 0.05  # Sleep interval between calls to prevent rate limiting

HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY}",
    "accept": "application/json",
}

# Configure persistent HTTP session with exponential backoff retry strategy
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
)
session.mount("https://", HTTPAdapter(max_retries=retries))

# Initialize AWS S3 client outside handler to reuse across warm executions
s3_client = boto3.client("s3")


def fetch_movies_for_window(
    start_date: str, end_date: str, snapshot_date: str
) -> list[dict]:
    """Retrieve all theatrical movie records released within a specified date window.

    Queries the TMDB Discover API iteratively across pagination pages,
    enforcing a 500-page limit and filtering out adult and promotional video
    records. Injects the snapshot observation date into each entity for
    analytical partitioning.

    Args:
        start_date: Earliest primary release date string formatted as
          YYYY-MM-DD.
        end_date: Latest primary release date string formatted as YYYY-MM-DD.
        snapshot_date: Execution date string formatted as YYYY-MM-DD representing
          the periodic snapshot capture timestamp.

    Returns:
        List of movie entity dictionaries containing current performance metrics
        (popularity, vote_count, vote_average).

    Data Shape Example (Return List Item):
        [
            {
                "id": 1184918,
                "title": "The Wild Robot",
                "release_date": "2026-09-20",
                "popularity": 842.31,
                "vote_count": 1824,
                "vote_average": 8.52,
                "adult": False,
                "video": False,
                "snapshot_date": "2026-10-15"
            }, ...
        ]
    """
    records_collected = []
    page = 1
    total_pages = 1

    # Traverse all pagination pages returned by Discover API
    while page <= total_pages:
        # Schema Example for Discover API Query Parameters:
        # {
        #     "include_adult": "false",
        #     "include_video": "false",
        #     "primary_release_date.gte": "2026-10-01",
        #     "primary_release_date.lte": "2026-10-15",
        #     "page": 1
        # }
        params = {
            "include_adult": "false",
            "include_video": "false",
            "primary_release_date.gte": start_date,
            "primary_release_date.lte": end_date,
            "page": page,
        }

        try:
            response = session.get(
                DISCOVER_API_URL,
                params=params,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )

            # Handle TMDB rate-limiting explicitly
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logger.warning(
                    "Rate limit reached on Discover API. Backing off for %s seconds.",
                    retry_after,
                )
                time.sleep(retry_after)
                continue

            if response.status_code != 200:
                logger.error(
                    "Discover API returned HTTP %s on page %s for range [%s to %s]. Skipping page.",
                    response.status_code,
                    page,
                    start_date,
                    end_date,
                )
                page += 1
                continue

            # Schema Example of Raw Discover API Response (`payload`):
            # {
            #     "page": 1,
            #     "total_pages": 42,
            #     "total_results": 834,
            #     "results": [
            #         {
            #             "id": 1184918,
            #             "title": "The Wild Robot",
            #             "original_language": "en",
            #             "release_date": "2026-09-20",
            #             "popularity": 842.31,
            #             "vote_count": 1824,
            #             "vote_average": 8.52,
            #             "adult": false,
            #             "video": false,
            #             "genre_ids": [16, 878, 10751]
            #         }, ...
            #     ]
            # }
            payload = response.json()

            # Dynamically determine total available pages capped by TMDB's hard boundary (500 pages)
            if page == 1:
                total_pages = min(payload.get("total_pages", 1), 500)
                logger.info(
                    "Window [%s to %s]: discovered %s movies across %s pages.",
                    start_date,
                    end_date,
                    payload.get("total_results", 0),
                    total_pages,
                )

            page_results = payload.get("results", [])

            # Enrich and filter movie records
            for movie in page_results:
                # Exclude adult content and non-theatrical videos
                if movie.get("adult", False) or movie.get("video", False):
                    continue

                # Inject explicit snapshot metadata for downstream Data Warehouse ingestion
                # Mutates `movie` dict by adding: {"snapshot_date": "YYYY-MM-DD"}
                movie["snapshot_date"] = snapshot_date
                records_collected.append(movie)

            # Log periodic pagination progress
            if page % 10 == 0 or page == total_pages:
                logger.info(
                    "Fetched window [%s to %s] page %s/%s: %s records extracted so far.",
                    start_date,
                    end_date,
                    page,
                    total_pages,
                    len(records_collected),
                )

            page += 1
            time.sleep(RATE_LIMIT_PAUSE)

        except requests.exceptions.RequestException as err:
            logger.error(
                "Network exception during discover query at page %s for range [%s to %s]: %s",
                page,
                start_date,
                end_date,
                err,
            )
            page += 1
            time.sleep(2)

    return records_collected


def upload_json_to_s3(payload: list[dict], bucket: str, object_key: str) -> None:
    """Upload serialized JSON records directly into target S3 Data Lake path.

    Args:
        payload: List of movie entity dictionaries.
        bucket: Target AWS S3 bucket name.
        object_key: Destination object key path within S3.
    """
    try:
        json_buffer = json.dumps(payload, ensure_ascii=False)
        s3_client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=json_buffer.encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(
            "Successfully uploaded %s snapshot records to s3://%s/%s",
            len(payload),
            bucket,
            object_key,
        )
    except ClientError as err:
        logger.error("Failed to upload snapshot object to S3 [%s]: %s", object_key, err)
        raise


def lambda_handler(event, context):
    """Entry point orchestration for TMDB Daily Snapshot Fetcher.

    Splits the trailing 30-day lifecycle into two sub-windows (0-14 days and
    15-30 days ago), extracts snapshot metrics from the Discover API, deduplicates
    records, and persists the payload into S3.

    Args:
        event: Lambda invocation event dictionary. Optional override parameter:
            {"snapshot_date": "YYYY-MM-DD"}
        context: AWS Lambda runtime context object.

    Returns:
        Standard HTTP-compatible response dictionary with execution metrics.

    Sample Inbound Event:
        {}  # Automated mode (defaults to current UTC date)
        or
        {"snapshot_date": "2026-10-15"}  # Backfill / manual intervention

    Sample Return Payload:
        {
            "statusCode": 200,
            "body": "{\"status\": \"success\", \"snapshot_date\": \"2026-10-15\", ...}"
        }
    """
    utc_now = datetime.now(timezone.utc)

    # Determine snapshot observation date (defaults to current UTC date)
    snapshot_date_str = event.get("snapshot_date", utc_now.strftime("%Y-%m-%d"))
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date()

    logger.info(
        "=== Commencing TMDB Daily Snapshot for date: %s ===", snapshot_date_str
    )

    # -----------------------------------------------------------------------
    # Step 1: Define two distinct date windows to prevent 500-page overflow
    # -----------------------------------------------------------------------
    # Window A: Recent releases (0 to 14 days ago)
    window_a_start = (snapshot_date - timedelta(days=14)).strftime("%Y-%m-%d")
    window_a_end = snapshot_date.strftime("%Y-%m-%d")

    # Window B: Maturing releases (15 to 30 days ago)
    window_b_start = (snapshot_date - timedelta(days=30)).strftime("%Y-%m-%d")
    window_b_end = (snapshot_date - timedelta(days=15)).strftime("%Y-%m-%d")

    # -----------------------------------------------------------------------
    # Step 2: Fetch records from both release windows
    # -----------------------------------------------------------------------
    logger.info(
        "Executing Batch 1/2: Window A [%s to %s]", window_a_start, window_a_end
    )
    records_window_a = fetch_movies_for_window(
        window_a_start, window_a_end, snapshot_date_str
    )

    logger.info(
        "Executing Batch 2/2: Window B [%s to %s]", window_b_start, window_b_end
    )
    records_window_b = fetch_movies_for_window(
        window_b_start, window_b_end, snapshot_date_str
    )

    # -----------------------------------------------------------------------
    # Step 3: Combine and deduplicate records by movie_id
    # -----------------------------------------------------------------------
    # Schema Blueprint of intermediate lookup dictionary (`unique_movies`):
    # {
    #     1184918: {
    #         "id": 1184918,
    #         "title": "The Wild Robot",
    #         "release_date": "2026-09-20",
    #         "popularity": 842.31,
    #         "vote_count": 1824,
    #         "vote_average": 8.52,
    #         "snapshot_date": "2026-10-15"
    #     }, ...
    # }
    combined_records = records_window_a + records_window_b
    unique_movies = {}

    for movie in combined_records:
        movie_id = movie.get("id")
        if movie_id and movie_id not in unique_movies:
            unique_movies[movie_id] = movie

    final_payload = list(unique_movies.values())
    logger.info(
        "Aggregation complete: %s total items reduced to %s unique movie snapshots.",
        len(combined_records),
        len(final_payload),
    )

    # -----------------------------------------------------------------------
    # Step 4: Persist snapshot payload into partitioned S3 Data Lake path
    # -----------------------------------------------------------------------
    # Schema Blueprint of JSON written to S3 (Array of movie objects):
    # [
    #     {
    #         "id": 1184918,
    #         "title": "The Wild Robot",
    #         "popularity": 842.31,
    #         "vote_count": 1824,
    #         "vote_average": 8.52,
    #         "release_date": "2026-09-20",
    #         "snapshot_date": "2026-10-15"
    #     }, ...
    # ]
    if final_payload:
        s3_partition_key = (
            f"raw/daily_snapshot/year={snapshot_date.year}/month={snapshot_date.month:02d}/day={snapshot_date.day:02d}/"
            f"movies_snapshot_{snapshot_date_str}.json"
        )
        upload_json_to_s3(final_payload, S3_BUCKET_NAME, s3_partition_key)

    # Schema Blueprint of summary response dict:
    # {
    #     "status": "success",
    #     "snapshot_date": "2026-10-15",
    #     "window_a": {"range": ["2026-10-01", "2026-10-15"], "count": 420},
    #     "window_b": {"range": ["2026-09-15", "2026-09-30"], "count": 390},
    #     "total_unique_snapshots": 810
    # }
    summary = {
        "status": "success",
        "snapshot_date": snapshot_date_str,
        "window_a": {
            "range": [window_a_start, window_a_end],
            "count": len(records_window_a),
        },
        "window_b": {
            "range": [window_b_start, window_b_end],
            "count": len(records_window_b),
        },
        "total_unique_snapshots": len(final_payload),
    }
    logger.info("Snapshot ingestion completed successfully: %s", summary)

    return {"statusCode": 200, "body": json.dumps(summary)}
