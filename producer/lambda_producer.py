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

# ---------------------------------------------------------------------------
# Global Logging & Environment Configuration
# ---------------------------------------------------------------------------
# Configure runtime logging for AWS CloudWatch ingestion
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Retrieve configuration parameters from Lambda environment variables
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")

# Initialize persistent HTTP session with exponential backoff retry strategy
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Initialize AWS SQS client outside handler to reuse across warm invocations
sqs_client = boto3.client("sqs")

# Target API endpoints and authentication headers
CHANGES_API_URL = "https://api.themoviedb.org/3/movie/changes"
HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY}",
    "accept": "application/json",
}


def fetch_changed_movie_ids(start_date: str, end_date: str) -> list[int]:
    """Retrieve all movie IDs that experienced changes between start_date and end_date.

    Args:
        start_date: Start date string formatted as YYYY-MM-DD.
        end_date: End date string formatted as YYYY-MM-DD.

    Returns:
        List of unique TMDB movie IDs flagged in the Changes API.
    """
    movie_ids = []
    page = 1
    total_pages = 1

    # Traverse all pagination pages returned by the Changes API
    while page <= total_pages:
        # Define query window and target page index
        response = session.get(
            CHANGES_API_URL,
            params={"start_date": start_date, "end_date": end_date, "page": page},
            headers=HEADERS,
            timeout=10,
        )

        # Skip failed page requests and proceed to the next page index
        if response.status_code != 200:
            page += 1
            continue

        payload = response.json()

        # Dynamically set upper pagination limit based on initial page response
        if page == 1:
            total_pages = payload.get("total_pages", 1)

        records = payload.get("results", [])

        # Filter out adult-classified content and collect valid TMDB movie IDs
        movie_ids.extend(
            [
                item["id"]
                for item in records
                if "id" in item and not item.get("adult", False)
            ]
        )
        page += 1

    # Deduplicate extracted IDs across pagination windows
    return list(set(movie_ids))


def push_to_sqs(movie_ids: list[int]) -> int:
    """Send movie IDs to SQS in batches of 10 (AWS Limit per call)."""
    total_sent = 0
    batch_size = 10  # Enforce AWS SQS SendMessageBatch maximum limit

    # Chunk the movie ID list into batches of 10 items
    for i in range(0, len(movie_ids), batch_size):
        chunk = movie_ids[i : i + batch_size]

        # Construct SQS batch entries with required unique batch-level identifiers
        entries = [
            {"Id": str(idx), "MessageBody": json.dumps({"movie_id": mid})}
            for idx, mid in enumerate(chunk)
        ]

        # Dispatch batch directly to targeted Amazon SQS Queue
        sqs_client.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
        total_sent += len(chunk)

    return total_sent


def lambda_handler(event, context):
    """Fetches updated movie IDs from TMDB Changes API and pushes them to SQS."""
    # Obtain current UTC timestamp for execution tracking
    utc_now = datetime.now(timezone.utc)

    # Resolve date boundaries: fallback to trailing 24-hour UTC window if omitted
    start_date = event.get("start_date", utc_now.strftime("%Y-%m-%d"))
    end_date = event.get("end_date", (utc_now + timedelta(days=1)).strftime("%Y-%m-%d"))

    # Stage 1: Discover changed movie entities
    logger.info("Fetching changed movies for %s to %s", start_date, end_date)
    changed_ids = fetch_changed_movie_ids(start_date, end_date)
    logger.info("Found %s movie IDs", len(changed_ids))

    # Stage 2: Enqueue discovered IDs for asynchronous downstream consumption
    if changed_ids:
        sent_count = push_to_sqs(changed_ids)
        logger.info("Pushed %s messages to SQS successfully", sent_count)

    # Return standard JSON response payload for observability and caller verification
    return {
        "statusCode": 200,
        "body": json.dumps({"status": "success", "ids_pushed": len(changed_ids)}),
    }
