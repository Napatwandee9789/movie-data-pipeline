"""TMDB Incremental Movie Data Ingestion Pipeline (AWS Lambda).

Fetches recently updated movie IDs via TMDB Movie Changes API, retrieves full
metadata (including credits and keywords), and uploads daily partition payloads
directly to the AWS S3 Data Lake.
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
# Logging Configuration (Direct CloudWatch Integration)
# ---------------------------------------------------------------------------
# ใน Lambda ใช้ Default Handler เพื่อส่งต่อไปยัง AWS CloudWatch Logs อัตโนมัติ
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# กำหนด Format ของ Log ให้ตรงกับสไตล์ Backfill
log_formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
if logger.handlers:
    for handler in logger.handlers:
        handler.setFormatter(log_formatter)
else:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

# ---------------------------------------------------------------------------
# Environment Variables & HTTP Session Setup
# ---------------------------------------------------------------------------
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

if not TMDB_API_KEY or not S3_BUCKET_NAME:
    raise ValueError(
        "Missing required environment variables: TMDB_API_KEY or S3_BUCKET_NAME."
    )

CHANGES_API_URL = "https://api.themoviedb.org/3/movie/changes"
MOVIE_DETAILS_API_URL = "https://api.themoviedb.org/3/movie/{movie_id}"
REQUEST_TIMEOUT = 10  # seconds
RATE_LIMIT_PAUSE = 0.05  # seconds between requests to honor API rate limits

HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY}",
    "accept": "application/json",
}

# กำหนด Connection Pool และ Retry Adapter นอก Handler เพื่อ reuse ใน Warm Start
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
)
session.mount("https://", HTTPAdapter(max_retries=retries))

s3_client = boto3.client("s3")


# ---------------------------------------------------------------------------
# Core Ingestion Functions
# ---------------------------------------------------------------------------
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

    while page <= total_pages:
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "page": page,
        }

        try:
            response = session.get(
                CHANGES_API_URL,
                params=params,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )

            # Handle TMDB rate-limiting explicitly
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logger.warning(
                    "Rate limit reached on Changes API. Backing off for %s seconds.",
                    retry_after,
                )
                time.sleep(retry_after)
                continue

            if response.status_code != 200:
                logger.error(
                    "Changes API returned HTTP %s on page %s for range [%s to %s]. Skipping page.",
                    response.status_code,
                    page,
                    start_date,
                    end_date,
                )
                page += 1
                continue

            payload = response.json()

            if page == 1:
                total_pages = payload.get("total_pages", 1)
                logger.info(
                    "Changes range [%s to %s] reported %s items across %s pages.",
                    start_date,
                    end_date,
                    payload.get("total_results", 0),
                    total_pages,
                )

            records = payload.get("results", [])
            # กรองเฉพาะ record ที่ไม่ใช่ adult เพื่อรักษา consistency เดียวกับ backfill
            movie_ids.extend(
                [
                    item["id"]
                    for item in records
                    if "id" in item and not item.get("adult", False)
                ]
            )

            if page % 10 == 0 or page == total_pages:
                logger.info(
                    "Fetched changes page %s/%s: %s IDs collected so far.",
                    page,
                    total_pages,
                    len(movie_ids),
                )

            page += 1
            time.sleep(RATE_LIMIT_PAUSE)

        except requests.exceptions.RequestException as err:
            logger.error(
                "Network exception during changes query at page %s: %s",
                page,
                err,
            )
            page += 1
            time.sleep(2)

    return list(set(movie_ids))


def fetch_movie_details(movie_id: int):
    """Retrieve full movie specifications, credits, and keywords for a movie ID.

    Args:
        movie_id: Unique TMDB identifier.

    Returns:
        Dictionary containing detailed movie metadata, or None if extraction
        failed.
    """
    params = {
        "append_to_response": "credits,keywords",
        "language": "en-US",
    }
    target_url = MOVIE_DETAILS_API_URL.format(movie_id=movie_id)

    try:
        response = session.get(
            target_url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            time.sleep(retry_after)
            return fetch_movie_details(movie_id)

        if response.status_code == 200:
            return response.json()

        logger.warning(
            "Movie detail query failed for ID %s with HTTP %s",
            movie_id,
            response.status_code,
        )
        return None

    except requests.exceptions.RequestException as err:
        logger.error("Connection error while requesting movie ID %s: %s", movie_id, err)
        return None


def upload_json_to_s3(payload, bucket: str, object_key: str) -> None:
    """Upload serialized JSON records directly into targeted S3 object path.

    Args:
        payload: List of movie entity dictionaries.
        bucket: Target AWS S3 Bucket name.
        object_key: Destination path key within S3 bucket.
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
            "Successfully uploaded %s records to s3://%s/%s",
            len(payload),
            bucket,
            object_key,
        )
    except ClientError as err:
        logger.error("Failed to upload object to S3 [%s]: %s", object_key, err)
        raise


# ---------------------------------------------------------------------------
# AWS Lambda Handler
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    """Entrypoint for AWS Lambda.

    Supports custom date overrides from event payload:
    {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}
    Defaults to [Today to Today + 1 Day] (UTC).
    """
    utc_now = datetime.now(timezone.utc)

    # กำหนดค่าช่วงวัน: รองรับ manual trigger ผ่าน event หรือคำนวณวันปัจจุบัน + 1 วัน
    start_date = event.get(
        "start_date", (utc_now - timedelta(days=1)).strftime("%Y-%m-%d")
    )
    end_date = event.get("end_date", utc_now.strftime("%Y-%m-%d"))

    logger.info(
        "=== Commencing daily incremental ingestion for period: %s to %s ===",
        start_date,
        end_date,
    )

    # Stage 1: Collect Changed Movie IDs
    changed_ids = fetch_changed_movie_ids(start_date, end_date)

    if not changed_ids:
        logger.info(
            "No movie updates detected for period %s to %s.",
            start_date,
            end_date,
        )
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "No updates found", "records_ingested": 0}),
        }

    # Stage 2: Enrich movie entities with full metadata
    daily_payload = []
    logger.info("Enriching %s updated movies with metadata...", len(changed_ids))

    for idx, movie_id in enumerate(changed_ids, 1):
        # ตรวจสอบ execution context timeout ของ Lambda เพื่อ safety exit ก่อนตัดจบ 15 นาที
        if context and context.get_remaining_time_in_millis() < 30000:
            logger.warning(
                "Approaching Lambda execution timeout (< 30s remaining). Terminating loop early at index %s.",
                idx,
            )
            break

        detail = fetch_movie_details(movie_id)
        if detail:
            daily_payload.append(detail)

        if idx % 50 == 0:
            logger.info("Progress: %s/%s movies enriched", idx, len(changed_ids))

        time.sleep(RATE_LIMIT_PAUSE)

    # Stage 3: Load staged records into S3 Data Lake daily partition
    if daily_payload:
        execution_date = utc_now.strftime("%Y-%m-%d")
        s3_partition_key = (
            f"raw/daily/year={utc_now.year}/month={utc_now.month:02d}/day={utc_now.day:02d}/"
            f"movies_daily_{execution_date}.json"
        )
        upload_json_to_s3(daily_payload, S3_BUCKET_NAME, s3_partition_key)

    summary = {
        "status": "success",
        "start_date": start_date,
        "end_date": end_date,
        "discovered_ids": len(changed_ids),
        "ids_ingested": changed_ids,
    }
    logger.info("Daily ingestion completed successfully: %s", summary)

    return {"statusCode": 200, "body": json.dumps(summary)}
