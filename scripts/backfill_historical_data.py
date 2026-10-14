"""TMDB Historical Movie Data Ingestion Pipeline.

Fetches historical movie records and detailed metadata via TMDB REST API,
partitions data by year and month, and stages raw JSON payloads into an AWS S3
Data Lake.
"""

import calendar
from datetime import date, datetime
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import time
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ---------------------------------------------------------------------------
# Configuration & Dual-Logging Setup (Console + File)
# ---------------------------------------------------------------------------
LOG_FILENAME = "tmdb_backfill.log"

# กำหนด Format ของ Log
log_formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 1. Console Handler: พ่นออกหน้าจอ Terminal
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# 2. File Handler: บันทึกลงไฟล์ .log
# ใช้ RotatingFileHandler เพื่อจำกัดขนาดไฟล์ไม่ให้ใหญ่เกินไป (สูงสุด 10MB เก็บสำรอง 3 ไฟล์)
file_handler = RotatingFileHandler(
    LOG_FILENAME,
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=3,
    encoding="utf-8",
)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

if not TMDB_API_KEY or not S3_BUCKET_NAME:
    raise ValueError(
        "Missing required environment variables: TMDB_API_KEY or S3_BUCKET_NAME."
    )

DISCOVER_API_URL = "https://api.themoviedb.org/3/discover/movie"
MOVIE_DETAILS_API_URL = "https://api.themoviedb.org/3/movie/{movie_id}"
REQUEST_TIMEOUT = 10  # seconds
RATE_LIMIT_PAUSE = 0.05  # seconds between requests to honor API rate limits

HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY}",
    "accept": "application/json",
}

# Configure resilient HTTP session with automated connection-level retries
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
def fetch_movie_ids_for_date_range(start_date, end_date):
    """Retrieve all unique movie IDs released within a specific date interval.

    Args:
        start_date: Start date string formatted as YYYY-MM-DD.
        end_date: End date string formatted as YYYY-MM-DD.

    Returns:
        List of unique TMDB movie IDs.
    """
    movie_ids = []
    page = 1
    total_pages = 1

    while page <= total_pages:
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
                    "Rate limit reached. Backing off for %s seconds.",
                    retry_after,
                )
                time.sleep(retry_after)
                continue

            if response.status_code != 200:
                logger.error(
                    "Discover API returned HTTP %s on page %s for range [%s - %s]. Skipping page.",
                    response.status_code,
                    page,
                    start_date,
                    end_date,
                )
                page += 1
                continue

            payload = response.json()

            # Set dynamic upper page bound constrained by TMDB's 500-page limit
            if page == 1:
                total_pages = min(payload.get("total_pages", 1), 500)
                logger.info(
                    "Range [%s to %s] contains %s movies across %s pages.",
                    start_date,
                    end_date,
                    payload.get("total_results", 0),
                    total_pages,
                )

            records = payload.get("results", [])
            movie_ids.extend([item["id"] for item in records if "id" in item])
            logger.info(
                "Fetched page %s/%s: %s movie IDs collected so far.",
                page,
                total_pages,
                len(movie_ids),
            )

            page += 1
            time.sleep(RATE_LIMIT_PAUSE)

        except requests.exceptions.RequestException as err:
            logger.error(
                "Network exception during discover query at page %s: %s",
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
# Main Orchestration Loop
# ---------------------------------------------------------------------------
def run_pipeline(start_year: int = 2016) -> None:
    """Execute monthly historical backfill pipeline from start_year through current date."""
    today: date = datetime.now().date()
    current_year: int = today.year
    current_month: int = today.month

    for year in range(start_year, current_year + 1):
        max_month: int = current_month if year == current_year else 12

        for month in range(1, max_month + 1):
            start_date = f"{year}-{month:02d}-01"

            # Bound end date to current day if processing active current month
            if year == current_year and month == current_month:
                end_date = today.strftime("%Y-%m-%d")
            else:
                last_day = calendar.monthrange(year, month)[1]
                end_date = f"{year}-{month:02d}-{last_day:02d}"

            logger.info(
                "--- Commencing ingestion for period: %s to %s ---",
                start_date,
                end_date,
            )

            # Stage 1: Collect Movie IDs
            target_ids = fetch_movie_ids_for_date_range(start_date, end_date)

            if not target_ids:
                logger.info(
                    "No movie records discovered for period %s to %s.",
                    start_date,
                    end_date,
                )
                continue

            # Stage 2: Enrich movie entities with detailed metadata
            monthly_payload = []
            logger.info("Enriching %s movie entities with metadata...", len(target_ids))

            for idx, movie_id in enumerate(target_ids, 1):
                detail = fetch_movie_details(movie_id)
                if detail:
                    monthly_payload.append(detail)
                if idx % 100 == 0:
                    logger.info("Progress: %s/%s movies enriched", idx, len(target_ids))
                time.sleep(RATE_LIMIT_PAUSE)

            # Stage 3: Load staged records into S3 Data Lake partition
            if monthly_payload:
                s3_partition_key = (
                    f"raw/historical/year={year}/month={month:02d}/"
                    f"movies_{year}_{month:02d}.json"
                )
                upload_json_to_s3(monthly_payload, S3_BUCKET_NAME, s3_partition_key)

            # Stage 4: Explicit memory management
            del target_ids
            del monthly_payload

    logger.info("Historical data ingestion completed successfully.")


if __name__ == "__main__":
    run_pipeline(start_year=2018)
