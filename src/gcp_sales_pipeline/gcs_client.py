from __future__ import annotations

import io
import logging
from datetime import date, timedelta
from typing import List, Optional

import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage
from google.cloud.exceptions import NotFound

from .exceptions import DataLoadError, TooManyFilesError

logger = logging.getLogger(__name__)


def _debug_list_nearby_sales_files(
    bucket: storage.Bucket,
    current_date: date,
    max_results: int = 20,
) -> None:
    """
    Debug helper: list a few sales files in the bucket when an expected file is missing.

    Behaviour:
      - list ALL blobs whose name starts with 'sales_'
      - sort them in descending order by name (latest-looking first)
      - log up to `max_results` of them

    This ignores the specific requested date, and shows a global view of available
    sales files in the bucket.
    """
    prefix = "sales_"
    logger.info(
        "Listing up to %d sales files in bucket %s with prefix '%s' (global, not only this month)...",
        max_results,
        bucket.name,
        prefix,
    )

    names: list[str] = []
    try:
        for blob in bucket.list_blobs(prefix=prefix):
            names.append(blob.name)
    except GoogleAPIError as exc:
        logger.warning(
            "Failed to list sales files in bucket %s with prefix '%s': %s",
            bucket.name,
            prefix,
            exc,
        )
        return

    if not names:
        logger.info(
            "No sales files found in bucket %s with prefix '%s'.",
            bucket.name,
            prefix,
        )
        return

    # Sort in descending order (latest filename first)
    names.sort(reverse=True)

    # Keep only the top `max_results`
    limited = names[:max_results]

    logger.info(
        "Sales files in bucket %s (prefix '%s', latest first, limit %d):",
        bucket.name,
        prefix,
        max_results,
    )
    for name in limited:
        logger.info("  - gs://%s/%s", bucket.name, name)


def load_sales_parquet(
    project_id: str,
    bucket_name: str,
    start_date: date,
    end_date: date,
    max_files: Optional[int] = 500,
) -> pd.DataFrame:
    """
    Read sales Parquet files for a date range from a GCS bucket and return
    a Pandas DataFrame.

    GCS layout is expected to be:

        gs://{bucket_name}/sales_YYYY-MM-DD.parquet

    For each day in [start_date, end_date], the function tries to read
    `sales_YYYY-MM-DD.parquet`.

    Expected schema:
      - product_id (STRING)
      - price (FLOAT)
      - quantity (INTEGER)
      - sold_at (TIMESTAMP)
      - order_id (STRING)
    """
    if end_date < start_date:
        raise ValueError(f"end_date {end_date} must be on or after start_date {start_date}")

    num_days = (end_date - start_date).days + 1
    if max_files is not None and num_days > max_files:
        msg = (
            f"Requested date range {start_date} to {end_date} spans {num_days} days "
            f"which exceeds max_files={max_files}."
        )
        logger.error(msg)
        raise TooManyFilesError(msg)

    logger.info(
        "Loading sales from GCS bucket=%s for range %s to %s " "(max days/files=%s)",
        bucket_name,
        start_date,
        end_date,
        max_files,
    )

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    frames: List[pd.DataFrame] = []

    for i in range(num_days):
        current_date = start_date + timedelta(days=i)
        blob_name = f"sales_{current_date:%Y-%m-%d}.parquet"
        full_uri = f"gs://{bucket_name}/{blob_name}"
        blob = bucket.blob(blob_name)

        logger.debug("Attempting to download %s", full_uri)
        try:
            data = blob.download_as_bytes()
        except NotFound:
            logger.warning("Sales file not found for date %s: %s", current_date, full_uri)
            _debug_list_nearby_sales_files(bucket, current_date, max_results=10)
            continue
        except GoogleAPIError as exc:
            msg = f"Failed to download sales file {full_uri}"
            logger.error(msg, exc_info=True)
            raise DataLoadError(msg) from exc
        except Exception as exc:  # pragma: no cover - catch-all safety
            msg = f"Unexpected error while downloading {full_uri}"
            logger.error(msg, exc_info=True)
            raise DataLoadError(msg) from exc

        try:
            df_day = pd.read_parquet(io.BytesIO(data))
        except Exception as exc:
            msg = f"Failed to read Parquet from {full_uri}"
            logger.error(msg, exc_info=True)
            raise DataLoadError(msg) from exc

        frames.append(df_day)

    if not frames:
        logger.warning(
            "No sales data loaded from bucket %s for date range %s to %s",
            bucket_name,
            start_date,
            end_date,
        )
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    logger.info(
        "Loaded %d sales rows from %d day file(s) in bucket %s",
        len(df),
        len(frames),
        bucket_name,
    )
    return df
