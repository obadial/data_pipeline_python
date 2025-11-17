from __future__ import annotations

import logging
from typing import Final

import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery

from .exceptions import DataLoadError, DataQualityError

logger = logging.getLogger(__name__)

DEFAULT_DATASET: Final[str] = "test"
DEFAULT_PRODUCTS_TABLE: Final[str] = "products"


def load_products(
    project_id: str,
    dataset_id: str = DEFAULT_DATASET,
    table_id: str = DEFAULT_PRODUCTS_TABLE,
) -> pd.DataFrame:
    """
    Load the products table from BigQuery into a Pandas DataFrame.

    Expected schema:
      - product_id (STRING)
      - product_name (STRING)
      - category (STRING)
      - brand (STRING)
      - condition (STRING)
    """
    client = bigquery.Client(project=project_id)
    table_full_name = f"{project_id}.{dataset_id}.{table_id}"

    query = f"""
        SELECT
          product_id,
          product_name,
          category,
          brand,
          condition
        FROM `{table_full_name}`
    """

    logger.info("Loading products from BigQuery: %s", table_full_name)

    try:
        job = client.query(query)
        df = job.result().to_dataframe()
    except GoogleAPIError as exc:
        msg = f"Failed to load products from BigQuery table {table_full_name}"
        logger.error(msg, exc_info=True)
        raise DataLoadError(msg) from exc
    except Exception as exc:  # pragma: no cover - catch-all safety
        msg = f"Unexpected error while loading products from {table_full_name}"
        logger.error(msg, exc_info=True)
        raise DataLoadError(msg) from exc

    required_cols = {"product_id", "product_name", "category", "brand", "condition"}
    missing = required_cols - set(df.columns)
    if missing:
        msg = (
            f"Products table {table_full_name} is missing required columns: "
            f"{', '.join(sorted(missing))}"
        )
        logger.error(msg)
        raise DataQualityError(msg)

    logger.info("Loaded %d product rows from BigQuery", len(df))
    return df
