from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

from .bq_client import load_products
from .filters import TimeGranularity, filter_sales_by_date
from .gcs_client import load_sales_parquet

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for the export pipeline."""

    project_id: str
    ref_date: date
    granularity: TimeGranularity = TimeGranularity.DAY

    bq_dataset: str = "bm_mock_data"
    bq_table: str = "products"

    gcs_bucket: str = "bm_mock_sales"

    # Filters
    brands: Optional[Sequence[str]] = None
    product_ids: Optional[Sequence[str]] = None

    # Output
    output_dir: Path = Path("data/export")
    output_format: str = "parquet"  # "parquet" or "csv"

    # Safety limit for GCS files (days)
    max_sales_files: int = 500


def _build_suffix(ref_date: date, granularity: TimeGranularity) -> str:
    """Build the file suffix based on the granularity."""
    if granularity == TimeGranularity.DAY:
        return ref_date.isoformat()
    if granularity == TimeGranularity.MONTH:
        return ref_date.strftime("%Y-%m")
    if granularity == TimeGranularity.QUARTER:
        quarter = (ref_date.month - 1) // 3 + 1
        return f"{ref_date.year}-Q{quarter}"
    if granularity == TimeGranularity.YEAR:
        return str(ref_date.year)
    raise ValueError(f"Unsupported granularity: {granularity}")


def _build_output_path(config: PipelineConfig) -> Path:
    """Build the full output path."""
    suffix = _build_suffix(config.ref_date, config.granularity)
    filename = f"sales_products_{config.granularity.value}_{suffix}.{config.output_format}"
    return config.output_dir / filename


def _compute_date_range(ref_date: date, granularity: TimeGranularity) -> tuple[date, date]:
    """
    Compute [start_date, end_date] for the given reference date and granularity.
    """
    if granularity == TimeGranularity.DAY:
        return ref_date, ref_date

    if granularity == TimeGranularity.MONTH:
        start = ref_date.replace(day=1)
        if ref_date.month == 12:
            next_month = date(ref_date.year + 1, 1, 1)
        else:
            next_month = date(ref_date.year, ref_date.month + 1, 1)
        end = next_month - timedelta(days=1)
        return start, end

    if granularity == TimeGranularity.QUARTER:
        quarter = (ref_date.month - 1) // 3 + 1
        first_month = 3 * (quarter - 1) + 1
        start = date(ref_date.year, first_month, 1)

        if quarter == 4:
            next_q_start = date(ref_date.year + 1, 1, 1)
        else:
            next_q_start = date(ref_date.year, first_month + 3, 1)

        end = next_q_start - timedelta(days=1)
        return start, end

    if granularity == TimeGranularity.YEAR:
        start = date(ref_date.year, 1, 1)
        end = date(ref_date.year + 1, 1, 1) - timedelta(days=1)
        return start, end

    raise ValueError(f"Unsupported granularity: {granularity}")


def _empty_final_dataframe() -> pd.DataFrame:
    """Return an empty DataFrame with the expected final schema (base columns)."""
    return pd.DataFrame(
        columns=[
            "product_id",
            "price",
            "quantity",
            "sold_at",
            "order_id",
            "product_name",
            "category",
            "brand",
            "condition",
        ]
    )


def _enrich_with_date_and_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add reporting-friendly columns:

    - sale_date: DATE(sold_at)
    - year, month, quarter ("Q1", "Q2", ...)
    - processed_at: timestamp of the pipeline run (UTC)

    Also sorts the dataframe by (sale_date, order_id) when possible.
    """
    # Always ensure these columns exist, even on empty dataframes.
    if df.empty:
        if "sold_at" not in df.columns:
            df["sold_at"] = pd.Series(dtype="datetime64[ns, UTC]")
        df["sale_date"] = pd.Series(dtype="object")
        df["year"] = pd.Series(dtype="Int64")
        df["month"] = pd.Series(dtype="Int64")
        df["quarter"] = pd.Series(dtype="object")
        df["processed_at"] = pd.Series(dtype="datetime64[ns, UTC]")
        return df

    # Normalize sold_at to datetime with UTC
    if "sold_at" in df.columns:
        sold_at = pd.to_datetime(df["sold_at"], utc=True, errors="coerce")
        df["sold_at"] = sold_at
        df["sale_date"] = sold_at.dt.date
        df["year"] = sold_at.dt.year.astype("Int64")
        df["month"] = sold_at.dt.month.astype("Int64")
        df["quarter"] = "Q" + sold_at.dt.quarter.astype(str)
    else:
        df["sale_date"] = pd.Series(dtype="object")
        df["year"] = pd.Series(dtype="Int64")
        df["month"] = pd.Series(dtype="Int64")
        df["quarter"] = pd.Series(dtype="object")

    # Single timestamp for the whole run
    processed_at = pd.Timestamp.now(tz="UTC")
    df["processed_at"] = processed_at

    # Sort by date then order_id if present
    sort_cols: list[str] = []
    if "sale_date" in df.columns:
        sort_cols.append("sale_date")
    if "order_id" in df.columns:
        sort_cols.append("order_id")

    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    return df


def run_pipeline(config: PipelineConfig) -> Path:
    """
    Run the full pipeline:

    1. Load products from BigQuery.
    2. Drop invalid product rows (NULL product_id).
    3. Compute the date range for the requested granularity.
    4. Load sales from GCS for that range (daily files).
    5. If no sales, export an empty dataset.
    6. Filter sales by sold_at to keep only rows matching the granularity.
    7. Pragmatically deduplicate products on product_id (keep last) if needed.
    8. Join sales with products on product_id (many-to-one).
    9. Apply filters for brands / product_ids (AND combination).
    10. Enrich with processed_at, date/period columns, and sort by (sale_date, order_id).
    11. Write the final dataset to a suffix-based export file.
    """
    logger.info("Starting pipeline with config: %s", config)

    # 1. Products from BigQuery
    products_df = load_products(
        project_id=config.project_id,
        dataset_id=config.bq_dataset,
        table_id=config.bq_table,
    )

    # 2. Drop invalid product rows where product_id is NULL
    null_count = products_df["product_id"].isna().sum()
    if null_count > 0:
        logger.warning(
            "Dropping %d product rows with NULL product_id from products table " "%s.%s.%s before joining.",
            null_count,
            config.project_id,
            config.bq_dataset,
            config.bq_table,
        )
        products_df = products_df[products_df["product_id"].notna()].copy()

    # 3. Date range
    start_date, end_date = _compute_date_range(config.ref_date, config.granularity)
    logger.info(
        "Computed date range for granularity %s: %s to %s",
        config.granularity,
        start_date,
        end_date,
    )

    # 4. Sales from GCS
    sales_df = load_sales_parquet(
        project_id=config.project_id,
        bucket_name=config.gcs_bucket,
        start_date=start_date,
        end_date=end_date,
        max_files=config.max_sales_files,
    )

    if sales_df.empty:
        logger.warning(
            "No sales data loaded from GCS for range %s to %s. " "Exporting an empty dataset.",
            start_date,
            end_date,
        )
        merged_df = _empty_final_dataframe()
    else:
        # 5. Temporal filter on sold_at column
        filtered_sales_df = filter_sales_by_date(
            sales_df,
            ref_date=config.ref_date,
            granularity=config.granularity,
        )

        if filtered_sales_df.empty:
            logger.warning(
                "Sales data loaded from GCS but no rows match the requested "
                "date/granularity. Exporting an empty dataset."
            )
            merged_df = _empty_final_dataframe()
        else:
            # OPTIONAL: drop sales rows with NULL product_id
            null_sales_count = filtered_sales_df["product_id"].isna().sum()
            if null_sales_count > 0:
                logger.warning(
                    "Dropping %d sales rows with NULL product_id before joining.",
                    null_sales_count,
                )
                filtered_sales_df = filtered_sales_df[filtered_sales_df["product_id"].notna()].copy()

            # 6. Pragmatic dedup: ensure product_id is unique in products_df
            if not products_df["product_id"].is_unique:
                duplicated_ids = (
                    products_df.loc[
                        products_df["product_id"].duplicated(keep=False),
                        "product_id",
                    ]
                    .dropna()
                    .unique()
                )

                logger.warning(
                    "Found %d non-unique product_id values in products table. "
                    "Example IDs: %s. Deduplicating by keeping the last row "
                    "per product_id.",
                    len(duplicated_ids),
                    ", ".join(map(str, duplicated_ids[:5])),
                )

                products_df = (
                    products_df.sort_values("product_id")
                    .drop_duplicates(subset="product_id", keep="last")
                    .reset_index(drop=True)
                )

            # 7. Join (many sales rows -> one product row per product_id)
            merged_df = filtered_sales_df.merge(
                products_df,
                on="product_id",
                how="left",
                validate="many_to_one",
            )

            # 8. Business filters
            if config.brands:
                logger.info("Applying brand filter: %s", config.brands)
                merged_df = merged_df[merged_df["brand"].isin(config.brands)]

            if config.product_ids:
                logger.info("Applying product_id filter: %s", config.product_ids)
                merged_df = merged_df[merged_df["product_id"].isin(config.product_ids)]

    merged_df = _enrich_with_date_and_metadata(merged_df)
    output_path = _build_output_path(config)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Writing output to %s", output_path)
    if config.output_format == "parquet":
        merged_df.to_parquet(output_path, index=False)
    elif config.output_format == "csv":
        merged_df.to_csv(output_path, index=False)
    else:
        msg = f"Unsupported output format: {config.output_format}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Pipeline finished successfully.")
    return output_path
