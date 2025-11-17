from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from .exceptions import DataLoadError, DataQualityError, TooManyFilesError
from .filters import TimeGranularity
from .pipeline import PipelineConfig, run_pipeline

DATE_FORMAT = "%Y-%m-%d"


def _valid_date(value: str):
    """Validate a YYYY-MM-DD date for argparse."""
    try:
        return datetime.strptime(value, DATE_FORMAT).date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}', expected YYYY-MM-DD.") from exc


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Join products (BigQuery) and sales (GCS) filtered by date " "and export as parquet/csv.")
    )

    parser.add_argument(
        "--date",
        type=_valid_date,
        required=True,
        help="Reference date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--project-id",
        default="bot-sandbox-interviews-eb7b",
        help="GCP project ID (e.g. my-gcp-project). (default: bot-sandbox-interviews-eb7b)",
    )

    parser.add_argument(
        "--granularity",
        choices=[g.value for g in TimeGranularity],
        default=TimeGranularity.DAY.value,
        help="Time granularity: day, month, quarter, year (default: day).",
    )

    parser.add_argument(
        "--brand",
        action="append",
        help=("Filter on one or more brands. " "Can be specified multiple times (AND combined with other filters)."),
    )

    parser.add_argument(
        "--product-id",
        dest="product_ids",
        action="append",
        help=(
            "Filter on one or more product_id values. "
            "Can be specified multiple times (AND combined with other filters)."
        ),
    )

    parser.add_argument(
        "--bq-dataset",
        default="bm_mock_data",
        help="BigQuery dataset containing the products table (default: bm_mock_data).",
    )

    parser.add_argument(
        "--bq-table",
        default="products",
        help="Products table name (default: products).",
    )

    parser.add_argument(
        "--gcs-bucket",
        default="bm_mock_sales",
        help="GCS bucket containing the sales files (default: bm_mock_sales).",
    )

    parser.add_argument(
        "--output-dir",
        default="data/export",
        help="Output directory for exports (default: data/export).",
    )

    parser.add_argument(
        "--output-format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Export file format (default: parquet).",
    )

    parser.add_argument(
        "--max-sales-files",
        type=int,
        default=500,
        help=("Maximum number of daily sales files (days) to read from GCS before " "failing (default: 500)."),
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )

    return parser.parse_args(argv)


def _check_credentials_env() -> None:
    """
    Ensure GOOGLE_APPLICATION_CREDENTIALS points to a valid service account file.
    """
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        msg = (
            "Environment variable GOOGLE_APPLICATION_CREDENTIALS is not set. "
            "It must point to your service account JSON file (e.g. bot-sandbox-interviews-sa.json)."
        )
        raise SystemExit(msg)

    if not Path(creds_path).is_file():
        msg = "Credentials file specified by GOOGLE_APPLICATION_CREDENTIALS " f"does not exist: {creds_path}"
        raise SystemExit(msg)


def main(argv: Optional[list[str]] = None) -> None:
    # Load .env file if present
    load_dotenv()

    args = parse_args(argv)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    _check_credentials_env()

    granularity = TimeGranularity(args.granularity)
    output_dir = Path(args.output_dir)

    config = PipelineConfig(
        project_id=args.project_id,
        ref_date=args.date,
        granularity=granularity,
        bq_dataset=args.bq_dataset,
        bq_table=args.bq_table,
        gcs_bucket=args.gcs_bucket,
        brands=args.brand,
        product_ids=args.product_ids,
        output_dir=output_dir,
        output_format=args.output_format,
        max_sales_files=args.max_sales_files,
    )

    try:
        output_path = run_pipeline(config)
    except TooManyFilesError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(2)
    except (DataLoadError, DataQualityError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception:  # pragma: no cover - generic catch-all
        print("[ERROR] Unexpected error while running the pipeline.", file=sys.stderr)
        logging.getLogger(__name__).exception("Unexpected error")
        sys.exit(99)

    print(f"Export written to: {output_path}")
