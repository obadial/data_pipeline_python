from datetime import date
from pathlib import Path

import pandas as pd

from gcp_sales_pipeline.pipeline import PipelineConfig, run_pipeline


def test_run_pipeline_with_mocks(monkeypatch, tmp_path: Path):
    # -- Mocks for BigQuery and GCS --
    def fake_load_products(project_id: str, dataset_id: str, table_id: str):
        return pd.DataFrame(
            {
                "product_id": ["A", "B"],
                "product_name": ["Prod A", "Prod B"],
                "category": ["cat1", "cat2"],
                "brand": ["BrandX", "BrandY"],
                "condition": ["new", "used"],
            }
        )

    def fake_load_sales_parquet(
        project_id: str,
        bucket_name: str,
        start_date: date,
        end_date: date,
        max_files: int | None = 500,
    ):
        return pd.DataFrame(
            {
                "product_id": ["A", "B"],
                "price": [10.0, 20.0],
                "quantity": [1, 2],
                "sold_at": ["2025-01-10T10:00:00Z", "2025-01-10T11:00:00Z"],
                "order_id": ["O1", "O2"],
            }
        )

    monkeypatch.setattr(
        "gcp_sales_pipeline.pipeline.load_products",
        fake_load_products,
    )
    monkeypatch.setattr(
        "gcp_sales_pipeline.pipeline.load_sales_parquet",
        fake_load_sales_parquet,
    )

    # -- Pipeline configuration --
    config = PipelineConfig(
        project_id="dummy-project",
        ref_date=date(2025, 1, 10),
        output_dir=tmp_path,
    )

    output_path = run_pipeline(config)

    assert output_path.exists()

    # Read back the exported file and assert structure
    df = pd.read_parquet(output_path)
    assert set(df.columns) == {
        "product_id",
        "price",
        "quantity",
        "sold_at",
        "order_id",
        "product_name",
        "category",
        "brand",
        "condition",
        "processed_at",
        "sale_date",
        "quarter",
        "year",
        "month",
    }
    assert len(df) == 2
