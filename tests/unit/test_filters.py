from datetime import date

import pandas as pd

from gcp_sales_pipeline.filters import TimeGranularity, filter_sales_by_date


def _make_sales_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_id": ["A", "B", "C", "D"],
            "sold_at": [
                "2025-01-10T10:00:00Z",
                "2025-01-31T23:59:59Z",
                "2025-02-01T00:00:00Z",
                "2025-04-15T12:00:00Z",
            ],
            "quantity": [1, 2, 3, 4],
        }
    )


def test_filter_day():
    df = _make_sales_df()
    ref_date = date(2025, 1, 10)

    filtered = filter_sales_by_date(df, ref_date, TimeGranularity.DAY)

    assert len(filtered) == 1
    assert set(filtered["product_id"]) == {"A"}


def test_filter_month():
    df = _make_sales_df()
    ref_date = date(2025, 1, 5)

    filtered = filter_sales_by_date(df, ref_date, TimeGranularity.MONTH)

    assert len(filtered) == 2
    assert set(filtered["product_id"]) == {"A", "B"}


def test_filter_quarter():
    df = _make_sales_df()
    ref_date = date(2025, 1, 1)

    filtered = filter_sales_by_date(df, ref_date, TimeGranularity.QUARTER)

    # Q1 = January to March -> A, B, C
    assert len(filtered) == 3
    assert set(filtered["product_id"]) == {"A", "B", "C"}


def test_filter_year():
    df = _make_sales_df()
    ref_date = date(2025, 7, 1)

    filtered = filter_sales_by_date(df, ref_date, TimeGranularity.YEAR)

    assert len(filtered) == 4
