from __future__ import annotations

from datetime import date
from enum import Enum

import pandas as pd


class TimeGranularity(str, Enum):
    """Supported time granularities for filtering."""

    DAY = "day"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


def filter_sales_by_date(
    sales_df: pd.DataFrame,
    ref_date: date,
    granularity: TimeGranularity,
) -> pd.DataFrame:
    """
    Filter the sales dataframe according to the reference date and granularity.

    - DAY:     keep records exactly on that date (YYYY-MM-DD)
    - MONTH:   same year + same month
    - QUARTER: same year + same quarter
    - YEAR:    same year

    The dataframe is expected to contain a `sold_at` column (timestamp-like).
    """
    if sales_df.empty:
        return sales_df

    df = sales_df.copy()
    df["sold_at"] = pd.to_datetime(df["sold_at"], utc=True)

    if granularity == TimeGranularity.DAY:
        mask = df["sold_at"].dt.date == ref_date
    elif granularity == TimeGranularity.MONTH:
        mask = (df["sold_at"].dt.year == ref_date.year) & (
            df["sold_at"].dt.month == ref_date.month
        )
    elif granularity == TimeGranularity.QUARTER:
        ref_quarter = (ref_date.month - 1) // 3 + 1
        sold_quarter = (df["sold_at"].dt.month - 1) // 3 + 1
        mask = (df["sold_at"].dt.year == ref_date.year) & (sold_quarter == ref_quarter)
    elif granularity == TimeGranularity.YEAR:
        mask = df["sold_at"].dt.year == ref_date.year
    else:
        raise ValueError(f"Unsupported granularity: {granularity}")

    return df.loc[mask].reset_index(drop=True)
