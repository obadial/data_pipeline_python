from __future__ import annotations

"""
gcp_sales_pipeline

Pipeline to read products from BigQuery and sales from GCS, filter them by time,
join, and export an analytics-ready dataset.
"""

__all__ = ["__version__"]
__version__ = "0.1.0"
