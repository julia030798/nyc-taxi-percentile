"""
This package provides core functionality for computing distance percentiles
on NYC Yellow Taxi trip data stored in Parquet format.

Main Functions:
    load_parquet: Load a Parquet file from a local path or HTTP(S) URL into a Polars DataFrame.
    compute_percentile_trips: Calculate a percentile threshold and filter trips above it.

Example:
    >>> from nyc_taxi_percentile import load_parquet, compute_percentile_trips
    >>> df = load_parquet("yellow_tripdata_2025-01.parquet")
    >>> filtered_df, threshold = compute_percentile_trips(df, percentile=0.9)
    >>> print(f"Found {len(filtered_df)} trips above {threshold} miles")
"""

from .core import load_parquet, compute_percentile_trips

__all__ = ["load_parquet", "compute_percentile_trips"]
__version__ = "1.0.0"

