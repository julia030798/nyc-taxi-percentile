import os
import tempfile
from typing import Tuple, Optional

from urllib.parse import urlparse

import polars as pl
import requests


def is_url(path: str) -> bool:
    """Check if path is an HTTP(S) URL."""
    return path.startswith("http://") or path.startswith("https://")


def download_to_tempfile(url: str) -> str:
    """Download URL to temporary file, returns path."""
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Error downloading '{url}': {exc}") from exc

    parsed = urlparse(url)
    _, ext = os.path.splitext(parsed.path)
    if not ext:
        ext = ".parquet"

    tmp_file = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
    try:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                tmp_file.write(chunk)
        tmp_file.flush()
    finally:
        tmp_file.close()

    return tmp_file.name


def load_parquet(input_path: str) -> pl.DataFrame:
    """Load Parquet file from local path or URL into Polars DataFrame."""
    if is_url(input_path):
        tmp_path = download_to_tempfile(input_path)
        try:
            df = pl.read_parquet(tmp_path)
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    else:
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file '{input_path}' does not exist.")
        df = pl.read_parquet(input_path)
    return df


def compute_percentile_trips(
    df: pl.DataFrame,
    percentile: float = 0.9,
    distance_column: str = "trip_distance",
) -> Tuple[pl.DataFrame, Optional[float]]:
    """
    Compute percentile threshold and return trips with distance strictly above it.
    
    Returns tuple of (filtered_dataframe, percentile_value).
    Uses '>' (strictly greater) to exclude trips exactly at the threshold.
    """
    if distance_column not in df.columns:
        raise ValueError(
            f"Column '{distance_column}' not found. "
            "Ensure file follows NYC Yellow Taxi data dictionary."
        )

    if df.height == 0:
        return df.clone(), None

    percentile_value = float(
        df.select(pl.col(distance_column).quantile(percentile)).item()
    )

    result = (
        df.filter(pl.col(distance_column) > percentile_value)
        .sort(distance_column)
    )

    return result, percentile_value


