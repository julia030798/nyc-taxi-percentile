"""
Simple test script for the NYC Yellow Taxi percentile logic.

This is intentionally not using a testing framework (like pytest) so that it
can be executed directly from the command line.

If everything is OK, it will print "Test passed". Otherwise it will print
an error message and exit with a non-zero status code.
"""

import sys

import polars as pl

from nyc_taxi_percentile import compute_percentile_trips


def run_tests() -> None:
    # 1) Happy path: simple DataFrame where we can check some properties
    df = pl.DataFrame(
        {
            "trip_distance": [1.0, 2.0, 3.0, 4.0, 5.0, 10.0],
            "passenger_count": [1, 1, 2, 1, 3, 2],
        }
    )

    filtered_df, percentile_value = compute_percentile_trips(df, percentile=0.9)

    # Basic checks: percentile_value must not be None and must be between min and max
    assert percentile_value is not None, "Percentile value should not be None"
    assert df["trip_distance"].min() <= percentile_value <= df["trip_distance"].max()

    # All returned trips must have distance strictly greater than the percentile
    assert (filtered_df["trip_distance"] > percentile_value).all(), (
        "All returned trips should have distance strictly greater "
        "than the percentile value"
    )

    # 2) Edge case: empty DataFrame
    empty_df = pl.DataFrame({"trip_distance": []})
    filtered_empty, empty_percentile = compute_percentile_trips(empty_df)
    assert filtered_empty.height == 0, "Filtered DataFrame should be empty for empty input"
    assert empty_percentile is None, "Percentile should be None for empty input"

    # 3) Error case: missing distance column
    wrong_df = pl.DataFrame({"distance": [1.0, 2.0, 3.0]})
    try:
        compute_percentile_trips(wrong_df)
    except ValueError:
        # This is expected
        pass
    else:
        raise AssertionError(
            "compute_percentile_trips should raise ValueError if 'trip_distance' "
            "column is missing"
        )


def main() -> int:
    try:
        run_tests()
    except AssertionError as exc:
        print(f"Test failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - simple test runner
        print(f"Test failed with unexpected error: {exc}", file=sys.stderr)
        return 1

    print("Test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

