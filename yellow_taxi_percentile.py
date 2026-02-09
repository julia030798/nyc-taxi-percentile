import argparse
import sys

from nyc_taxi_percentile import load_parquet, compute_percentile_trips


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Return NYC Yellow Taxi trips over a given distance percentile."
    )
    parser.add_argument("input_file", help="Path or URL to Parquet file")
    parser.add_argument("output_file", nargs="?", help="Output Parquet file (optional)")
    parser.add_argument("--percentile", type=float, default=0.9, help="Percentile (0-1, default: 0.9)")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    if not (0.0 < args.percentile < 1.0):
        print(f"Error: --percentile must be between 0 and 1, got {args.percentile}", file=sys.stderr)
        return 1

    try:
        df = load_parquet(args.input_file)
        filtered_df, percentile_value = compute_percentile_trips(df, percentile=args.percentile)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Total trips: {len(df)}")
    if percentile_value is None:
        print("Input file is empty.")
        return 0

    print(f"{args.percentile:.2f} percentile: {percentile_value:.4f} miles")
    print(f"Trips over percentile: {len(filtered_df)}")

    if args.output_file:
        try:
            filtered_df.write_parquet(args.output_file)
            print(f"Output written to: {args.output_file}")
        except Exception as exc:  # noqa: BLE001
            print(f"Error writing output: {exc}", file=sys.stderr)
            return 1
    else:
        if len(filtered_df) > 0:
            print("\nFirst trips over percentile:")
            print(filtered_df.head().to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

