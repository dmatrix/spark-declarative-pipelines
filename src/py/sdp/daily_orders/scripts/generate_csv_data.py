#!/usr/bin/env python3
"""
Generate CSV files with synthetic order data for the Daily Orders pipeline.

This script creates multiple CSV files containing random order data, which serves
as the source data for the SDP streaming ingestion pattern. By default, it generates
50 CSV files with 1000 orders each (50,000 total orders).

This script reuses the data generation logic from utils/order_gen_util.py to maintain
consistency and follow DRY principles.

Schema includes: order_id, order_item, price, items_ordered, status, state, date_ordered

Usage:
    cd /path/to/daily_orders
    uv run python scripts/generate_csv_data.py

    # Custom configuration
    uv run python scripts/generate_csv_data.py --num-files 100 --orders-per-file 50

    # Clean existing files before generating
    uv run python scripts/generate_csv_data.py --clean

    # Generate with reproducible random data
    uv run python scripts/generate_csv_data.py --random-state 42
"""

import argparse
import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from utils import order_gen_util
except ImportError:
    print("Error: Could not import order_gen_util. Make sure you're running from the daily_orders directory.")
    sys.exit(1)


def clean_data_directory(data_dir: Path) -> int:
    """
    Remove all existing CSV files from the data directory.

    Args:
        data_dir: Path to the data directory

    Returns:
        int: Number of files deleted
    """
    csv_files = list(data_dir.glob("*.csv"))
    count = 0

    for csv_file in csv_files:
        csv_file.unlink()
        count += 1

    return count


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Generate CSV files with synthetic order data for SDP streaming ingestion",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=50,
        help="Number of CSV files to generate"
    )
    parser.add_argument(
        "--orders-per-file",
        type=int,
        default=1000,
        help="Number of orders per CSV file"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean existing CSV files before generating new ones"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data",
        help="Output directory for CSV files (relative to daily_orders)"
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=None,
        help="Random seed for reproducibility. If not set, results will vary each run."
    )

    args = parser.parse_args()

    # Determine data directory path
    data_dir = Path(args.output_dir)
    if not data_dir.is_absolute():
        # Assume we're running from daily_orders directory
        data_dir = Path.cwd() / data_dir

    # Create data directory if it doesn't exist
    data_dir.mkdir(parents=True, exist_ok=True)

    # Clean existing files if requested
    if args.clean:
        deleted_count = clean_data_directory(data_dir)
        print(f"🧹 Cleaned {deleted_count} existing CSV files")

    # Initialize Spark session (required by order_gen_util)
    spark = SparkSession.builder \
        .appName("GenerateOrderCSVData") \
        .master("local[*]") \
        .getOrCreate()

    # Calculate totals
    total_orders = args.num_files * args.orders_per_file

    print(f"📊 Generating CSV Data for Daily Orders Pipeline")
    print(f"=" * 60)
    print(f"Configuration:")
    print(f"  - Number of files: {args.num_files}")
    print(f"  - Orders per file: {args.orders_per_file}")
    print(f"  - Total orders: {total_orders:,}")
    print(f"  - Output directory: {data_dir}")
    print(f"  - Random state: {args.random_state if args.random_state is not None else 'None (non-reproducible)'}")
    print(f"  - File naming: orders_001.csv ... orders_{args.num_files:03d}.csv")
    print()

    # Generate CSV files using order_gen_util
    print(f"⚙️  Generating CSV files using order_gen_util...")

    for i in range(1, args.num_files + 1):
        file_name = f"orders_{i:03d}.csv"
        file_path = data_dir / file_name

        # Use order_gen_util to generate DataFrame with orders
        # If random_state is provided, use it with an offset per file for variety while maintaining reproducibility
        if args.random_state is not None:
            file_seed = args.random_state + i
            orders_df = order_gen_util.create_random_order_items(num_items=args.orders_per_file, random_state=file_seed)
        else:
            orders_df = order_gen_util.create_random_order_items(num_items=args.orders_per_file)

        # Write DataFrame to CSV
        orders_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(file_path) + "_temp")

        # Find the generated CSV file and rename it
        temp_dir = Path(str(file_path) + "_temp")
        csv_file = next(temp_dir.glob("*.csv"))
        csv_file.rename(file_path)

        # Clean up temp directory
        for file in temp_dir.iterdir():
            file.unlink()
        temp_dir.rmdir()

        # Progress indicator
        if i % 10 == 0 or i == args.num_files:
            print(f"  Generated {i}/{args.num_files} files ({i * args.orders_per_file:,} orders)")

    # Stop Spark session
    spark.stop()

    print()
    print(f"✅ Successfully generated {args.num_files} CSV files with {total_orders:,} total orders")
    print(f"📁 Files saved to: {data_dir.absolute()}")
    print()
    print("Next steps:")
    print("  1. Run the pipeline: ./run_pipeline.sh")
    print("  2. Query the data: uv run python scripts/query_tables.py")
    print("  3. Calculate sales tax: uv run python scripts/calculate_sales_tax.py")
    print("  4. Analyze by state: uv run python scripts/state_analytics.py")


if __name__ == "__main__":
    main()
