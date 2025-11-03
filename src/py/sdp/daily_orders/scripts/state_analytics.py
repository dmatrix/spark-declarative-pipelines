#!/usr/bin/env python3
"""
Analyze order distribution and revenue by US state from the Daily Orders pipeline.

This script provides geographic analytics by aggregating orders, revenue, and
status distribution across all 50 US states. Useful for understanding regional
performance and identifying high-value markets.

Usage:
    cd /path/to/daily_orders
    uv run python scripts/state_analytics.py

    # Show top N states
    uv run python scripts/state_analytics.py --top 20

    # Show specific state details
    uv run python scripts/state_analytics.py --state California

    # Export results to CSV
    uv run python scripts/state_analytics.py --export state_report.csv
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count, sum as _sum, avg, round as _round, col, desc
)

# Initialize Spark session with Hive support
spark = (SparkSession.builder.appName("StateAnalytics")
       .enableHiveSupport()
        .getOrCreate())


def show_state_distribution(top_n: int = 10):
    """
    Display order distribution and revenue by state.

    Args:
        top_n: Number of top states to display (default: 10)
    """
    # Read orders from materialized view
    orders_df = spark.read.table("orders_mv")

    print("\n" + "="*90)
    print(f"ORDER DISTRIBUTION BY STATE (Top {top_n})")
    print("="*90)

    state_stats = orders_df.groupBy("state") \
        .agg(
            count("*").alias("total_orders"),
            _round(_sum(col("price") * col("items_ordered")), 2).alias("total_revenue"),
            _round(avg("price"), 2).alias("avg_order_price"),
            _round(avg("items_ordered"), 2).alias("avg_items_per_order")
        ) \
        .orderBy(desc("total_orders"))

    state_stats.show(top_n, truncate=False)

    # Overall statistics
    total_orders = orders_df.count()
    total_states = state_stats.count()

    print(f"\nTotal Orders: {total_orders:,}")
    print(f"States with Orders: {total_states}")
    print(f"Average Orders per State: {total_orders / total_states:.1f}")


def show_status_by_state(top_n: int = 10):
    """
    Display order status distribution by state.

    Args:
        top_n: Number of top states to display (default: 10)
    """
    orders_df = spark.read.table("orders_mv")

    print("\n" + "="*90)
    print(f"ORDER STATUS DISTRIBUTION BY STATE (Top {top_n})")
    print("="*90)

    # Pivot to show status distribution
    status_by_state = orders_df.groupBy("state").pivot("status").count() \
        .fillna(0) \
        .withColumn("total", col("approved") + col("fulfilled") + col("pending") + col("cancelled")) \
        .orderBy(desc("total"))

    status_by_state.show(top_n, truncate=False)


def show_state_details(state_name: str):
    """
    Display detailed analytics for a specific state.

    Args:
        state_name: Name of the US state to analyze
    """
    orders_df = spark.read.table("orders_mv")

    # Filter for specific state
    state_orders = orders_df.filter(col("state") == state_name)

    total_orders = state_orders.count()

    if total_orders == 0:
        print(f"\n❌ No orders found for state: {state_name}")
        print("Available states can be listed with: --top 50")
        return

    print("\n" + "="*90)
    print(f"DETAILED ANALYTICS FOR {state_name.upper()}")
    print("="*90)

    # Overall statistics
    print(f"\n📊 Overall Statistics:")
    print(f"  - Total Orders: {total_orders:,}")

    overall_stats = state_orders.agg(
        _round(_sum(col("price") * col("items_ordered")), 2).alias("total_revenue"),
        _round(avg("price"), 2).alias("avg_price"),
        _round(avg("items_ordered"), 2).alias("avg_items")
    ).collect()[0]

    print(f"  - Total Revenue: ${overall_stats['total_revenue']:,.2f}")
    print(f"  - Average Order Price: ${overall_stats['avg_price']:,.2f}")
    print(f"  - Average Items per Order: {overall_stats['avg_items']:.2f}")

    # Status breakdown
    print(f"\n📋 Order Status Breakdown:")
    status_counts = state_orders.groupBy("status").count() \
        .orderBy(desc("count"))

    for row in status_counts.collect():
        percentage = (row['count'] / total_orders) * 100
        print(f"  - {row['status'].capitalize()}: {row['count']:,} ({percentage:.1f}%)")

    # Top products
    print(f"\n🛒 Top Products:")
    top_products = state_orders.groupBy("order_item") \
        .agg(
            count("*").alias("order_count"),
            _round(_sum(col("price") * col("items_ordered")), 2).alias("revenue")
        ) \
        .orderBy(desc("order_count")) \
        .limit(5)

    top_products.show(truncate=False)


def export_to_csv(output_file: str):
    """
    Export state analytics to CSV file.

    Args:
        output_file: Path to output CSV file
    """
    orders_df = spark.read.table("orders_mv")

    print(f"\n📤 Exporting state analytics to {output_file}...")

    state_stats = orders_df.groupBy("state") \
        .agg(
            count("*").alias("total_orders"),
            _round(_sum(col("price") * col("items_ordered")), 2).alias("total_revenue"),
            _round(avg("price"), 2).alias("avg_order_price"),
            _round(avg("items_ordered"), 2).alias("avg_items_per_order")
        ) \
        .orderBy(desc("total_orders"))

    # Write to CSV
    state_stats.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(output_file + "_temp")

    # Rename and cleanup (similar to generate_csv_data.py)
    from pathlib import Path
    temp_dir = Path(output_file + "_temp")
    csv_file = next(temp_dir.glob("*.csv"))
    csv_file.rename(output_file)

    # Clean up temp directory
    for file in temp_dir.iterdir():
        file.unlink()
    temp_dir.rmdir()

    print(f"✅ Export complete: {output_file}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Analyze order distribution and revenue by US state",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Show top 10 states by order volume:
    uv run python scripts/state_analytics.py

  Show top 20 states:
    uv run python scripts/state_analytics.py --top 20

  Analyze specific state:
    uv run python scripts/state_analytics.py --state California
    uv run python scripts/state_analytics.py --state "New York"

  Export to CSV:
    uv run python scripts/state_analytics.py --export state_report.csv
        """
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of top states to display (default: 10)"
    )
    parser.add_argument(
        "--state",
        type=str,
        default=None,
        help="Show detailed analytics for a specific state"
    )
    parser.add_argument(
        "--export",
        type=str,
        default=None,
        help="Export state analytics to CSV file"
    )
    parser.add_argument(
        "--show-status",
        action="store_true",
        help="Show order status distribution by state"
    )

    args = parser.parse_args()

    # Handle state-specific query
    if args.state:
        show_state_details(args.state)
    # Handle export
    elif args.export:
        export_to_csv(args.export)
    # Default: show distribution
    else:
        show_state_distribution(args.top)

        if args.show_status:
            show_status_by_state(args.top)

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()
