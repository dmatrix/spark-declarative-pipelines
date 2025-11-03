#!/usr/bin/env python3
"""
Query orders from materialized views with optional status filtering.

This script queries order data from materialized views created by the SDP pipeline.
You can query all orders or filter by specific status (approved, fulfilled, pending, cancelled).

Usage:
    cd /path/to/daily_orders

    # Query all orders
    uv run python scripts/query_tables.py

    # Query orders by status
    uv run python scripts/query_tables.py --status orders
    uv run python scripts/query_tables.py --status approved
    uv run python scripts/query_tables.py --status fulfilled
    uv run python scripts/query_tables.py --status pending
    uv run python scripts/query_tables.py --status cancelled

    # Limit number of rows displayed
    uv run python scripts/query_tables.py --status approved --limit 20

    # List available tables and statuses
    uv run python scripts/query_tables.py --list

    # Show help
    uv run python scripts/query_tables.py --help
"""

import argparse
from pyspark.sql import DataFrame, SparkSession

# Location of the Spark database has all the materialized views
spark_db_location = "file:///./spark-warehouse/"

# Initialize Spark session with Hive support
spark = (SparkSession.builder.appName("QueryOrders")
       .enableHiveSupport()
        .getOrCreate())


def query_orders_by_status(status: str = None) -> DataFrame:
    """
    Query orders from materialized views based on status.

    Args:
        status: Which materialized view to query:
                - 'orders' or None: queries orders_mv (all orders)
                - 'approved': queries approved_orders_mv
                - 'fulfilled': queries fulfilled_orders_mv
                - 'pending': queries pending_orders_mv
                - 'cancelled': queries cancelled_orders_mv

    Returns:
        DataFrame: DataFrame containing order items with selected columns.
    """
    # Determine which materialized view to query
    if status is None or status == "orders":
        table_name = "orders_mv"
    else:
        table_name = f"{status}_orders_mv"

    # Read from the appropriate materialized view
    df = spark.read.table(table_name)

    # Select key columns for display
    df = df.select("order_id", "status", "order_item", "price", "items_ordered", "state", "date_ordered")

    return df


def list_available_tables():
    """List all available materialized views and status options."""
    print("\n" + "="*80)
    print("AVAILABLE MATERIALIZED VIEWS")
    print("="*80 + "\n")

    tables = [
        ("orders_mv", "All orders (default)", "--status orders"),
        ("approved_orders_mv", "Orders with 'approved' status", "--status approved"),
        ("fulfilled_orders_mv", "Orders with 'fulfilled' status", "--status fulfilled"),
        ("pending_orders_mv", "Orders with 'pending' status", "--status pending"),
        ("cancelled_orders_mv", "Orders with 'cancelled' status", "--status cancelled"),
    ]

    print(f"{'TABLE NAME':<25} {'DESCRIPTION':<35} {'COMMAND'}")
    print("-" * 80)
    for table_name, description, command in tables:
        print(f"{table_name:<25} {description:<35} {command}")

    print("\n" + "="*80)
    print("USAGE EXAMPLES")
    print("="*80 + "\n")

    examples = [
        "uv run python scripts/query_tables.py",
        "uv run python scripts/query_tables.py --status approved",
        "uv run python scripts/query_tables.py --status fulfilled --limit 20",
        "uv run python scripts/query_tables.py --list",
        "uv run python scripts/query_tables.py --help",
    ]

    for example in examples:
        print(f"  {example}")

    print()


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Query orders from Daily Orders materialized views",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Query all orders:
    uv run python scripts/query_tables.py

  Query specific status:
    uv run python scripts/query_tables.py --status approved
    uv run python scripts/query_tables.py --status fulfilled --limit 20

  List available tables:
    uv run python scripts/query_tables.py --list
        """
    )
    parser.add_argument(
        "--status",
        type=str,
        choices=["orders", "approved", "fulfilled", "pending", "cancelled"],
        default=None,
        help="Which materialized view to query: orders (all), approved, fulfilled, pending, or cancelled. Default: orders"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of rows to display (default: 10)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available materialized views and exit"
    )

    args = parser.parse_args()

    # Handle --list option
    if args.list:
        list_available_tables()
        return

    # Query the orders
    print(f"\n{'='*80}")
    if args.status is None or args.status == "orders":
        print("Querying ALL orders from orders_mv")
        status_display = "all"
    else:
        print(f"Querying {args.status.upper()} orders from {args.status}_orders_mv")
        status_display = args.status
    print(f"{'='*80}\n")

    orders_df = query_orders_by_status(args.status)

    # Get total count
    total_count = orders_df.count()
    print(f"Total {status_display} orders: {total_count}\n")

    # Show the DataFrame
    orders_df.show(args.limit, truncate=False)

    if total_count > args.limit:
        print(f"\nShowing {args.limit} of {total_count} orders. Use --limit to show more.")

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()
