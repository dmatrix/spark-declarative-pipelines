#!/usr/bin/env python3
"""
Spark Declarative Pipelines (SDP) Examples

This module provides a command-line interface to run the SDP example pipelines:
- Daily Orders: E-commerce order processing and analytics
- Oil Rigs: Industrial IoT sensor monitoring and analysis

Usage:
    python main.py --help
    python main.py daily-orders
    python main.py oil-rigs
"""

import argparse
import sys
import subprocess
import os
from pathlib import Path


def run_daily_orders_pipeline():
    """Run the Daily Orders e-commerce pipeline."""
    print("🏪 Running Daily Orders E-commerce Pipeline...")
    print("=" * 50)

    daily_orders_dir = Path("daily_orders")
    if not daily_orders_dir.exists():
        print(f"Error: {daily_orders_dir} directory not found!")
        return 1
    
    try:
        # Change to daily_orders directory and run pipeline
        os.chdir(daily_orders_dir)
        
        print("1. Executing SDP pipeline...")
        try:
            subprocess.run(["./run_pipeline.sh"], check=True)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print("❌ ERROR: SDP pipeline command failed!")
            print("   Spark Declarative Pipelines CLI is missing files.")
            print("   Please check the SDP CLI before running this pipeline.")
            return 1
        
        print("\n2. Querying order data...")
        subprocess.run(["python", "scripts/query_tables.py"], check=True)

        print("\n3. Calculating sales tax and analytics...")
        subprocess.run(["python", "scripts/calculate_sales_tax.py"], check=True)
        
        print("\n✅ Daily Orders pipeline completed successfully!")
        return 0
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    finally:
        os.chdir("..")


def run_oil_rigs_pipeline():
    """Run the Oil Rigs industrial monitoring pipeline."""
    print("🛢️  Running Oil Rigs Industrial Monitoring Pipeline...")
    print("=" * 50)
    
    oil_rigs_dir = Path("oil_rigs")
    if not oil_rigs_dir.exists():
        print(f"Error: {oil_rigs_dir} directory not found!")
        return 1
    
    try:
        # Change to oil_rigs directory and run pipeline
        os.chdir(oil_rigs_dir)
        
        print("1. Executing SDP pipeline...")
        try:
            subprocess.run(["./run_pipeline.sh"], check=True)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print("❌ ERROR: SDP pipeline command failed!")
            print("   Spark Declarative Pipelines CLI is missing files.")
            print("   Please check the SDP CLI before running this pipeline.")
            return 1
        
        print("\n2. Querying sensor data...")
        subprocess.run(["python", "query_oil_rigs_tables.py"], check=True)

        print("\n3. Generating visualizations...")
        subprocess.run(["uv", "run", "python", "scripts/plot_sensors.py", "--all-metrics", "--rig", "each", "--output-dir", "artifacts"], check=True)

        print("\n✅ Oil Rigs pipeline completed successfully!")
        return 0
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    finally:
        os.chdir("..")


def main():
    """Main entry point for SDP examples."""
    parser = argparse.ArgumentParser(
        description="Spark Declarative Pipelines (SDP) Examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py daily-orders # Run Daily Orders e-commerce pipeline
  python main.py oil-rigs     # Run Oil Rigs sensor monitoring pipeline
  
Requirements:
  - Spark Declarative Pipelines CLI must be installed
  - Use 'spark-pipelines --help' to verify CLI availability
  
For more information, see SDP_README.md
        """
    )
    
    parser.add_argument(
        "pipeline",
        choices=["daily-orders", "oil-rigs"],
        help="Pipeline to run"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="SDP Examples v0.1.0"
    )
    
    if len(sys.argv) == 1:
        parser.print_help()
        return 0
    
    args = parser.parse_args()
    
    print("🚀 Spark Declarative Pipelines (SDP) Examples")
    print("=" * 50)
    
    if args.pipeline == "daily-orders":
        return run_daily_orders_pipeline()
    elif args.pipeline == "oil-rigs":
        return run_oil_rigs_pipeline()
    else:
        print(f"❌ Unknown pipeline: {args.pipeline}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
