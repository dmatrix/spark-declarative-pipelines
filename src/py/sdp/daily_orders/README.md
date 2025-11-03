# Daily Orders - E-commerce Order Processing Pipeline

## Overview

Daily Orders is a complete e-commerce order processing and analytics system built using Spark Declarative Pipelines (SDP). It demonstrates **CSV-based streaming ingestion**, materialized view transformations, and comprehensive sales analytics including tax calculations.

This pipeline showcases a realistic data engineering pattern where CSV files serve as the source of truth, demonstrating SDP's ability to ingest data from external sources rather than generating it in-memory.

## Project Structure

```
daily_orders/
├── README.md                       # This file
├── pipeline.yml                    # SDP pipeline configuration
├── run_pipeline.sh                 # Pipeline execution script with CSV generation
├── __init__.py                     # Python package initialization
├── data/                           # CSV source data directory
│   ├── .gitignore                  # Ignore CSV files (generated dynamically)
│   ├── orders_001.csv              # Order data files (50 files × 1000 orders)
│   ├── orders_002.csv
│   └── ... (50 files total = 50,000 orders)
├── transformations/                # Data transformation definitions
│   ├── orders_mv.py                # Main orders materialized view (reads CSVs)
│   ├── approved_orders_mv.sql      # Approved orders filter (SQL)
│   ├── fulfilled_orders_mv.sql     # Fulfilled orders filter (SQL)
│   └── pending_orders_mv.sql       # Pending orders filter (SQL)
├── scripts/                        # Query and analysis scripts
│   ├── generate_csv_data.py        # Generate CSV source files
│   ├── query_tables.py             # Query and filter order data
│   ├── calculate_sales_tax.py      # Sales tax calculations and analytics
│   └── state_analytics.py          # Geographic analytics by US state
├── tests/                          # Test suite for materialized views
│   └── test_materialized_views.py  # 9 comprehensive tests
├── spark-warehouse/                # Generated Spark warehouse data (auto-generated)
└── metastore_db/                   # Derby database files (auto-generated)
```

## Features

- **CSV-Based Ingestion**: Reads order data from CSV files (50,000 orders across 50 files)
- **Realistic Data Pattern**: CSV files serve as source of truth, showcasing external data ingestion
- **Geographic Analytics**: Order distribution and revenue analysis across all 50 US states
- **Reproducible Data Generation**: Random state support for deterministic data generation
- **Materialized Views**: Declarative transformations using Python and SQL
- **Order Status Filtering**: Separate views for approved, fulfilled, pending, and cancelled orders
- **Sales Tax Calculations**: Comprehensive tax computation and analytics
- **Analytics Queries**: Ready-to-use query scripts for data analysis with filtering options
- **Data Generation Script**: Easily generate new CSV datasets with custom configurations

## Running the Pipeline

### Option 1: Using the Shell Script (Recommended)

The shell script automatically handles CSV generation if needed:

```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/daily_orders
./run_pipeline.sh
```

This script will:
1. Check for CSV files in `data/` directory
2. Generate 50 CSV files (50,000 orders) if they don't exist
3. Clean Spark warehouse and metastore
4. Run the SDP pipeline

### Option 2: Using Python Directly

```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp
uv run python main.py daily-orders
```

### Option 3: Using SDP CLI

```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/daily_orders
# First, generate CSV data if needed
uv run python scripts/generate_csv_data.py

# Then run the pipeline
spark-pipelines run --conf spark.sql.catalogImplementation=hive --conf spark.sql.warehouse.dir=spark-warehouse
```

## Data Generation

### Generating CSV Source Data

The pipeline reads order data from CSV files. Generate them using:

```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/daily_orders

# Generate 50 files with 1000 orders each (50,000 total orders)
uv run python scripts/generate_csv_data.py

# Custom configuration
uv run python scripts/generate_csv_data.py --num-files 100 --orders-per-file 500

# Regenerate data (clean existing files first)
uv run python scripts/generate_csv_data.py --clean

# Generate with reproducible random data
uv run python scripts/generate_csv_data.py --random-state 42
```

**Key Points:**
- CSV files are generated using `order_gen_util.py` for data consistency
- Default: 50 files × 1000 orders = 50,000 total orders
- Files are named sequentially: `orders_001.csv` through `orders_050.csv`
- Supports `--random-state` flag for reproducible data generation
- Order statuses include: **approved**, **fulfilled**, **pending**, **cancelled**
- Data includes realistic product catalog with 20 different items
- Each order is associated with a random US state (all 50 states)

## Data Transformations

### 1. Orders Materialized View (orders_mv.py)

The base materialized view that reads order data from CSV files:

- **Name**: `orders_mv`
- **Type**: Python transformation with `@dp.materialized_view` decorator
- **Data Source**: CSV files in `data/` directory (batch read with wildcard pattern)
- **Schema**:
  - `order_id`: Unique order identifier (UUID)
  - `order_item`: Product name (20 possible items)
  - `price`: Item price (float, $10-$1000 range)
  - `items_ordered`: Quantity ordered (integer, 1-10 range)
  - `status`: Order status (approved/fulfilled/pending/cancelled)
  - `state`: US state where order was placed (all 50 states)
  - `date_ordered`: Order date (within last 30 days)
- **Implementation**: Uses explicit schema definition and reads all CSV files with `spark.read.csv("data/*.csv")`

### 2. Approved Orders View (approved_orders_mv.sql)

SQL-based materialized view filtering for approved orders:

```sql
CREATE MATERIALIZED VIEW approved_orders_mv AS
SELECT * FROM orders_mv
WHERE status = 'approved';
```

### 3. Fulfilled Orders View (fulfilled_orders_mv.sql)

SQL-based materialized view for fulfilled orders:

```sql
CREATE MATERIALIZED VIEW fulfilled_orders_mv AS
SELECT * FROM orders_mv
WHERE status = 'fulfilled';
```

### 4. Pending Orders View (pending_orders_mv.sql)

SQL-based materialized view for pending orders:

```sql
CREATE MATERIALIZED VIEW pending_orders_mv AS
SELECT * FROM orders_mv
WHERE status = 'pending';
```

## Query Scripts

### 1. Query Tables (scripts/query_tables.py)

Queries the orders materialized views with filtering options and displays orders with all fields including state.

**Run the script:**
```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/daily_orders

# Query all orders
uv run python scripts/query_tables.py

# Query by status
uv run python scripts/query_tables.py --status approved
uv run python scripts/query_tables.py --status fulfilled --limit 20

# List all available tables
uv run python scripts/query_tables.py --list
```

**Sample Output:**
```
+------------------------------------+--------+-------------+------+
|order_id                            |status  |order_item   |price |
+------------------------------------+--------+-------------+------+
|8552e9db-0c10-4fe6-92d5-a0c950a0975a|approved|Scooter      |905.52|
|9346dcad-41c2-43ed-abe5-10b2749e23e3|approved|Headphones   |491.98|
|efebeee5-1cb6-42fc-a67a-9e46e416ca8b|approved|Board Game   |694.2 |
|a6197d4f-7b1b-44a7-bfef-5d74cdde1967|approved|Tennis Racket|185.34|
|91a83429-47be-48f8-92d2-d80ea960e313|approved|Headphones   |232.26|
|0d280b01-55ba-4674-ac71-98ffcae76708|approved|Video Game   |250.04|
|4b8e8c2f-c405-49b1-a26e-b9190a723cbf|approved|Basketball   |525.96|
|d8992d6f-ee64-42ea-8008-f31c03e3618d|approved|Action Figure|12.09 |
|daea3b08-d9c8-4276-bc7c-c7bc6d7e2c1e|approved|Video Game   |114.82|
|bc2bcb5c-ea2e-4f8e-b32c-978445f7a9fa|approved|Tennis Racket|726.08|
+------------------------------------+--------+-------------+------+
only showing top 10 rows
```

### 2. Calculate Sales Tax (scripts/calculate_sales_tax.py)

Calculates total order prices and applies 15% sales tax to approved orders. Provides detailed analytics including:
- Individual order totals with tax
- Summary statistics (total sales, tax collected, grand total)
- Breakdown by order item

**Run the script:**
```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/daily_orders
uv run python scripts/calculate_sales_tax.py
```

**Sample Output:**

#### Approved Orders with Total Prices and Sales Tax (15%)
```
+------------------------------------+-------------+------+-------------+------------+-----------+---------+--------------+
|order_id                            |order_item   |price |items_ordered|date_ordered|total_price|sales_tax|total_with_tax|
+------------------------------------+-------------+------+-------------+------------+-----------+---------+--------------+
|8552e9db-0c10-4fe6-92d5-a0c950a0975a|Scooter      |905.52|6            |2025-09-19  |5433.12    |814.97   |6248.09       |
|9346dcad-41c2-43ed-abe5-10b2749e23e3|Headphones   |491.98|4            |2025-09-30  |1967.92    |295.19   |2263.11       |
|efebeee5-1cb6-42fc-a67a-9e46e416ca8b|Board Game   |694.2 |3            |2025-10-15  |2082.6     |312.39   |2394.99       |
|a6197d4f-7b1b-44a7-bfef-5d74cdde1967|Tennis Racket|185.34|2            |2025-10-06  |370.68     |55.6     |426.28        |
|91a83429-47be-48f8-92d2-d80ea960e313|Headphones   |232.26|9            |2025-10-16  |2090.34    |313.55   |2403.89       |
|0d280b01-55ba-4674-ac71-98ffcae76708|Video Game   |250.04|4            |2025-09-21  |1000.16    |150.02   |1150.18       |
|4b8e8c2f-c405-49b1-a26e-b9190a723cbf|Basketball   |525.96|10           |2025-09-25  |5259.6     |788.94   |6048.54       |
|d8992d6f-ee64-42ea-8008-f31c03e3618d|Action Figure|12.09 |7            |2025-09-23  |84.63      |12.69    |97.32         |
|daea3b08-d9c8-4276-bc7c-c7bc6d7e2c1e|Video Game   |114.82|3            |2025-09-29  |344.46     |51.67    |396.13        |
|bc2bcb5c-ea2e-4f8e-b32c-978445f7a9fa|Tennis Racket|726.08|5            |2025-09-28  |3630.4     |544.56   |4174.96       |
+------------------------------------+-------------+------+-------------+------------+-----------+---------+--------------+
only showing top 10 rows
```

#### Summary Statistics
```
+----------------------+-------------------+--------------------+
|total_sales_before_tax|total_tax_collected|total_sales_with_tax|
+----------------------+-------------------+--------------------+
|83816.45              |12572.47           |96388.92            |
+----------------------+-------------------+--------------------+
```

#### Breakdown by Order Item
```
+---------------+----------------------+-------------------+--------------------+
|order_item     |total_sales_before_tax|total_tax_collected|total_sales_with_tax|
+---------------+----------------------+-------------------+--------------------+
|Scooter        |12917.17              |1937.57            |14854.74            |
|Basketball     |12726.2               |1908.94            |14635.14            |
|Headphones     |11474.02              |1721.1             |13195.12            |
|Action Figure  |7531.81               |1129.76            |8661.57             |
|Video Game     |7306.82               |1096.02            |8402.84             |
|Drone          |7280.52               |1092.08            |8372.6              |
|Toy Car        |4912.7                |736.91             |5649.61             |
|Camera         |4099.16               |614.88             |4714.04             |
|Tennis Racket  |4001.08               |600.16             |4601.24             |
|Laptop         |3585.0                |537.75             |4122.75             |
|Electric Guitar|3459.2                |518.88             |3978.08             |
|Board Game     |2082.6                |312.39             |2394.99             |
|Smartwatch     |1906.84               |286.03             |2192.87             |
|Tablet         |285.65                |42.85              |328.5               |
|Puzzle         |247.68                |37.15              |284.83              |
+---------------+----------------------+-------------------+--------------------+
```

### 3. State Analytics (scripts/state_analytics.py)

Analyzes order distribution and revenue across all 50 US states. Provides geographic insights into sales performance, order status distribution by state, and state-specific detailed analytics.

**Run the script:**
```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/daily_orders

# Show top 10 states by order volume (default)
uv run python scripts/state_analytics.py

# Show top 20 states
uv run python scripts/state_analytics.py --top 20

# Analyze specific state
uv run python scripts/state_analytics.py --state California
uv run python scripts/state_analytics.py --state "New York"

# Show order status distribution by state
uv run python scripts/state_analytics.py --show-status

# Export to CSV
uv run python scripts/state_analytics.py --export state_report.csv
```

**Sample Output - State Distribution:**
```
==========================================================================================
ORDER DISTRIBUTION BY STATE (Top 10)
==========================================================================================
+------------+------------+-------------+---------------+-------------------+
|state       |total_orders|total_revenue|avg_order_price|avg_items_per_order|
+------------+------------+-------------+---------------+-------------------+
|Washington  |1089        |3111420.05   |504.63         |5.68               |
|Kansas      |1057        |2984711.22   |499.94         |5.56               |
|Pennsylvania|1047        |2958265.05   |498.93         |5.61               |
|Oklahoma    |1043        |2996106.15   |513.04         |5.63               |
|Hawaii      |1040        |2940039.89   |511.7          |5.47               |
+------------+------------+-------------+---------------+-------------------+

Total Orders: 50,000
States with Orders: 50
Average Orders per State: 1000.0
```

**Sample Output - State Details (California):**
```
==========================================================================================
DETAILED ANALYTICS FOR CALIFORNIA
==========================================================================================

📊 Overall Statistics:
  - Total Orders: 981
  - Total Revenue: $2,635,554.17
  - Average Order Price: $494.18
  - Average Items per Order: 5.42

📋 Order Status Breakdown:
  - Cancelled: 269 (27.4%)
  - Fulfilled: 240 (24.5%)
  - Approved: 238 (24.3%)
  - Pending: 234 (23.9%)

🛒 Top Products:
+---------------+-----------+---------+
|order_item     |order_count|revenue  |
+---------------+-----------+---------+
|Board Game     |63         |168732.42|
|VR Headset     |58         |175516.37|
|Action Figure  |57         |182425.51|
+---------------+-----------+---------+
```

## Testing

The Daily Orders pipeline includes comprehensive tests for querying and validating materialized views. All tests use the UV package manager for consistent execution.

### Prerequisites for Testing

Before running tests, ensure:
1. **Dependencies are installed** (including dev dependencies):
   ```bash
   cd /path/to/spark-declarative-pipelines/src/py/sdp
   uv sync --extra dev
   ```

2. **Pipeline has been executed** to create materialized views:
   ```bash
   cd /path/to/spark-declarative-pipelines/src/py/sdp/daily_orders
   ./run_pipeline.sh
   ```

### Running Tests

```bash
# From the daily_orders directory
cd /path/to/spark-declarative-pipelines/src/py/sdp/daily_orders

# Run all tests
uv run pytest tests/ -v

# Run tests with detailed output (shows query results and data)
uv run pytest tests/ -v -s
```

### Running Specific Tests

```bash
cd /path/to/spark-declarative-pipelines/src/py/sdp/daily_orders

# Run only materialized view tests
uv run pytest tests/test_materialized_views.py -v

# Run a specific test function
uv run pytest tests/test_materialized_views.py::test_query_orders_mv -v

# Run tests matching a pattern
uv run pytest -k "orders" -v
```

### Test Coverage

The test suite includes **9 test functions** covering:

#### Core Materialized View Tests
- `test_query_orders_mv` - Queries base orders_mv and validates schema/data
- `test_query_approved_orders_mv` - Validates approved orders filtering
- `test_query_fulfilled_orders_mv` - Validates fulfilled orders filtering
- `test_query_pending_orders_mv` - Validates pending orders filtering

#### Data Validation Tests
- `test_verify_status_distribution` - Ensures status views sum to total orders
- `test_verify_column_consistency` - Validates consistent schema across all views

#### Analytics Tests
- `test_query_orders_by_price_range` - Analyzes price distribution (< $100, $100-$500, >= $500)
- `test_query_orders_by_item` - Groups orders by product with statistics
- `test_query_orders_by_date_range` - Validates date ranges

### Expected Test Output

```
============================= test session starts ==============================
collected 9 items

tests/test_materialized_views.py::test_query_orders_mv PASSED           [ 11%]
tests/test_materialized_views.py::test_query_approved_orders_mv PASSED  [ 22%]
tests/test_materialized_views.py::test_query_fulfilled_orders_mv PASSED [ 33%]
tests/test_materialized_views.py::test_query_pending_orders_mv PASSED   [ 44%]
tests/test_materialized_views.py::test_verify_status_distribution PASSED [ 55%]
tests/test_materialized_views.py::test_verify_column_consistency PASSED [ 66%]
tests/test_materialized_views.py::test_query_orders_by_price_range PASSED [ 77%]
tests/test_materialized_views.py::test_query_orders_by_item PASSED      [ 88%]
tests/test_materialized_views.py::test_query_orders_by_date_range PASSED [100%]

========================= 9 passed in 7.07s ============================
```

### Sample Test Output with Details

When running with `-s` flag, tests show query results:

```
=== orders_mv Sample Data ===
+------------------------------------+----------+------+-------------+---------+------------+
|order_id                            |order_item|price |items_ordered|status   |date_ordered|
+------------------------------------+----------+------+-------------+---------+------------+
|067f85a9-b726-43f2-a318-fdaf974d0c5f|Board Game|923.22|7            |pending  |2025-09-25  |
|8552e9db-0c10-4fe6-92d5-a0c950a0975a|Scooter   |905.52|6            |approved |2025-09-19  |
+------------------------------------+----------+------+-------------+---------+------------+
Total rows: 100

=== Status Distribution ===
Total orders: 100
Approved: 35
Fulfilled: 29
Pending: 36
Sum: 100
```

## Key Insights from Sales Data

Based on the sample output above:

- **Total Revenue**: $83,816.45 in sales before tax
- **Tax Revenue**: $12,572.47 collected at 15% tax rate
- **Grand Total**: $96,388.92 including tax
- **Top Selling Items**:
  1. Scooter: $14,854.74 (with tax)
  2. Basketball: $14,635.14 (with tax)
  3. Headphones: $13,195.12 (with tax)

## Technical Details

### Data Generation

The pipeline uses the `order_gen_util.create_random_order_items()` function to generate synthetic order data with realistic attributes:
- Random product selection from a predefined catalog
- Randomized pricing and quantities
- Varied order statuses (approved, pending, fulfilled)
- Date generation for temporal analysis

### Materialized View Pattern

The pipeline demonstrates the SDP framework's hybrid approach:
- **Python transformations**: Base data generation using decorators
- **SQL transformations**: Downstream filtering and aggregations
- **Dynamic module loading**: Cross-pipeline code sharing via importlib

### Pipeline Configuration

The `pipeline.yml` file uses glob patterns to auto-discover transformations:
```yaml
name: daily_orders
storage: storage-root
libraries:
  - glob:
      include: transformations/**
```

## Dependencies

- PySpark 4.1.0.dev3
- Faker (for data generation)
- Plotly (for visualization capabilities)

## Next Steps

1. **Extend Analytics**: Add more complex aggregations and time-series analysis
2. **Add Visualizations**: Create Plotly charts for sales trends
3. **Customer Segmentation**: Add customer data and segmentation views
4. **Inventory Integration**: Track inventory levels based on order data
5. **Revenue Forecasting**: Build predictive models for sales forecasting

## Related Pipelines

- **Oil Rigs Pipeline**: Industrial IoT sensor monitoring example ([../oil_rigs/](../oil_rigs/))
- **Music Analytics Pipeline**: Lakeflow Declarative Pipelines example ([../../ldp/music_analytics/](../../ldp/music_analytics/))
