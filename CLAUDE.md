# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains modern ETL pipeline implementations demonstrating different data processing paradigms and frameworks. The project showcases both **Spark Declarative Pipelines (SDP)** for analytics workloads and **Lakeflow Spark Declarative Pipelines (LSDP)** for streaming data processing:

### SDP Examples (src/py/sdp/)
1. **Daily Orders** (`src/py/sdp/daily_orders/`) - E-commerce order processing and analytics system with CSV-based ingestion
2. **Oil Rigs** (`src/py/sdp/oil_rigs/`) - Industrial IoT sensor monitoring and analysis system

### LSDP Examples (src/py/lsdp/)
1. **Music Analytics** (`src/py/lsdp/music_analytics/`) - Million Song Dataset processing with medallion architecture

The project is structured as a uv-managed Python package with virtual environment isolation and modern dependency management.

## Development Commands

⚠️ **IMPORTANT**: All commands in this section must be executed using `uv run` to ensure they run within the SDP directory's virtual environment (`src/py/sdp/.venv`). The UV tool automatically manages the virtual environment when using `uv run`.

### Environment Setup
```bash
# Navigate to the SDP directory
cd src/py/sdp

# Install dependencies and create virtual environment
uv sync

# Install dev dependencies (for testing, linting, etc.)
uv sync --extra dev

# Note: You don't need to manually activate the venv
# 'uv run' automatically uses .venv when executing commands
```

### Running Pipelines

#### SDP Pipelines
```bash
# IMPORTANT: Ensure UV virtual environment is set up first
cd src/py/sdp && uv sync

# Method 1: Using shell scripts (recommended)
cd src/py/sdp/daily_orders && ./run_pipeline.sh
# OR
cd src/py/sdp/oil_rigs && ./run_pipeline.sh

# Method 2: Using Python main.py with UV
cd src/py/sdp && uv run python main.py daily-orders
cd src/py/sdp && uv run python main.py oil-rigs

# Method 3: Using spark-pipelines CLI directly (requires CLI installation)
cd src/py/sdp/daily_orders
spark-pipelines run --conf spark.sql.catalogImplementation=hive --conf spark.sql.warehouse.dir=spark-warehouse
```

#### LSDP Lakeflow Spark Declarative Pipelines (on Databricks)
```bash
# Music Analytics pipeline (Lakeflow Spark Declarative Pipelines on Databricks)
cd src/py/lsdp/music_analytics

# View pipeline documentation and architecture
cat README.md

# Deploy to Databricks workspace (requires Databricks environment)
# See transformations/sdp_musical_pipeline.py for implementation
```

### Development and Testing Commands

#### Running Tests
```bash
# IMPORTANT: Install dev dependencies first (includes pytest)
cd src/py/sdp && uv sync --extra dev

# All test commands must use 'uv run' to activate the virtual environment

# Run Daily Orders tests (9 comprehensive tests for materialized views)
cd src/py/sdp/daily_orders
uv run pytest tests/ -v

# Run tests with detailed output (shows query results)
uv run pytest tests/ -v -s

# Run specific test function
uv run pytest tests/test_materialized_views.py::test_query_orders_mv -v

# Run tests matching a pattern
uv run pytest -k "orders" -v
```

#### Code Quality Tools
```bash
# IMPORTANT: All code quality tools must run with 'uv run' from sdp directory

cd src/py/sdp

# Code formatting and linting
uv run black .
uv run flake8 .
uv run mypy .

# Install package in development mode (optional)
uv pip install -e .
```

#### Query and Analysis Scripts
```bash
# IMPORTANT: All commands must run with uv from the sdp directory's virtual environment

# Method 1: Use entry points (requires: cd src/py/sdp && uv sync)
cd src/py/sdp
uv run sdp-daily-orders     # Run Daily Orders queries
uv run sdp-oil-rigs         # Run Oil Rigs queries

# Method 2: Run scripts directly from pipeline directory
cd src/py/sdp/daily_orders
uv run python scripts/generate_csv_data.py       # Generate CSV source files
uv run python scripts/query_tables.py            # Query approved orders
uv run python scripts/calculate_sales_tax.py     # Calculate sales tax and analytics
```

## Architecture Overview

### Core Framework Components

#### SDP (Spark Declarative Pipelines)
- **Framework**: Python decorators and SQL for declarative data transformations
- **Materialized Views**: Data transformations defined with `@sdp.materialized_view` decorator
- **Pipeline Configuration**: YAML files (`pipeline.yml`) define transformation discovery patterns using glob patterns
- **Storage**: Local Spark warehouse with Hive-compatible storage

#### LSDP (Lakeflow Spark Declarative Pipelines on Databricks)
- **Framework**: Databricks native declarative pipeline framework (formerly Delta Live Tables)
- **Medallion Architecture**: Bronze/Silver/Gold data layers with automatic lineage
- **Data Quality**: Built-in expectations and validation with `@dp.expect`
- **Storage**: Delta tables with Unity Catalog integration

### Project Structure Patterns

#### SDP Pipeline Structure
```
sdp_pipeline_name/
├── pipeline.yml              # Pipeline configuration with glob patterns
├── transformations/           # Data transformation definitions
│   ├── *.py                  # Python-based transformations with @sdp.materialized_view
│   └── *.sql                 # SQL-based transformations
├── tests/                    # Test suite for materialized views
│   └── test_materialized_views.py  # Comprehensive tests for querying views
├── artifacts/utils/          # Pipeline-specific utilities
├── run_pipeline.sh           # Pipeline execution script
├── query_tables.py           # Query and display data
├── calculate_sales_tax.py    # Sales tax calculations (BrickFood)
└── *.py                      # Other query and analysis modules
```

#### LSDP Pipeline Structure
```
lsdp_pipeline_name/
├── README.md                 # Comprehensive pipeline documentation
├── images/                   # Pipeline visualization assets
└── transformations/          # LSDP transformation definitions
    └── *.py                  # Python files with @dp.table decorators
```

### Data Flow Architecture

#### SDP Data Flow
1. **Data Generation**: Utility modules generate synthetic data using Faker library
2. **Transformations**: Materialized views process data using both Python and SQL transformations
3. **Storage**: Data persists to Hive-compatible spark-warehouse directory
4. **Analytics**: Query modules provide data access and visualization capabilities

#### LSDP Data Flow (Music Analytics)
1. **Bronze Layer**: Raw data ingestion from Million Song Dataset with Auto Loader (`songs_raw`)
2. **Silver Layer**: Specialized data preparation with comprehensive validation
   - `songs_metadata_silver`: Release and temporal information with year/duration validation
   - `songs_audio_features_silver`: Musical characteristics with tempo/time signature validation
3. **Gold Layer**: Advanced analytics views across three categories:
   - **Temporal Analytics**: `top_artists_by_year`, `yearly_song_stats`, `release_trends_gold`, `artist_location_summary`
   - **Artist Analytics**: `top_artists_overall`, `artist_discography_gold`, `comprehensive_artist_profile_gold`
   - **Musical Analysis**: `musical_characteristics_gold`, `tempo_time_signature_analysis_gold`
4. **Visualization**: Comprehensive README with updated medallion architecture diagrams

### Key Framework Patterns

#### SDP Patterns
- **Decorator-based Transformations**: `@sdp.materialized_view` decorator converts functions to Spark transformations
- **Dynamic Module Loading**: Utility modules loaded via `importlib.util` for cross-pipeline code sharing
- **Hybrid SQL/Python**: SQL files and Python functions seamlessly integrated in transformation pipeline
- **Configuration-driven Discovery**: `pipeline.yml` uses glob patterns to auto-discover transformation files

#### LSDP on Databricks  Patterns
- **Medallion Architecture**: Bronze/Silver/Gold progression with clear data lineage and specialized silver tables
- **Data Quality Framework**: Comprehensive `@dp.expect` decorators for validation rules (tempo ranges, year validation, duration checks)
- **Streaming Ingestion**: Auto Loader for incremental data processing with schema enforcement
- **Declarative Definitions**: `@dp.table` decorators for transformation specification with automatic dependency resolution
- **Specialized Silver Tables**: Domain-focused tables (`metadata_silver`, `audio_features_silver`) for targeted analytics
- **Advanced Gold Analytics**: Multi-dimensional analysis tables combining temporal, artist, and musical perspectives

## Important Dependencies

### SDP Dependencies
- **PySpark 4.1.0rc2**: Core Spark functionality with latest features (installed from local packages)
- **pyspark-connect 4.1.0rc2**: Spark Connect support for remote Spark clusters (installed from local packages)
- **Python 3.12+**: Required for the project
- **faker**: Synthetic data generation for realistic test datasets
- **plotly**: Data visualization capabilities for analytics
- **pytest, black, flake8, mypy**: Development and code quality tools (install with `uv sync --extra dev`)
- **databricks-sdk**: Databricks SDK for platform integration

### LSDP Dependencies
- **Databricks Runtime**: Required for Spark Declarative Pipelines
- **Spark Declarative Pipelines**: Declarative pipeline framework (formerly Delta Live Tables)
- **Auto Loader**: Streaming file ingestion capability
- **Unity Catalog**: Data governance and lineage tracking

## Working with Transformations

### SDP Transformations
- Transformations in `transformations/` directories are auto-discovered via pipeline.yml glob patterns
- Python transformations must use `@sdp.materialized_view` decorator and return DataFrame
- SQL transformations are standard .sql files processed by the SDP framework
- Shared utilities are in `utils/` and loaded dynamically across pipelines

### SDP Testing
- Tests are organized under each pipeline's `tests/` directory (e.g., `daily_orders/tests/`)
- Test files use pytest framework with simple function-based tests (no classes)
- Daily Orders test suite includes 9 comprehensive tests:
  - **Core Tests**: Query all materialized views (orders_mv, approved_orders_mv, fulfilled_orders_mv, pending_orders_mv)
  - **Validation Tests**: Verify status distribution and column consistency
  - **Analytics Tests**: Price range analysis, item-level statistics, date range validation
- Run tests from pipeline directory: `cd daily_orders && uv run pytest tests/ -v`
- All tests validate data integrity, schema consistency, and analytical queries

### LSDP on Databricks Transformations
- Use `@dp.table` decorator to define materialized views
- Apply `@dp.expect` decorators for data quality validation
- Leverage `dp.read()` for referencing upstream tables
- Auto Loader handles streaming data ingestion with schema evolution

## Project Structure Overview
```
etl-pipelines/
├── CLAUDE.md                 # Claude Code guidance (this file)
├── README.md                 # Project overview and setup guide
└── src/py/
    ├── sdp/                  # Spark Declarative Pipelines
    │   ├── pyproject.toml    # UV project configuration
    │   ├── README.md         # Comprehensive SDP documentation
    │   ├── daily_orders/     # E-commerce analytics pipeline (CSV-based)
    │   │   ├── data/         # CSV source files (50 files × 1000 orders = 50,000 total)
    │   │   ├── scripts/      # Query and analysis scripts
    │   │   │   ├── generate_csv_data.py   # Generate CSV source files
    │   │   │   ├── query_tables.py        # Query and filter orders
    │   │   │   ├── calculate_sales_tax.py # Sales tax analytics
    │   │   │   └── state_analytics.py     # Geographic analytics by state
    │   │   ├── tests/        # Test suite with 9 tests for materialized views
    │   │   └── transformations/  # Orders materialized views (Python + SQL)
    │   ├── oil_rigs/         # IoT sensor monitoring pipeline
    │   └── utils/            # Shared utilities (order_gen_util, oil_gen_util)
    ├── lsdp/                 # Lakeflow Spark Declarative Pipelines (on Databricks)
    │   └── music_analytics/  # Million Song Dataset processing
    └── generators/           # Cross-framework data generators
```

## Daily Orders Pipeline Details

The Daily Orders pipeline demonstrates e-commerce order processing with CSV-based ingestion and comprehensive testing:

### CSV-Based Architecture
- **Data Source**: 50 CSV files in `data/` directory (50,000 total orders)
- **Ingestion Pattern**: Batch reading with wildcard pattern `data/*.csv`
- **Data Generation**: `scripts/generate_csv_data.py` using `order_gen_util.py` for consistency
- **Schema**: Explicit schema definition (order_id, order_item, price, items_ordered, status, state, date_ordered)
- **Statuses**: approved, fulfilled, pending, cancelled (4 status types)

### Materialized Views
1. **orders_mv** - Base orders table reading from CSV files (50,000 orders from 50 files)
2. **approved_orders_mv** - Filtered view for approved orders
3. **fulfilled_orders_mv** - Filtered view for fulfilled orders
4. **pending_orders_mv** - Filtered view for pending orders

### Data Generation Scripts
- **scripts/generate_csv_data.py** - Generate CSV source files
  - Default: 50 files × 1000 orders = 50,000 orders
  - Reuses `order_gen_util.py` logic (DRY principle)
  - Configurable via CLI args: --num-files, --orders-per-file, --clean, --random-state
  - Supports reproducible data generation with --random-state flag

### Analytics Scripts
- **scripts/query_tables.py** - Queries and displays orders with filtering options
  - Supports --list flag to show all available materialized views
  - Supports --status flag to filter by order status (orders, approved, fulfilled, pending, cancelled)
  - Includes state field in output for geographic analysis
- **scripts/calculate_sales_tax.py** - Calculates 15% sales tax, provides:
  - Individual order totals with tax
  - Summary statistics (total sales, tax collected, grand total)
  - Breakdown by product item
- **scripts/state_analytics.py** - Geographic analytics by US state
  - Displays order distribution and revenue across all 50 states
  - State-specific detailed analytics (--state flag)
  - Order status distribution by state (--show-status flag)
  - CSV export capability (--export flag)

### Test Suite (tests/test_materialized_views.py)
All 9 tests pass successfully and cover:
- Querying all 4 materialized views
- Validating status distribution (sum of status views = total orders)
- Verifying column consistency across views
- Price range analytics (< $100, $100-$500, >= $500)
- Product-level statistics and aggregations
- Date range validation

### Key Architectural Pattern
The pipeline demonstrates a realistic data engineering pattern:
- CSV files serve as the **source of truth** (external data ingestion)
- Materialized views process and transform the CSV data
- Showcases SDP's ability to work with external file sources
- Contrast with Oil Rigs pipeline which generates data in-memory