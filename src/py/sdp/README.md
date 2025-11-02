# Spark Declarative Pipelines (SDP) Examples

This directory contains example implementations of **Spark Declarative Pipelines (SDP)**, a framework for building and managing data pipelines using Apache Spark. The SDP framework enables declarative data transformations through Python decorators and SQL, providing a clean and maintainable approach to data pipeline development.

This project is structured as a **uv-managed Python package** with PySpark 4.1.0-dev3 and Spark Connect support, providing a modern development environment with dependency management and virtual environment isolation.

## Overview

The SDP directory demonstrates two complete data processing pipelines:

1. **BrickFood** - An e-commerce order processing and analytics system
2. **Oil Rigs** - An industrial IoT sensor monitoring and analysis system

Each project showcases different aspects of the SDP framework, from synthetic data generation and materialized view creation to business analytics and sensor data visualization.

## Prerequisites and Setup

### Requirements

1. **Python 3.12+**: Required for the project
2. **UV Package Manager**: **Highly recommended** for dependency management and virtual environments
3. **Spark Declarative Pipelines CLI**: Required for running the pipelines (`spark-pipelines` command)

### Installation with UV (Recommended)

**UV** is the recommended Python package manager for this project, providing fast dependency resolution, reproducible builds, and seamless virtual environment management.

1. **Install UV**:
   ```bash
   # macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone and setup the project**:
   ```bash
   cd /path/to/spark-declarative-pipelines/src/py/sdp

   # Install dependencies and create virtual environment
   uv sync
   ```

3. **Verify installation**:
   ```bash
   # Check that PySpark is available
   uv run python -c "import pyspark; print('PySpark version:', pyspark.__version__)"

   # Check SDP CLI availability (required for pipelines)
   spark-pipelines --help
   ```

## Quick Start

### Running Pipelines

```bash
# Navigate to SDP directory
cd src/py/sdp

# Run BrickFood e-commerce pipeline
uv run python main.py brickfood

# Run Oil Rigs sensor monitoring pipeline
uv run python main.py oil-rigs

# Get help
uv run python main.py --help
```

### Testing Utilities

```bash
# Test order generation utility
uv run sdp-test-orders

# Test oil sensor generation utility
uv run sdp-test-oil-sensors
```

## Pipeline Examples

### BrickFood E-commerce Pipeline

Demonstrates a complete e-commerce order processing system with order lifecycle management, financial calculations, and business analytics.

**Key Features:**
- Synthetic e-commerce order data generation
- Order status management (approved, fulfilled, pending)
- Sales tax calculations and analytics
- Product category analysis

**Learn More:** See [brickfood/README.md](brickfood/README.md) for:
- Detailed project structure
- Data transformations and materialized views
- Pipeline execution instructions
- Testing guide
- Query scripts and analytics examples

### Oil Rigs Industrial Monitoring Pipeline

Simulates a comprehensive industrial IoT sensor monitoring system for oil drilling operations, tracking critical operational parameters across multiple geographic locations in Texas.

**Key Features:**
- Multi-location monitoring (Permian Basin, Eagle Ford Shale)
- Realistic sensor data with operational patterns
- Temperature, pressure, and water level tracking
- Interactive time-series visualizations with PySpark 4.0+ native plotting

**Learn More:** See [oil_rigs/README.md](oil_rigs/README.md) for:
- Detailed project structure
- Sensor specifications and data model
- Pipeline execution instructions
- Visualization guide (PySpark native plotting)
- Query scripts and analytics examples

## Shared Utility Functions

The project includes centralized utility functions in the `utils/` directory for data generation that can be used independently or as part of the pipelines:

### Order Generation Utilities (`utils/order_gen_util.py`)

Generates realistic e-commerce order data for testing and development.

**Key Functions:**
- `create_random_order_items(num_items=100)`: Generate random order DataFrame
- 20+ product categories (toys, electronics, sports equipment)
- Realistic price ranges, quantities, and order statuses

**Usage Examples:** See [brickfood/README.md](brickfood/README.md#data-generation)

### Oil Rig Sensor Utilities (`utils/oil_gen_util.py`)

Generates realistic industrial IoT sensor data for oil rig monitoring systems with meaningful operational patterns.

**Key Functions:**
- `create_oil_rig_events_dataframe(rig_name, start_date, num_events)`: Generate complete sensor DataFrame
- `get_available_rigs()`: List configured oil rigs
- `get_rig_info(rig_name)`: Get rig location and specifications

**Supported Rigs:**
- Permian Rig (Midland, Texas)
- Eagle Ford Rig (Karnes City, Texas)

**Sensor Types:** Temperature (°F), Pressure (PSI), Water Level (feet)

**Usage Examples:** See [oil_rigs/README.md](oil_rigs/README.md#realistic-data-generation)

## SDP Framework Architecture

### Core Components

1. **Pipeline Configuration** (`pipeline.yml`)
   - Defines transformation discovery patterns using glob patterns
   - Includes both Python (`.py`) and SQL (`.sql`) transformations

2. **Materialized Views**
   - **Python**: Use `@sdp.materialized_view` decorator for complex data generation
   - **SQL**: Standard SQL DDL for filtering and aggregation operations

3. **Pipeline Execution**
   - Uses `spark-pipelines run` command with Hive catalog support
   - Automatically manages dependencies between materialized views
   - Stores data in local Spark warehouse directories

4. **Shared Utilities**
   - Centralized data generation for consistent test data
   - Reusable components shared across pipelines
   - Parameterized functions with sensible defaults

### Data Storage

- **Spark Warehouse**: Local file-based storage in `spark-warehouse/` directories
- **Metastore**: Derby database for metadata management (`metastore_db/`)
- **Parquet Format**: Efficient columnar storage for analytical queries

## Development Workflow

1. **Define Transformations**: Create materialized views in `transformations/` directory
2. **Use Shared Utilities**: Import utilities for data generation
3. **Configure Pipeline**: Update `pipeline.yml` to include new transformations
4. **Execute Pipeline**: Run using CLI or shell scripts
5. **Query Data**: Use provided query scripts or create custom analytics
6. **Visualize Results**: Generate charts and reports

## Dependencies

### Core Dependencies
- **PySpark 4.1.0-dev3**: Latest Apache Spark features
- **PySpark Connect 4.1.0-dev3**: Spark Connect client for remote clusters
- **Faker 37.6.0+**: Realistic synthetic data generation
- **Plotly 6.3.0+**: Interactive data visualizations

### System Requirements
- **Python 3.12+**: Required minimum version
- **UV Package Manager**: Recommended for dependency management
- **Spark Declarative Pipelines CLI**: External tool for pipeline execution
- **Java 11+**: Required by PySpark (automatically handled by Spark)

## Documentation Links

- **[BrickFood README](brickfood/README.md)**: Complete e-commerce pipeline documentation
- **[Oil Rigs README](oil_rigs/README.md)**: Complete IoT monitoring pipeline documentation
- **[Project Root README](../../../README.md)**: Overall project overview
- **[CLAUDE.md](../../../CLAUDE.md)**: Claude Code configuration and development guidance

## Troubleshooting

### SDP CLI Not Available

If you see an error about `spark-pipelines` command not being available, install the Spark Declarative Pipelines CLI tool and ensure it's in your PATH. The cli is part of PySpark 4.1.0-dev3

### Environment Issues

```bash
# Recreate the virtual environment
uv sync --reinstall

# Check Python version
uv run python --version

# Verify PySpark installation
uv run python -c "import pyspark; print(pyspark.__version__)"
```

### Permission Issues

```bash
# Make sure shell scripts are executable
chmod +x brickfood/run_pipeline.sh
chmod +x oil_rigs/run_pipeline.sh
```

---

*This SDP implementation provides a solid foundation for understanding declarative data pipeline development with Apache Spark, combining Python's flexibility with SQL's simplicity, all managed through modern UV tooling. For detailed usage, testing, and execution instructions, see the respective pipeline README files.*
