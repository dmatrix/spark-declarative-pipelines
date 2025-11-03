#!/bin/bash

echo "🏪 Daily Orders Pipeline - CSV-based Streaming Ingestion"
echo "=========================================================="
echo ""

# Step 1: Generate CSV data files
echo "📊 Step 1/3: Generating CSV data files..."
if [ ! -d "data" ] || [ -z "$(ls -A data/*.csv 2>/dev/null)" ]; then
    echo "  No CSV files found. Generating 50 files with 1000 orders each..."
    uv run python scripts/generate_csv_data.py --num-files 50 --orders-per-file 1000
    echo ""
else
    echo -n "  The Daily Orders csv files exist in the data/. Do you want to regenerate? [Y/N]:"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        echo "  Regenerating CSV files..."
        uv run python scripts/generate_csv_data.py --num-files 50 --orders-per-file 1000 --clean
        echo ""
    else
        echo "  ✓ Using existing CSV files in data/ directory"
        echo ""
    fi
fi

# Step 2: Clean existing Spark warehouse and metastore
echo "🧹 Step 2/3: Cleaning Spark warehouse and metastore..."
# check if a directory exists then remove it
if [ -d "spark-warehouse" ]; then
    rm -rf spark-warehouse
    echo "  ✓ Removed spark-warehouse"
fi

# remove the metastore_db directory if it exists
if [ -d "metastore_db" ]; then
    rm -rf metastore_db
    echo "  ✓ Removed metastore_db"
fi
echo ""

# Step 3: Run the SDP pipeline
echo "⚙️  Step 3/3: Running Spark Declarative Pipeline..."
spark-pipelines run --conf spark.sql.catalogImplementation=hive

echo ""
echo "✅ Pipeline execution complete!"
echo ""
echo "Next steps:"
echo "  - Query orders: uv run python scripts/query_tables.py"
echo "  - Calculate tax: uv run python scripts/calculate_sales_tax.py"
echo "  - Analyze by state: uv run python scripts/state_analytics.py"
echo "  - Run tests: uv run pytest tests/ -v"
