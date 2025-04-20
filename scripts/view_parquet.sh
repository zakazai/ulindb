#!/bin/bash

# Script to view Parquet files in a human-readable format

# Parse input parameters
PARQUET_DIR=${1:-"data/parquet"}
TABLE_NAME=$2

if [ -z "$TABLE_NAME" ]; then
    echo "Usage: ./scripts/view_parquet.sh [parquet_dir] [table_name]"
    echo "Example: ./scripts/view_parquet.sh data/parquet users"
    exit 1
fi

PARQUET_FILE="$PARQUET_DIR/$TABLE_NAME.parquet"

# Check if file exists
if [ ! -f "$PARQUET_FILE" ]; then
    echo "Error: Parquet file for table '$TABLE_NAME' not found at $PARQUET_FILE"
    exit 1
fi

# Install required packages if not present
pip_list=$(pip3 list)
if ! echo "$pip_list" | grep -q "pandas"; then
    echo "Installing required packages..."
    pip3 install pandas pyarrow fastparquet --quiet
fi

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
VIEWER_SCRIPT="$SCRIPT_DIR/parquet_viewer.py"

# Check if the viewer script exists
if [ ! -f "$VIEWER_SCRIPT" ]; then
    echo "Error: Parquet viewer script not found at $VIEWER_SCRIPT"
    exit 1
fi

# Run the Python script
echo "Analyzing Parquet file: $PARQUET_FILE"
python3 "$VIEWER_SCRIPT" "$PARQUET_FILE"