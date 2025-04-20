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

# Create a temporary Python script to view Parquet file
cat > /tmp/parquet_viewer.py << 'EOF'
import sys
import os
import json
import pandas as pd

def read_parquet_file(file_path):
    """Read and parse a Parquet file."""
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist")
        sys.exit(1)
    
    try:
        # Read Parquet file into DataFrame
        df = pd.read_parquet(file_path)
        
        # Get file metadata
        file_info = os.stat(file_path)
        
        # Extract schema information
        schema = pd.io.parquet.api.fastparquet.ParquetFile(file_path).schema
        
        # Format the results
        result = {
            "file_size": file_info.st_size,
            "num_rows": len(df),
            "schema": {column: str(dtype) for column, dtype in zip(df.columns, df.dtypes)},
            "rows": []
        }
        
        # Process table_name and data_json columns specially
        if 'table_name' in df.columns and 'data_json' in df.columns:
            # This is our special format with table_name and JSON data
            for i, row in df.iterrows():
                table_name = row['table_name']
                try:
                    data = json.loads(row['data_json'])
                    result["rows"].append({
                        "row_index": i,
                        "table_name": table_name,
                        "data": data
                    })
                except:
                    result["rows"].append({
                        "row_index": i,
                        "table_name": table_name,
                        "data": f"Error parsing JSON: {row['data_json']}"
                    })
        else:
            # Regular Parquet format
            for i, row in df.iterrows():
                result["rows"].append({
                    "row_index": i,
                    "data": row.to_dict()
                })
        
        return result
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print("Usage: python parquet_viewer.py <parquet_file>")
        sys.exit(1)
        
    file_path = sys.argv[1]
    result = read_parquet_file(file_path)
    
    # Print summary information
    print(f"Parquet File: {file_path}")
    print(f"File Size: {result['file_size']} bytes")
    print(f"Number of Rows: {result['num_rows']}")
    print("\nSchema:")
    for column, dtype in result['schema'].items():
        print(f"  {column}: {dtype}")
    
    print("\nRows:")
    for row in result["rows"]:
        print(f"\nRow {row['row_index']}:")
        if 'table_name' in row:
            print(f"  Table: {row['table_name']}")
        
        if isinstance(row['data'], dict):
            for key, value in row['data'].items():
                print(f"  {key}: {value}")
        else:
            print(f"  {row['data']}")

if __name__ == "__main__":
    main()
EOF

# Run the Python script
echo "Analyzing Parquet file: $PARQUET_FILE"
python3 /tmp/parquet_viewer.py "$PARQUET_FILE"

chmod +x "$0"