#!/bin/bash

# Script to view the BTree storage in a human-readable format

# Parse input parameters
FILE_PATH=${1:-"data/ulindb.btree"}

# Check if file exists
if [ ! -f "$FILE_PATH" ]; then
    echo "Error: BTree file '$FILE_PATH' not found."
    echo "Usage: ./scripts/view_btree.sh [path_to_btree_file]"
    exit 1
fi

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
VIEWER_SCRIPT="$SCRIPT_DIR/btree_viewer.py"

# Check if the viewer script exists
if [ ! -f "$VIEWER_SCRIPT" ]; then
    echo "Error: BTree viewer script not found at $VIEWER_SCRIPT"
    exit 1
fi

# Run the Python script
echo "Analyzing BTree file: $FILE_PATH"
python3 "$VIEWER_SCRIPT" "$FILE_PATH"