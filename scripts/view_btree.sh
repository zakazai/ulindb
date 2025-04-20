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

# Create a temporary Python script to view BTree file
cat > /tmp/btree_viewer.py << 'EOF'
import sys
import os
import json
import struct

def read_btree_file(file_path):
    """Read and parse a BTree file."""
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist")
        sys.exit(1)
        
    with open(file_path, 'rb') as f:
        # Get file size
        f.seek(0, 2)
        file_size = f.tell()
        f.seek(0)
        
        if file_size == 0:
            return {"file_size": 0, "num_pages": 0, "pages": []}
            
        # Read root offset
        root_offset = struct.unpack('>q', f.read(8))[0]
        
        # Calculate number of pages (assuming 4096-byte pages)
        page_size = 4096
        num_pages = (file_size + page_size - 1) // page_size
        
        result = {
            "file_size": file_size,
            "num_pages": num_pages,
            "root_offset": root_offset,
            "pages": []
        }
        
        # Read each page
        for page_num in range(num_pages):
            page_offset = page_num * page_size
            f.seek(page_offset)
            
            # Read page data
            page_data = f.read(page_size)
            if len(page_data) < 16:  # Need at least header
                continue
                
            # Parse header
            num_keys = struct.unpack('>q', page_data[0:8])[0]
            is_leaf = struct.unpack('>q', page_data[8:16])[0] == 1
            
            page = {
                "page_num": page_num,
                "offset": page_offset,
                "is_root": page_offset == root_offset,
                "is_leaf": is_leaf,
                "num_keys": num_keys,
                "keys": [],
                "values": []
            }
            
            # Parse keys and values
            offset = 16  # Skip header
            for i in range(num_keys):
                if offset + 4 > len(page_data):
                    break
                    
                # Read key
                key_len = struct.unpack('>I', page_data[offset:offset+4])[0]
                offset += 4
                
                if offset + key_len > len(page_data):
                    break
                    
                key = page_data[offset:offset+key_len].decode('utf-8', errors='replace')
                offset += key_len
                
                # Read value
                if offset + 4 > len(page_data):
                    break
                    
                value_len = struct.unpack('>I', page_data[offset:offset+4])[0]
                offset += 4
                
                if offset + value_len > len(page_data):
                    break
                
                # Try to parse value as JSON
                value_bytes = page_data[offset:offset+value_len]
                try:
                    value = json.loads(value_bytes)
                except:
                    value = f"<binary data: {value_len} bytes>"
                    
                offset += value_len
                
                page["keys"].append(key)
                page["values"].append(value)
                
            result["pages"].append(page)
            
        return result

def main():
    if len(sys.argv) < 2:
        print("Usage: python btree_viewer.py <btree_file>")
        sys.exit(1)
        
    file_path = sys.argv[1]
    result = read_btree_file(file_path)
    
    # Pretty print JSON
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
EOF

# Run the Python script
echo "Analyzing BTree file: $FILE_PATH"
python3 /tmp/btree_viewer.py "$FILE_PATH"

chmod +x "$0"