#!/bin/bash

# Script to view the BTree storage in a human-readable format

# Parse input parameters
FILE_PATH=${1:-"data/ulindb.btree"}
TEMP_FILE=/tmp/ulindb_btree_dump.json

# Check if file exists
if [ ! -f "$FILE_PATH" ]; then
    echo "Error: BTree file '$FILE_PATH' not found."
    echo "Usage: ./scripts/view_btree.sh [path_to_btree_file]"
    exit 1
fi

# Create a simple helper Go program to dump BTree contents
cat > /tmp/btree_dump.go << 'EOF'
package main

import (
    "encoding/binary"
    "encoding/json"
    "fmt"
    "os"
)

func main() {
    if len(os.Args) < 2 {
        fmt.Println("Usage: go run btree_dump.go <btree_file_path>")
        os.Exit(1)
    }

    filePath := os.Args[1]
    file, err := os.Open(filePath)
    if err != nil {
        fmt.Printf("Error opening file: %v\n", err)
        os.Exit(1)
    }
    defer file.Close()

    // Get file info
    fileInfo, err := file.Stat()
    if err != nil {
        fmt.Printf("Error getting file info: %v\n", err)
        os.Exit(1)
    }

    if fileInfo.Size() == 0 {
        fmt.Println("BTree file is empty")
        os.Exit(0)
    }

    pageSize := 4096 // Same as in btree_storage.go
    numPages := (fileInfo.Size() + int64(pageSize) - 1) / int64(pageSize)
    
    // Output structured JSON
    result := map[string]interface{}{
        "file_size": fileInfo.Size(),
        "num_pages": numPages,
        "pages": []interface{}{},
    }

    pages := []interface{}{}
    
    // Read root offset
    var rootOffset int64
    binary.Read(file, binary.BigEndian, &rootOffset)
    result["root_offset"] = rootOffset

    // Read each page
    buffer := make([]byte, pageSize)
    for i := int64(0); i < numPages; i++ {
        offset := i * int64(pageSize)
        
        // Read the page
        _, err := file.Seek(offset, 0)
        if err != nil {
            fmt.Printf("Error seeking to page %d: %v\n", i, err)
            continue
        }
        
        n, err := file.Read(buffer)
        if err != nil {
            fmt.Printf("Error reading page %d: %v\n", i, err)
            continue
        }
        
        if n < 16 { // Need at least header
            continue
        }
        
        // Extract node data
        numKeys := binary.BigEndian.Uint64(buffer[0:])
        isLeaf := binary.BigEndian.Uint64(buffer[8:]) == 1
        
        page := map[string]interface{}{
            "page_num": i,
            "offset": offset,
            "is_root": offset == rootOffset,
            "is_leaf": isLeaf,
            "num_keys": numKeys,
            "keys": []interface{}{},
            "values": []interface{}{},
        }

        // Extract keys and values
        keys := []interface{}{}
        values := []interface{}{}
        
        bufOffset := int64(16) // Skip header
        pageSizeInt64 := int64(pageSize)
        
        for j := uint64(0); j < numKeys && bufOffset < pageSizeInt64-8; j++ {
            // Read key
            if bufOffset+4 > pageSizeInt64 {
                break
            }
            keyLen := binary.BigEndian.Uint32(buffer[bufOffset:])
            bufOffset += 4
            
            if bufOffset+int64(keyLen) > pageSizeInt64 {
                break
            }
            key := string(buffer[bufOffset:bufOffset+int64(keyLen)])
            bufOffset += int64(keyLen)
            
            // Read value
            if bufOffset+4 > pageSizeInt64 {
                break
            }
            valueLen := binary.BigEndian.Uint32(buffer[bufOffset:])
            bufOffset += 4
            
            if bufOffset+int64(valueLen) > pageSizeInt64 {
                break
            }
            
            // Try to decode value as JSON
            valueBytes := buffer[bufOffset:bufOffset+int64(valueLen)]
            var valueObj interface{}
            if err := json.Unmarshal(valueBytes, &valueObj); err == nil {
                values = append(values, valueObj)
            } else {
                values = append(values, fmt.Sprintf("<binary data: %d bytes>", valueLen))
            }
            
            bufOffset += int64(valueLen)
            
            keys = append(keys, key)
        }
        
        page["keys"] = keys
        page["values"] = values
        
        pages = append(pages, page)
    }
    
    result["pages"] = pages
    
    // Output as formatted JSON
    jsonData, err := json.MarshalIndent(result, "", "  ")
    if err != nil {
        fmt.Printf("Error formatting JSON: %v\n", err)
        os.Exit(1)
    }
    
    fmt.Println(string(jsonData))
}
EOF

# Compile and run the helper program
echo "Analyzing BTree file: $FILE_PATH"
go run /tmp/btree_dump.go "$FILE_PATH" > "$TEMP_FILE"

# Check if jq is available
if command -v jq &> /dev/null; then
    # Pretty-print the JSON with jq if available
    jq . "$TEMP_FILE"
    echo -e "\nDetailed structure saved to $TEMP_FILE"
else
    # Otherwise just cat the file
    cat "$TEMP_FILE"
    echo -e "\nInstall 'jq' for better formatting. Output saved to $TEMP_FILE"
fi

# Make the file executable
chmod +x "$0"