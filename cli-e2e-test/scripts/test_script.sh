#!/bin/bash

# Specify the folder path
folder_path=$(pwd)

# Use 'find' to get a list of files with '.py' extension and 'grep' to filter
log_files=$(find "$folder_path" -type f -name "*.py" 2>/dev/null)

# Check if any python files were found
if [ -n "$log_files" ]; then
    echo "Python files in $folder_path:"
    echo "$log_files"
else
    echo "No python files found in $folder_path."
fi