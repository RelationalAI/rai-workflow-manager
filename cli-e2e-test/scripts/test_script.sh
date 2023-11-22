#!/bin/bash

# Specify the folder path
folder_path=$(pwd)

# Use 'find' to get a list of files with '.py' extension and 'grep' to filter
log_files=$(find "$folder_path" -type f -name "*.py" 2>/dev/null)

# Check if any log files were found
if [ -n "$log_files" ]; then
    echo "Python scripts files in $folder_path:"
    echo "$log_files"
else
    echo "No log files found in $folder_path."
fi