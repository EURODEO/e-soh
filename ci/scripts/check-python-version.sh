#!/usr/bin/env bash

# Get Python version
python_version=$(python --version 2>&1 | cut -d' ' -f2)

# Extract major and minor version numbers
major_version=$(echo "$python_version" | cut -d'.' -f1)
minor_version=$(echo "$python_version" | cut -d'.' -f2)

# Check if Python version is greater than or equal to 3.11
if [[ "$major_version" -lt 3 || ( "$major_version" -eq 3 && "$minor_version" -lt 11 ) ]]; then
    echo "Error: Python version must be greater than or equal to 3.11"
    exit 1
fi
