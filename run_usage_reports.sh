# This script automates the process of generating reports for a list of institutions using a specified log file.

#!/bin/bash

# Check if the log file argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 log_file_path"
    exit 1
fi

# The log file path argument
LOG_FILE="$1"

# Define variables
INSTITUTIONS=("crkn" "University of Toronto" "Université de Montréal" "University of Regina")

PYTHON_SCRIPT="usage_report.py"

# Loop through each institution and run the Python script with the log file
for institution in "${INSTITUTIONS[@]}"; do
    echo "Running report for: $institution with log file: $LOG_FILE"
    python3 "$PYTHON_SCRIPT" "$institution" "$LOG_FILE"
done
