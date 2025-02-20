#!/bin/bash
current_date=$(date +%Y-%m-%d)

# Define directories
SOURCE_DIRECTORY="/home/lokesh/Telecome_Pipeline/SourceInputDir/$current_date"
LOG_FILE="/home/lokesh/Telecome_Pipeline/logs/log_$current_date.log"
ARCHIVE_DIR="/home/lokesh/Telecome_Pipeline/ArchiveDir"

# Create archive directory if not exists
mkdir -p "$ARCHIVE_DIR"

# Move extracted CSV files to archive
mv "$SOURCE_DIRECTORY" "$ARCHIVE_DIR"

# Check if move operation was successful
if [ $? -eq 0 ]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Achiving files =======================================================================" >> "$LOG_FILE"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] - Extracted files moved to archive" >> "$LOG_FILE"
else
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] - ERROR: Failed to move extracted files!" >> "$LOG_FILE"
    exit 1
fi