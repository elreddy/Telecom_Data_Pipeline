#!/bin/bash
current_date=$(date +%Y-%m-%d)

# Define directories
PROCESSED_DIR="/home/lokesh/Telecome_Pipeline/ProcessedDir/Processedbatch_$current_date"
ARCHIVE_DIR="/home/lokesh/Telecome_Pipeline/ArchiveDir"
LOG_FILE="/home/lokesh/Telecome_Pipeline/logs/log_$current_date.log"

# Create archive directory if not exists
mkdir -p "$ARCHIVE_DIR"

# Move processed files to archive
mv "$PROCESSED_DIR" "$ARCHIVE_DIR"

# Check if move operation was successful
if [ $? -eq 0 ]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Achiving files =======================================================================" >> "$LOG_FILE"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] - Processed files moved to archive" >> "$LOG_FILE"
else
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] - ERROR: Failed to move processed files!" >> "$LOG_FILE"
    exit 1
fi