#!/bin/bash

# This makes the script exit immediately on any error
set -e
set -o pipefail  # Fail on pipeline errors

# Script to extract cdr files from the github repository and storing in our machine
# Logging is enabled

current_date=$(date +%Y-%m-%d)

# Define Required directories
SOURCE_DIRECTORY="/home/lokesh/Telecome_Pipeline/SourceInputDir/$current_date"
LOG_DIRECTORY="/home/lokesh/Telecome_Pipeline/logs"

# Create necessary directories
mkdir -p "$SOURCE_DIRECTORY"

# Log file
LOG_FILE="$LOG_DIRECTORY/log_$current_date.log"

# Set full permissions (only for testing, provide required preveilages in production)
chmod 777 "$SOURCE_DIRECTORY"

# Function to log messages
log_message() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Start logging
# For better readability, we use a separator line before and after the log message
log_message "Task E ======================================================================================================================================"
log_message "Script started. Downloading CDR files..."

while read -r url; do
    # Remove any potential carriage return from the URL (in case of Windows-formatted files)
    url=$(echo "$url" | tr -d '\r')

    # Extract the base filename from the URL using the built-in 'basename' command.
    # Alternative approach using awk: filename=$(echo "$url" | awk -F '/' '{print $NF}')
    filename=$(basename "$url")

    # Remove the '.csv' extension and append the current date to the filename.
    # Alternative approach using awk: new_filename="$(echo "$filename" | awk -F '.' '{print $1}')_$current_date.csv"
    new_filename="${filename%.csv}_$current_date.csv"

    # Download the file with logging
    log_message "Downloading: $url -> $SOURCE_DIRECTORY/$new_filename"

    if ! wget "$url" -O "$SOURCE_DIRECTORY/$new_filename" >> "$LOG_FILE" 2>&1; then
        rm -rf "$SOURCE_DIRECTORY/$new_filename"       # Remove the partially downloaded file
        log_message "ERROR: Failed to download $url. Exiting script."
        exit 1     # Exit immediately if a download fails
    fi

    log_message "Successfully downloaded: $new_filename"

done < /home/lokesh/Telecome_Pipeline/Scripts/CDRurls.txt || { log_message "ERROR: Unable to read CDRurls.txt"; exit 1; }

log_message "Script completed successfully."
log_message "End of log for the run."
log_message "======================================================================================================================================"
