#!/bin/bash

# Directories and File Paths
DESTINATION="/home/chris/options_1/data/options_data.db"
SOURCE="/mnt/t/options_data.db"
TEMP_DESTINATION="${DESTINATION}_temp.db"
LOG_FILE="/home/chris/options_1/logs/sync_log.log"

# Ensure the DESTINATION directory exists
mkdir -p "$(dirname "${DESTINATION}")"

# Copy the working copy to a temporary file in the DESTINATION directory
cp "${SOURCE}" "${TEMP_DESTINATION}"

# Move the temporary file to the DESTINATION location (atomic operation)
mv "${TEMP_DESTINATION}" "${DESTINATION}"

# Logging
{
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Database synchronized to DESTINATION at ${DESTINATION}"
} | tee -a "${LOG_FILE}"

# Ensure the log file does not exceed 10,000 lines
tail -n 10000 "${LOG_FILE}" > "${LOG_FILE}.tmp" && mv "${LOG_FILE}.tmp" "${LOG_FILE}"

echo "Database synchronized to DESTINATION at ${DESTINATION}"
