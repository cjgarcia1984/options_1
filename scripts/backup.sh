#!/bin/bash

# Directories
SOURCE="/home/chris/options_1/data/options_data.db"
DEST="/mnt/c/options_data"
WORKING_COPY="${DEST}/options_data.db"
TEMP_COPY="${DEST}/options_data_temp.db"

# Create backup directory if it doesn't exist
mkdir -p "${DEST}/backups"

# Timestamp for the backup filename
TIMESTAMP=$(date +"%Y%m%d%H%M%S")

# Backup filename
BACKUP_FILE="${DEST}/backups/options_data_${TIMESTAMP}.db"

# Copy the source to a temporary file
cp "${SOURCE}" "${TEMP_COPY}"

# Move the temporary file to the working copy location (atomic operation)
mv "${TEMP_COPY}" "${WORKING_COPY}"

# Copy the new working copy to a new backup file
cp "${WORKING_COPY}" "${BACKUP_FILE}"

# Keep only the last 10 backups
cd "${DEST}/backups"
ls -tp | grep -v '/$' | tail -n +11 | xargs -I {} rm -- {}

echo "Backup created at ${BACKUP_FILE}"
echo "New working copy updated at ${WORKING_COPY}"
