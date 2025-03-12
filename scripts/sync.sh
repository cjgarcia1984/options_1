#!/bin/bash

# Directories and File Paths
DESTINATION="/home/chris/options_1/data/options_data.db"
SOURCE="/mnt/t/options_data.db"
TEMP_DESTINATION="${DESTINATION}_temp.db"

# Ensure the DESTINATION directory exists
mkdir -p "$(dirname "${DESTINATION}")"

# Copy the working copy to a temporary file in the DESTINATION directory
cp "${SOURCE}" "${TEMP_DESTINATION}"

# Move the temporary file to the DESTINATION location (atomic operation)
mv "${TEMP_DESTINATION}" "${DESTINATION}"

echo "Database synchronized to DESTINATION at ${DESTINATION}"
