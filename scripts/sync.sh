#!/bin/bash

# Directories and File Paths
SOURCE="/home/chris/options_1/data/options_data.db"
DEST="/mnt/c/options_data"
WORKING_COPY="${DEST}/options_data.db"
TEMP_SOURCE="${SOURCE}_temp.db"

# Ensure the source directory exists
mkdir -p "$(dirname "${SOURCE}")"

# Copy the working copy to a temporary file in the source directory
cp "${WORKING_COPY}" "${TEMP_SOURCE}"

# Move the temporary file to the source location (atomic operation)
mv "${TEMP_SOURCE}" "${SOURCE}"

echo "Database synchronized to source at ${SOURCE}"
