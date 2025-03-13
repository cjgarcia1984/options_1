#!/bin/bash

# Define log file
LOG_FILE="$HOME/options_1/fetch.log"
MAX_LINES=10000

# Run the fetch script and append output to log
/home/chris/options_1/.venv/bin/python /home/chris/options_1/fetch.py >> "$LOG_FILE" 2>&1

# Trim log file to the last 10,000 lines
tail -n $MAX_LINES "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"