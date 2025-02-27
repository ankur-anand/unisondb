#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <gc_log.txt>"
    exit 1
fi

GC_LOG_FILE="$1"
GC_CSV_FILE="gc_data.csv"

awk '/gc [0-9]+ @/ {print $2, $9+0, $11+0}' "$GC_LOG_FILE" > "$GC_CSV_FILE"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source venv/bin/activate
python3 "$SCRIPT_DIR/heap_usage_plot.py" "$GC_CSV_FILE"
deactivate