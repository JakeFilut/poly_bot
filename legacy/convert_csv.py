#!/usr/bin/env python3
"""
Convert Historical_Data.csv from old format to new universally-parseable format.

Old format issues:
- Timestamp like "Feb 05 2026 12 15" (spaces, unusual format)
- csv.QUOTE_ALL causing all values to be quoted including empty strings

New format:
- Timestamp like "2026-02-05 12:15:00" (ISO standard)
- csv.QUOTE_MINIMAL for better compatibility
"""

import csv
import os
import sys
from datetime import datetime

MONTH_MAP = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}

def convert_timestamp(old_ts):
    """Convert 'Feb 05 2026 12 15' to '2026-02-05 12:15:00'"""
    old_ts = old_ts.strip()

    # Already in new format?
    if ':' in old_ts:
        return old_ts

    parts = old_ts.split(' ')
    if len(parts) >= 5:
        month_abbr = parts[0]
        day = int(parts[1])
        year = int(parts[2])
        hour = int(parts[3])
        minute = int(parts[4])
        month = MONTH_MAP.get(month_abbr, 1)
        return f"{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:00"

    # Can't parse, return as-is
    return old_ts

def convert_csv(input_path, output_path=None):
    """Convert the CSV file to new format."""
    if output_path is None:
        output_path = input_path

    # Read all rows
    rows = []
    with open(input_path, 'r', newline='') as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        print("Empty file, nothing to convert")
        return

    # Convert timestamps (skip header row)
    converted = 0
    for i in range(1, len(rows)):
        if rows[i] and rows[i][0]:
            old_ts = rows[i][0]
            new_ts = convert_timestamp(old_ts)
            if new_ts != old_ts:
                rows[i][0] = new_ts
                converted += 1

    # Write back with minimal quoting
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)

    print(f"Converted {converted} timestamps")
    print(f"Output written to: {output_path}")

if __name__ == "__main__":
    input_file = sys.argv[1] if len(sys.argv) > 1 else "Historical_Data.csv"

    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        sys.exit(1)

    # Backup original
    backup_file = input_file + ".backup"
    if not os.path.exists(backup_file):
        import shutil
        shutil.copy(input_file, backup_file)
        print(f"Backup created: {backup_file}")

    convert_csv(input_file)
    print("\nDone! Your CSV should now be parseable by most tools.")
