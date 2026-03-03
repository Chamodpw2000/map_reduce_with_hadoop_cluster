#!/usr/bin/env python3
"""
Mapper for Weather Condition and Severity Analysis

Input: CSV data with Weather_Condition and Severity columns
Output: key-value pairs where key is weather condition and value is severity

For each Weather_Condition, we need to compute:
- Total accidents (count)
- Average severity (sum of severities / count)
"""

import sys
import csv
from io import StringIO

def mapper():
    """
    Reads CSV data from stdin and emits weather_condition\tseverity pairs
    """
    # Skip the header line
    header_processed = False
    
    for line in sys.stdin:
        line = line.strip()
        
        if not line:
            continue
            
        # Skip header row
        if not header_processed:
            if line.startswith('ID,') or line.startswith('ID\t'):
                header_processed = True
                continue
        
        try:
            # Parse CSV line
            # Use StringIO to handle comma-separated values properly
            csv_reader = csv.reader(StringIO(line))
            row = next(csv_reader)
            
            if len(row) < 29:  # Ensure we have enough columns
                continue
                
            # Extract relevant columns
            # Based on the column order: Weather_Condition is at index 28, Severity at index 2
            weather_condition = row[28].strip()  # Weather_Condition column
            severity = row[2].strip()            # Severity column
            
            # Clean weather condition (remove extra spaces, handle missing values)
            if weather_condition and weather_condition.lower() not in ['', 'null', 'nan', 'none']:
                # Clean severity (ensure it's a valid number)
                try:
                    severity_num = float(severity)
                    if severity_num > 0:  # Valid severity
                        # Emit: weather_condition\tseverity
                        print(f"{weather_condition}\t{severity_num}")
                except ValueError:
                    # Skip rows with invalid severity
                    continue
                    
        except (csv.Error, IndexError, ValueError) as e:
            # Skip malformed lines
            continue

if __name__ == "__main__":
    mapper()