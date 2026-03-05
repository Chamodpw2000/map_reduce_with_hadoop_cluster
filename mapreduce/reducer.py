#!/usr/bin/env python3
"""
Reducer for Weather Condition and Severity Analysis

Input: Sorted key-value pairs from mapper (weather_condition\tseverity)
Output: weather_condition\ttotal_accidents\taverage_severity

For each Weather_Condition, computes:
- Total accidents (count of records)
- Average severity (sum of severities / count)
"""

import sys

# Force UTF-8 encoding for stdin/stdout (Python 3.4 on Debian defaults to ASCII)
if sys.version_info < (3, 7):
    import codecs
    sys.stdin = codecs.getreader('utf-8')(sys.stdin.buffer, errors='replace')
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

def reducer():
    """
    Reads sorted mapper output and calculates statistics for each weather condition
    """
    current_weather = None
    current_count = 0
    current_severity_sum = 0.0
    
    for line in sys.stdin:
        line = line.strip()
        
        if not line:
            continue
            
        try:
            # Parse mapper output: weather_condition\tseverity
            weather_condition, severity_str = line.split('\t')
            severity = float(severity_str)
            
            # If this is a new weather condition, output the previous one
            if current_weather and current_weather != weather_condition:
                if current_count > 0:
                    avg_severity = current_severity_sum / current_count
                    print("{}\t{}\t{:.2f}".format(current_weather, current_count, avg_severity))
                
                # Reset for new weather condition
                current_weather = weather_condition
                current_count = 1
                current_severity_sum = severity
                
            elif current_weather == weather_condition:
                # Same weather condition, accumulate
                current_count += 1
                current_severity_sum += severity
                
            else:
                # First weather condition
                current_weather = weather_condition
                current_count = 1
                current_severity_sum = severity
                
        except ValueError:
            # Skip malformed lines
            continue
    
    # Output the last weather condition
    if current_weather and current_count > 0:
        avg_severity = current_severity_sum / current_count
        print("{}\t{}\t{:.2f}".format(current_weather, current_count, avg_severity))

if __name__ == "__main__":
    reducer()