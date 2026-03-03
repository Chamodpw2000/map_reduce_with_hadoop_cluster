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
                    print(f"{current_weather}\t{current_count}\t{avg_severity:.2f}")
                
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
        print(f"{current_weather}\t{current_count}\t{avg_severity:.2f}")

if __name__ == "__main__":
    reducer()