import pandas as pd
import matplotlib.pyplot as plt
import os

# Set style
plt.style.use('ggplot')
plt.rcParams['figure.figsize'] = (14, 10)

# Paths
input_file = 'output/weather_analysis_results.txt'
# Using the exact name from user request
output_dir = 'output_graphs' 

def visualize():
    print(f"Reading data from {input_file}...")
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.")
        return

    # Read the tab-separated file
    try:
        df = pd.read_csv(input_file, sep='\t', names=['Weather_Condition', 'Total_Accidents', 'Avg_Severity'])
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Sort by Total_Accidents for better visualization
    df = df.sort_values(by='Total_Accidents', ascending=False)
    
    # Take top 20 for readability
    top_20 = df.head(20)
    
    if top_20.empty:
        print("Error: No data to plot.")
        return

    print(f"Generating charts for top {len(top_20)} weather conditions...")

    # 1. Total Accidents vs Weather Condition
    plt.figure()
    plt.barh(top_20['Weather_Condition'], top_20['Total_Accidents'], color='skyblue')
    plt.xlabel('Total Accidents', fontsize=12)
    plt.ylabel('Weather Condition', fontsize=12)
    plt.title('Top 20 Weather Conditions by Total Accidents', fontsize=16)
    plt.gca().invert_yaxis()  # Put highest at top
    plt.tight_layout()
    accidents_plot = os.path.join(output_dir, 'accidents_by_weather.png')
    plt.savefig(accidents_plot)
    print(f"✓ Saved: {accidents_plot}")

    # 2. Avg Severity vs Weather Condition
    plt.figure()
    # Sort by severity for this chart
    top_20_severity = top_20.sort_values(by='Avg_Severity', ascending=False)
    plt.barh(top_20_severity['Weather_Condition'], top_20_severity['Avg_Severity'], color='salmon')
    plt.xlabel('Average Severity', fontsize=12)
    plt.ylabel('Weather Condition', fontsize=12)
    plt.title('Top 20 Weather Conditions by Average Severity (Highest First)', fontsize=16)
    plt.xlim(0, 4.5)
    plt.gca().invert_yaxis()  # Put highest at top
    plt.tight_layout()
    severity_plot = os.path.join(output_dir, 'severity_by_weather.png')
    plt.savefig(severity_plot)
    print(f"✓ Saved: {severity_plot}")

if __name__ == "__main__":
    visualize()
