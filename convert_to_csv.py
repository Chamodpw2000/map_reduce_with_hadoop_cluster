import pickle
import os

def convert_pickle_to_csv(pickle_file="data.pickle", csv_file="data.csv", include_index=True):
    """
    Convert a pickle file containing a pandas DataFrame to CSV format
    
    Parameters:
    - pickle_file: Path to the input pickle file
    - csv_file: Path to the output CSV file
    - include_index: Whether to include the DataFrame index in the CSV
    
    Returns:
    - True if successful, False otherwise
    """
    
    print(f"Converting '{pickle_file}' to '{csv_file}'...")
    
    try:
        # Load the pickle file
        with open(pickle_file, 'rb') as f:
            data = pickle.load(f)
        
        print(f"Loaded data: {type(data)}")
        
        # Check if it's a DataFrame
        if hasattr(data, 'to_csv'):
            print(f"Data shape: {data.shape}")
            
            # Save to CSV
            data.to_csv(csv_file, index=include_index)
            
            # Get file size
            file_size = os.path.getsize(csv_file) / (1024 * 1024)  # MB
            
            print(f"✅ Successfully saved to '{csv_file}'")
            print(f"📊 Rows: {data.shape[0]:,}, Columns: {data.shape[1]}")
            print(f"💾 File size: {file_size:.2f} MB")
            
            return True
            
        else:
            print(f"❌ Error: Data type {type(data)} cannot be saved as CSV")
            print("Only pandas DataFrames and Series support CSV export")
            return False
            
    except FileNotFoundError:
        print(f"❌ Error: Pickle file '{pickle_file}' not found")
        return False
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def main():
    """Main function with user interaction"""
    
    print("🔄 Pickle to CSV Converter")
    print("=" * 40)
    
    # Check if data.pickle exists
    if not os.path.exists("data.pickle"):
        print("❌ data.pickle file not found in current directory")
        return
    
    # Get output filename
    csv_filename = input("Enter output CSV filename (default: 'accident_data.csv'): ").strip()
    if not csv_filename:
        csv_filename = "accident_data.csv"
    
    # Add .csv extension if not present
    if not csv_filename.endswith('.csv'):
        csv_filename += '.csv'
    
    # Ask about index
    include_index = input("Include row index in CSV? (y/n, default=n): ").lower().strip()
    include_idx = include_index not in ['n', 'no', '']
    
    # Convert
    success = convert_pickle_to_csv("data.pickle", csv_filename, include_idx)
    
    if success:
        print(f"\n🎉 Conversion completed successfully!")
        print(f"📁 Your CSV file is ready: {csv_filename}")
    else:
        print(f"\n💥 Conversion failed!")

if __name__ == "__main__":
    main()