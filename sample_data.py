import pandas as pd
import numpy as np
import os

def sample_large_csv():
    """
    Efficiently sample from a large CSV file using chunk processing
    """
    print("=" * 50)
    print("CSV Data Sampling Script")
    print("=" * 50)
    
    csv_file = 'US_Accidents_March23.csv'
    output_file = 'data.csv'
    target_sample_size = 500000
    
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"ERROR: {csv_file} not found!")
        return
    
    # Get file info
    file_size_mb = os.path.getsize(csv_file) / (1024 ** 2)
    print(f"Found {csv_file} ({file_size_mb:.2f} MB)")
    
    try:
        # First, get the total number of rows efficiently
        print("Counting total rows...")
        
        # Read in chunks to count rows without loading entire file
        chunk_size = 10000
        total_rows = 0
        
        for i, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunk_size)):
            total_rows += len(chunk)
            if i % 100 == 0:  # Progress indicator
                print(f"  Processed {i * chunk_size:,} rows...")
        
        print(f"Total rows in dataset: {total_rows:,}")
        
        # Determine sample size
        actual_sample_size = min(target_sample_size, total_rows)
        print(f"Will sample {actual_sample_size:,} rows")
        
        # Generate random indices for sampling
        print("Generating random sample indices...")
        np.random.seed(42)  # For reproducibility
        sample_indices = sorted(np.random.choice(total_rows, size=actual_sample_size, replace=False))
        sample_set = set(sample_indices)
        
        print("Reading and sampling data...")
        
        # Read the file and collect sampled rows
        sampled_rows = []
        current_row = 0
        
        for chunk_num, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunk_size)):
            # Find which rows from this chunk we need
            chunk_rows_to_sample = []
            
            for i in range(len(chunk)):
                if current_row + i in sample_set:
                    chunk_rows_to_sample.append(i)
            
            if chunk_rows_to_sample:
                sampled_rows.append(chunk.iloc[chunk_rows_to_sample])
            
            current_row += len(chunk)
            
            # Progress indicator
            if chunk_num % 50 == 0:
                print(f"  Processed chunk {chunk_num}, current row: {current_row:,}")
        
        # Combine all sampled data
        print("Combining sampled data...")
        final_sample = pd.concat(sampled_rows, ignore_index=True)
        
        # Save to CSV
        print(f"Saving {len(final_sample):,} rows to {output_file}...")
        final_sample.to_csv(output_file, index=False)
        
        print("=" * 50)
        print("SUCCESS!")
        print(f"Created {output_file} with {len(final_sample):,} rows")
        print(f"Columns: {len(final_sample.columns)}")
        print("=" * 50)
        
        # Show preview
        print("Preview of sampled data:")
        print(final_sample.head())
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    sample_large_csv()