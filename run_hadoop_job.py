#!/usr/bin/env python3
"""
Run Weather Condition Analysis MapReduce Job on Hadoop Cluster
"""

import subprocess
import sys
import os
import time

class HadoopJobRunner:
    def __init__(self):
        self.hdfs_input_path = "/user/input/weather_data"
        self.hdfs_output_path = "/user/output/weather_analysis"
        self.local_data_file = "data.csv"
        self.local_output_dir = "output"
        
    def check_hadoop_status(self):
        """Check if Hadoop services are running"""
        print("Checking Hadoop services...")
        try:
            result = subprocess.run(['jps'], capture_output=True, text=True)
            if 'NameNode' in result.stdout and 'DataNode' in result.stdout:
                print("✓ Hadoop services are running")
                return True
            else:
                print("✗ Hadoop services not found. Please start Hadoop first.")
                print("Expected services: NameNode, DataNode, ResourceManager, NodeManager")
                print("Current services:")
                print(result.stdout)
                return False
        except FileNotFoundError:
            print("✗ Hadoop not found. Please install and configure Hadoop first.")
            return False
    
    def setup_hdfs_directories(self):
        """Create necessary HDFS directories"""
        print("Setting up HDFS directories...")
        
        commands = [
            f"hdfs dfs -mkdir -p /user",
            f"hdfs dfs -mkdir -p {os.path.dirname(self.hdfs_input_path)}",
            f"hdfs dfs -rm -r {self.hdfs_output_path}",  # Remove existing output
        ]
        
        for cmd in commands:
            try:
                result = subprocess.run(cmd.split(), capture_output=True, text=True)
                if "rm:" in result.stderr and "No such file" in result.stderr:
                    # Expected for first run when output doesn't exist
                    continue
                elif result.returncode != 0 and "File exists" not in result.stderr:
                    print(f"Warning: {cmd} failed: {result.stderr}")
            except Exception as e:
                print(f"Error executing {cmd}: {e}")
        
        print("✓ HDFS directories ready")
    
    def upload_data_to_hdfs(self):
        """Upload CSV data to HDFS"""
        print("Uploading data to HDFS...")
        
        if not os.path.exists(self.local_data_file):
            print(f"✗ Data file {self.local_data_file} not found!")
            return False
        
        # Get file size for progress indication
        file_size_mb = os.path.getsize(self.local_data_file) / (1024 * 1024)
        print(f"Uploading {self.local_data_file} ({file_size_mb:.1f} MB) to {self.hdfs_input_path}")
        
        try:
            cmd = f"hdfs dfs -put {self.local_data_file} {self.hdfs_input_path}"
            result = subprocess.run(cmd.split(), capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✓ Data uploaded successfully")
                return True
            else:
                print(f"✗ Upload failed: {result.stderr}")
                return False
        except Exception as e:
            print(f"✗ Upload error: {e}")
            return False
    
    def run_mapreduce_job(self):
        """Run the MapReduce job using Hadoop Streaming"""
        print("Starting MapReduce job...")
        
        # Construct Hadoop Streaming command
        streaming_jar = "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.1.jar"
        
        # For Docker, the path might be different
        if not os.path.exists(streaming_jar):
            # Common alternative paths
            alt_paths = [
                "/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.0.jar",
                "/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar"
            ]
            
            for path in alt_paths:
                if "*" in path:
                    # Use find command for wildcard
                    try:
                        result = subprocess.run(f"find {os.path.dirname(path)} -name {os.path.basename(path)}", 
                                              shell=True, capture_output=True, text=True)
                        if result.stdout.strip():
                            streaming_jar = result.stdout.strip().split('\n')[0]
                            break
                    except:
                        continue
                elif os.path.exists(path):
                    streaming_jar = path
                    break
        
        print(f"Using streaming jar: {streaming_jar}")
        
        # MapReduce command
        cmd = [
            "hadoop", "jar", streaming_jar,
            "-files", "mapreduce/mapper.py,mapreduce/reducer.py",
            "-mapper", "python3 mapper.py",
            "-reducer", "python3 reducer.py",
            "-input", self.hdfs_input_path,
            "-output", self.hdfs_output_path
        ]
        
        print(f"Running command: {' '.join(cmd)}")
        print("This may take several minutes depending on data size...")
        
        start_time = time.time()
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                                     text=True, bufsize=1, universal_newlines=True)
            
            # Show progress
            for line in process.stdout:
                line = line.strip()
                if line:
                    # Filter interesting log messages
                    if any(keyword in line.lower() for keyword in ['map', 'reduce', 'completed', 'running', 'failed', 'error']):
                        print(f"  {line}")
            
            process.wait()
            
            if process.returncode == 0:
                elapsed = time.time() - start_time
                print(f"✓ MapReduce job completed successfully in {elapsed:.1f} seconds")
                return True
            else:
                print(f"✗ MapReduce job failed with return code {process.returncode}")
                return False
                
        except Exception as e:
            print(f"✗ Error running MapReduce job: {e}")
            return False
    
    def download_results(self):
        """Download results from HDFS to local filesystem"""
        print("Downloading results...")
        
        # Create local output directory
        if not os.path.exists(self.local_output_dir):
            os.makedirs(self.local_output_dir)
        
        try:
            # Download the results
            cmd = f"hdfs dfs -get {self.hdfs_output_path}/* {self.local_output_dir}/"
            result = subprocess.run(cmd.split(), capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✓ Results downloaded to {self.local_output_dir}/")
                
                # Display results
                result_file = os.path.join(self.local_output_dir, "part-00000")
                if os.path.exists(result_file):
                    print("\n" + "=" * 70)
                    print("WEATHER CONDITION ANALYSIS RESULTS")
                    print("=" * 70)
                    print(f"{'Weather Condition':<25} {'Total Accidents':<15} {'Avg Severity':<12}")
                    print("-" * 70)
                    
                    with open(result_file, 'r') as f:
                        lines = f.readlines()
                        
                    # Sort by total accidents (descending)
                    results = []
                    for line in lines:
                        if line.strip():
                            parts = line.strip().split('\t')
                            if len(parts) == 3:
                                weather, count, avg_severity = parts
                                results.append((weather, int(count), float(avg_severity)))
                    
                    results.sort(key=lambda x: x[1], reverse=True)  # Sort by count
                    
                    for weather, count, avg_severity in results:
                        print(f"{weather:<25} {count:<15} {avg_severity:<12.2f}")
                    
                    print("=" * 70)
                    print(f"Total weather conditions analyzed: {len(results)}")
                    
                    # Save results as a clean CSV file
                    output_csv = os.path.join(self.local_output_dir, "weather_analysis_results.csv")
                    with open(output_csv, 'w') as csv_out:
                        csv_out.write("Weather_Condition,Total_Accidents,Average_Severity\n")
                        for weather, count, avg_severity in results:
                            csv_out.write(f"{weather},{count},{avg_severity:.2f}\n")
                    
                    print(f"✓ Results saved to: {output_csv}")
                    
                return True
            else:
                print(f"✗ Download failed: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"✗ Download error: {e}")
            return False
    
    def run_complete_job(self):
        """Run the complete MapReduce workflow"""
        print("HADOOP MAPREDUCE: Weather Condition Analysis")
        print("=" * 60)
        
        # Step-by-step execution
        steps = [
            ("Check Hadoop Status", self.check_hadoop_status),
            ("Setup HDFS Directories", self.setup_hdfs_directories),
            ("Upload Data to HDFS", self.upload_data_to_hdfs),
            ("Run MapReduce Job", self.run_mapreduce_job),
            ("Download Results", self.download_results)
        ]
        
        for step_name, step_func in steps:
            print(f"\nStep: {step_name}")
            print("-" * 40)
            
            if not step_func():
                print(f"✗ Failed at step: {step_name}")
                return False
        
        print("\n" + "=" * 60)
        print("✓ Complete workflow finished successfully!")
        print("✓ Weather condition analysis completed!")
        return True

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Run local test first
        print("Running local test first...")
        import subprocess
        result = subprocess.run([sys.executable, "test_mapreduce.py"], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print("Local test failed. Please fix issues before running on Hadoop.")
            return
    
    # Run Hadoop job
    runner = HadoopJobRunner()
    runner.run_complete_job()

if __name__ == "__main__":
    main()