# Hadoop MapReduce - Weather Condition Analysis

This project implements a MapReduce job to analyze weather conditions and accident severity from a 500,000 row accident dataset.

## 📊 Analysis Goal

For each **Weather_Condition**, compute:
- **Total accidents** (count of occurrences)
- **Average severity** (mean of severity values)

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (recommended) OR WSL2 + Java 8/11
- Python 3.x
- 500,000+ row dataset with Weather_Condition and Severity columns

### Option 1: Docker Setup (Recommended)

1. **Clone and Prepare**
   ```bash
   # Ensure data.csv is in the project directory
   ls data.csv  # Should show your dataset
   ```

2. **Start Hadoop Cluster**
   ```bash
   # Windows (PowerShell)
   .\setup_hadoop.ps1
   
   # Linux/Mac
   ./setup_hadoop.sh
   ```

3. **Test MapReduce Scripts**
   ```bash
   python test_mapreduce.py
   ```

4. **Run MapReduce Job**
   ```bash
   python run_hadoop_job.py
   ```

### Option 2: Manual Setup

Follow detailed instructions in [hadoop_setup.md](hadoop_setup.md)

## 📁 Project Structure

```
├── data.csv                    # Your 500K accident dataset
├── docker-compose.yml         # Hadoop cluster configuration
├── mapreduce/
│   ├── mapper.py              # MapReduce mapper
│   └── reducer.py             # MapReduce reducer
├── test_mapreduce.py          # Local testing script
├── run_hadoop_job.py          # Main Hadoop job runner
├── setup_hadoop.ps1           # Windows setup script
├── setup_hadoop.sh            # Linux/Mac setup script
├── hadoop_setup.md            # Detailed setup guide
└── output/                    # Results will be stored here
```

## 🔧 MapReduce Implementation

### Mapper (`mapper.py`)
- **Input**: CSV rows with Weather_Condition and Severity columns
- **Output**: `weather_condition\tseverity` pairs
- **Process**: Parses CSV, extracts weather condition and severity, emits key-value pairs

### Reducer (`reducer.py`)
- **Input**: Sorted weather_condition\tseverity pairs
- **Output**: `weather_condition\ttotal_accidents\taverage_severity`
- **Process**: Groups by weather condition, calculates count and average

### Example Output
```
Weather_Condition         Total_Accidents  Avg_Severity
Clear                     245,832          2.13
Overcast                  45,672           2.28
Light Rain               32,156           2.31
Mostly Cloudy            28,943           2.19
```

## 🖥️ Web Interfaces

Once Hadoop is running, access these web interfaces:

- **HDFS NameNode**: http://localhost:9870
  - Monitor HDFS storage and file system
- **YARN ResourceManager**: http://localhost:8088
  - Track MapReduce jobs and cluster resources
- **JobHistory Server**: http://localhost:19888
  - View completed job history and performance

## 🧪 Testing

### Local Test
```bash
python test_mapreduce.py
```
This runs mapper and reducer on a 1000-row sample to verify correctness.

### Full Hadoop Test
```bash
python run_hadoop_job.py --test
```
This runs local test first, then the full Hadoop job.

## 🎯 Step-by-Step Execution

### 1. Data Preparation
- Ensure `data.csv` contains Weather_Condition (column 28) and Severity (column 2)
- File should have ~500,000 rows

### 2. Hadoop Cluster
```bash
# Start cluster
docker-compose up -d

# Verify services
docker ps
# Should show hadoop-cluster running
```

### 3. MapReduce Job Execution
```bash
python run_hadoop_job.py
```

The script will:
1. ✅ Check Hadoop services
2. ✅ Setup HDFS directories
3. ✅ Upload data to HDFS
4. ✅ Run MapReduce job
5. ✅ Download and display results

## 📊 Expected Results

Sample output format:
```
WEATHER CONDITION ANALYSIS RESULTS
==================================================
Weather Condition         Total Accidents  Avg Severity
Clear                     245,832         2.13
Overcast                  45,672          2.28
Light Rain               32,156          2.31
Mostly Cloudy            28,943          2.19
Scattered Clouds         18,234          2.15
Rain                     12,567          2.35
Heavy Rain               3,456           2.41
Fog                      8,234           2.22
Snow                     2,345           2.38
```

## 🔍 Monitoring Job Progress

### Command Line
```bash
# Watch job progress
docker exec -it hadoop-cluster bash
hadoop job -list
yarn application -list
```

### Web UI
- Visit http://localhost:8088 to see running applications
- Click on ApplicationId to see detailed progress
- View logs and performance metrics

## 🛠️ Troubleshooting

### Common Issues

1. **"Hadoop services not found"**
   ```bash
   docker-compose down
   docker-compose up -d
   # Wait 30 seconds for initialization
   ```

2. **"Permission denied" on scripts**
   ```bash
   chmod +x mapreduce/*.py
   chmod +x *.sh
   ```

3. **"Out of memory" errors**
   - Increase Docker memory allocation to 4GB+
   - Or process data in smaller chunks

### Debug Commands
```bash
# Check Hadoop services
docker exec -it hadoop-cluster jps

# Check HDFS
docker exec -it hadoop-cluster hdfs dfs -ls /

# View logs
docker-compose logs hadoop

# Connect to cluster
docker exec -it hadoop-cluster bash
```

## 📈 Performance Notes

- **Dataset Size**: 500,000 rows (~100MB CSV)
- **Processing Time**: 2-5 minutes depending on hardware
- **Memory Usage**: 2-4GB recommended for Docker
- **Output Size**: Small (few KB for weather condition summaries)

## 🧹 Cleanup

```bash
# Stop Hadoop cluster
docker-compose down

# Remove containers and volumes
docker-compose down -v

# Clean up local files
rm -rf output/
```

## 📚 Learning Objectives

This project demonstrates:
- **Hadoop Ecosystem**: HDFS, YARN, MapReduce
- **Distributed Computing**: Processing large datasets across nodes
- **Hadoop Streaming**: Using Python with Hadoop MapReduce
- **Big Data Patterns**: Map-Reduce paradigm for aggregation
- **Cloud Computing**: Scalable data processing architectures

---

**Happy Hadoop Mapping and Reducing! 🎉**