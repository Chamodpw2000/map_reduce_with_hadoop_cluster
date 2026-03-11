

# Hadoop MapReduce - Weather Condition Analysis

This project implements a MapReduce job to analyze weather conditions and accident severity from a 500,000-row accident dataset.

## 📊 Analysis Goal

For each **Weather_Condition**, compute:

* **Total accidents** (count of occurrences)
* **Average severity** (mean of severity values)

---

## Quick Start

### Prerequisites

* **Docker Desktop (optional)** OR **WSL2/Linux + Java 11**
* Python 3.x
* Dataset: ~500,000 rows with `Weather_Condition` and `Severity` columns
* dataser link : https://www.kaggle.com/datasets/yuvrajdhepe/us-accidents-processed?resource=download

---

### Option 1: Docker Setup (Recommended)

1. **Clone and Prepare**

```bash
# Ensure data.csv is in the project directory
ls data.csv  # Verify dataset presence
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

4. **Run Full Hadoop Job**

```bash
python run_hadoop_job.py
```

---

### Option 2: Manual Hadoop Setup

This option demonstrates a manual Hadoop installation and execution on a local WSL2/Linux environment.

#### 1. Install Prerequisites

```bash
sudo apt update
sudo apt install openjdk-11-jdk python3 python3-pip -y
java -version       # Verify Java 11
python3 --version
```

#### 2. Download and Configure Hadoop

```bash
wget https://downloads.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
tar -xzvf hadoop-3.4.1.tar.gz
mv hadoop-3.4.1 ~/hadoop
```

Set environment variables in `~/.bashrc`:

```bash
export HADOOP_HOME=~/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
source ~/.bashrc
```

Update `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` to point to Java 11:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```
### 3. Configure SSH for Hadoop

Hadoop requires **passwordless SSH access to localhost** to start its distributed services.

1. Install SSH server

```bash
sudo apt install openssh-server -y
```

2. Start SSH service

```bash
sudo service ssh start
```

3. Generate SSH key

```bash
ssh-keygen -t rsa -P ""
```

Press **Enter** for all prompts.

4. Enable passwordless SSH

```bash
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

5. Test SSH connection

```bash
ssh localhost
```

If configured correctly, it should **log in without asking for a password**.

---

## Then Continue With Existing Steps

After SSH setup, continue with:

### 4. Format HDFS

```bash
hdfs namenode -format
```

### 5. Start Hadoop Services

```bash
start-dfs.sh
start-yarn.sh
jps
```

Expected output:

```
NameNode
DataNode
ResourceManager
NodeManager
```


#### 6. Prepare HDFS and Upload Data

```bash
hdfs dfs -mkdir -p /user/$USER/input
hdfs dfs -put data.csv /user/$USER/input/
hdfs dfs -ls /user/$USER/input
```

#### 7. Run MapReduce Job

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar \
    -input /user/$USER/input/data.csv \
    -output /user/$USER/output \
    -mapper "python3 mapreduce/mapper.py" \
    -reducer "python3 mapreduce/reducer.py"

hdfs dfs -cat /user/$USER/output/part-00000
```

#### 8. Verify and Interpret Results

* Confirm counts and average severity per weather condition
* Monitor progress via:

  * **HDFS NameNode**: [http://localhost:9870](http://localhost:9870)
  * **YARN ResourceManager**: [http://localhost:8088](http://localhost:8088)

#### 9. Cleanup

```bash
hdfs dfs -rm -r /user/$USER/input
hdfs dfs -rm -r /user/$USER/output
stop-dfs.sh
stop-yarn.sh
```

**Notes:**

* Make Python scripts executable: `chmod +x mapreduce/*.py`
* Recommended RAM: 4GB+

---

## 📁 Project Structure

```
├── data.csv                     # Accident dataset
├── docker-compose.yml           # Hadoop cluster config
├── mapreduce/
│   ├── mapper.py                # MapReduce mapper
│   └── reducer.py               # MapReduce reducer
├── test_mapreduce.py            # Local test script
├── run_hadoop_job.py            # Full Hadoop job runner
├── setup_hadoop.ps1             # Windows Docker setup
├── setup_hadoop.sh              # Linux/Mac Docker setup
├── hadoop_setup.md              # Manual Hadoop setup guide
└── output/                      # Results directory
```

---

## 🔧 MapReduce Implementation

### Mapper (`mapper.py`)

* **Input**: CSV rows with `Weather_Condition` and `Severity`
* **Output**: `weather_condition\tseverity` pairs
* **Process**: Parse CSV, extract columns, emit key-value pairs

### Reducer (`reducer.py`)

* **Input**: Sorted `weather_condition\tseverity` pairs
* **Output**: `weather_condition\ttotal_accidents\taverage_severity`
* **Process**: Group by weather condition, calculate count and average

### Example Output

```
Weather_Condition         Total_Accidents  Avg_Severity
Clear                     245,832          2.13
Overcast                  45,672           2.28
Light Rain                32,156           2.31
Mostly Cloudy             28,943           2.19
```

---

## 🧪 Testing

**Local Test**

```bash
python test_mapreduce.py
```

**Full Hadoop Test**

```bash
python run_hadoop_job.py --test
```

---

## 🖥️ Monitoring Job Progress

**Command Line**

```bash
hadoop job -list
yarn application -list
```

**Web UI**

* HDFS NameNode: [http://localhost:9870](http://localhost:9870)
* YARN ResourceManager: [http://localhost:8088](http://localhost:8088)
* JobHistory Server: [http://localhost:19888](http://localhost:19888)

---

## 🧹 Cleanup

```bash
docker-compose down -v   # If using Docker
rm -rf output/           # Remove local output
```

---

## 📈 Performance Notes

* **Dataset Size**: 500,000 rows (~100MB CSV)
* **Processing Time**: 2-5 minutes (depends on hardware)
* **Memory Usage**: 2-4GB recommended

---

## 📚 Learning Objectives

This project demonstrates:

* Hadoop ecosystem usage (HDFS, YARN, MapReduce)
* Distributed computing with large datasets
* Python-based Hadoop Streaming
* Aggregation patterns in MapReduce
* Scalable data processing architectures

---

