#!/usr/bin/env pwsh
# ============================================================
# Hadoop MapReduce Pipeline - Weather Condition Analysis
# Runs the full pipeline from cleanup to results
# ============================================================

$ErrorActionPreference = "Continue"
$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Set-Location $ProjectDir

function Write-Step($step, $msg) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "  STEP $step : $msg" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}

function Write-OK($msg) {
    Write-Host "  [OK] $msg" -ForegroundColor Green
}

function Write-Fail($msg) {
    Write-Host "  [FAIL] $msg" -ForegroundColor Red
}

# ----------------------------------------------------------
# STEP 0: Cleanup
# ----------------------------------------------------------
Write-Step 0 "Cleaning up previous run"

# Remove local output files
if (Test-Path "$ProjectDir\output") {
    Remove-Item "$ProjectDir\output\*" -Force -ErrorAction SilentlyContinue
    Write-OK "Cleaned local output/ directory"
} else {
    New-Item -ItemType Directory -Path "$ProjectDir\output" | Out-Null
    Write-OK "Created output/ directory"
}

# Stop and remove existing containers
Write-Host "  Stopping existing containers..." -ForegroundColor Yellow
$null = docker-compose down 2>&1
Write-OK "Containers stopped and removed"

# ----------------------------------------------------------
# STEP 1: Start Hadoop Cluster
# ----------------------------------------------------------
Write-Step 1 "Starting Hadoop Cluster (4 containers)"

$composeOutput = docker-compose up -d 2>&1
# docker-compose may return warnings on stderr even on success
# Verify by checking if containers were actually created
Start-Sleep -Seconds 10
$created = (docker ps -a --format "{{.Names}}" 2>&1) -join " "
if ($created -notmatch "hadoop-namenode") {
    Write-Fail "Failed to start containers"
    Write-Host $composeOutput
    exit 1
}
Write-OK "Containers created"

# Wait for all 4 containers to be running
Write-Host "  Waiting for containers to initialize..." -ForegroundColor Yellow
$maxWait = 120  # seconds
$elapsed = 0
$allRunning = $false

while ($elapsed -lt $maxWait) {
    Start-Sleep -Seconds 5
    $elapsed += 5

    $statuses = docker inspect --format "{{.State.Status}}" hadoop-namenode hadoop-datanode hadoop-resourcemanager hadoop-nodemanager 2>&1
    $running = ($statuses | Where-Object { $_ -eq "running" }).Count

    Write-Host "  [$elapsed`s] $running/4 containers running..." -ForegroundColor Yellow

    if ($running -eq 4) {
        $allRunning = $true
        break
    }
}

if (-not $allRunning) {
    Write-Fail "Not all containers started within $maxWait seconds"
    Write-Host "  Container statuses:"
    docker ps -a --format "{{.Names}} {{.Status}}"
    exit 1
}
Write-OK "All 4 containers running"

# Wait extra time for HDFS to leave safe mode
Write-Host "  Waiting for HDFS to be ready..." -ForegroundColor Yellow
$hdfsReady = $false
$elapsed = 0

while ($elapsed -lt 90) {
    Start-Sleep -Seconds 5
    $elapsed += 5

    $check = docker exec hadoop-namenode bash -c "hdfs dfs -mkdir -p /tmp/healthcheck 2>&1 && hdfs dfs -rm -r /tmp/healthcheck 2>&1 && echo HDFS_READY" 2>&1
    if ($check -match "HDFS_READY") {
        $hdfsReady = $true
        break
    }
    Write-Host "  [$elapsed`s] HDFS not ready yet..." -ForegroundColor Yellow
}

if (-not $hdfsReady) {
    Write-Fail "HDFS did not become ready within 90 seconds"
    exit 1
}
Write-OK "HDFS is ready"

# ----------------------------------------------------------
# STEP 2: Verify Python in NameNode
# ----------------------------------------------------------
Write-Step 2 "Verifying Python availability"

$pyVer = docker exec hadoop-namenode python3 --version 2>&1
if ($pyVer -match "Python") {
    Write-OK "Python installed: $pyVer"
} else {
    Write-Fail "Python3 not found in namenode container"
    exit 1
}

# ----------------------------------------------------------
# STEP 3: Clean HDFS & Upload Data
# ----------------------------------------------------------
Write-Step 3 "Preparing HDFS - Clean old data & Upload fresh data"

# Clean any existing HDFS data
docker exec hadoop-namenode bash -c "hdfs dfs -rm -r -f /user/input /user/output 2>/dev/null; echo CLEANED" 2>&1 | Out-Null

# Create input directory
docker exec hadoop-namenode bash -c "hdfs dfs -mkdir -p /user/input 2>&1"
Write-OK "HDFS directories created"

# Verify data.csv exists in mounted volume
$dataCheck = docker exec hadoop-namenode bash -c "ls -lh /data/data.csv 2>&1"
if ($dataCheck -match "data.csv") {
    Write-OK "data.csv found in container"
} else {
    Write-Fail "data.csv not found at /data/data.csv inside container"
    exit 1
}

# Upload data to HDFS
Write-Host "  Uploading data.csv to HDFS (this may take a moment)..." -ForegroundColor Yellow
$null = docker exec hadoop-namenode bash -c "hdfs dfs -put /data/data.csv /user/input/ 2>&1"

# Verify upload
$hdfsLs = docker exec hadoop-namenode bash -c "hdfs dfs -ls /user/input/ 2>&1"
if ($hdfsLs -match "data.csv") {
    Write-OK "data.csv uploaded to HDFS successfully"
} else {
    Write-Fail "data.csv not found in HDFS after upload"
    exit 1
}

# ----------------------------------------------------------
# STEP 4: Run MapReduce Job
# ----------------------------------------------------------
Write-Step 4 "Running MapReduce Streaming Job"

Write-Host "  Mapper:  /mapreduce/mapper.py" -ForegroundColor Yellow
Write-Host "  Reducer: /mapreduce/reducer.py" -ForegroundColor Yellow
Write-Host "  Input:   /user/input/data.csv" -ForegroundColor Yellow
Write-Host "  Output:  /user/output/weather_analysis" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Processing 500,000 rows... please wait..." -ForegroundColor Yellow
Write-Host ""

$jobStart = Get-Date

$jobOutput = docker exec hadoop-namenode bash -c "hadoop jar /opt/hadoop-2.7.4/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar -files /mapreduce/mapper.py,/mapreduce/reducer.py -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' -input /user/input/data.csv -output /user/output/weather_analysis 2>&1"

$jobEnd = Get-Date
$jobDuration = ($jobEnd - $jobStart).TotalSeconds

# Check if output was created
$outputCheck = docker exec hadoop-namenode bash -c "hdfs dfs -ls /user/output/weather_analysis/part-00000 2>&1"
if ($outputCheck -match "part-00000") {
    Write-OK "MapReduce job completed in $([math]::Round($jobDuration, 1)) seconds"
} else {
    Write-Fail "MapReduce job failed"
    Write-Host "  Job log:" -ForegroundColor Red
    Write-Host $jobOutput
    exit 1
}

# ----------------------------------------------------------
# STEP 5: Save Results
# ----------------------------------------------------------
Write-Step 5 "Saving Results"

# Save raw results from HDFS to container's output dir (mounted volume)
docker exec hadoop-namenode bash -c "hdfs dfs -get /user/output/weather_analysis/part-00000 /output/weather_analysis_results.txt 2>&1"
Write-OK "Raw results saved to output/weather_analysis_results.txt"

# Also create a CSV version
$results = docker exec hadoop-namenode bash -c "hdfs dfs -cat /user/output/weather_analysis/part-00000 2>&1"

# Write CSV header + data
$csvContent = "Weather_Condition,Total_Accidents,Average_Severity`n"
foreach ($line in $results -split "`n") {
    $line = $line.Trim()
    if ($line -and $line -match "^(.+)\t(\d+)\t([\d.]+)$") {
        $csvContent += "$($Matches[1]),$($Matches[2]),$($Matches[3])`n"
    }
}
$csvContent | Out-File -FilePath "$ProjectDir\output\weather_analysis_results.csv" -Encoding UTF8 -NoNewline
Write-OK "CSV results saved to output/weather_analysis_results.csv"

# ----------------------------------------------------------
# STEP 6: Display Results
# ----------------------------------------------------------
Write-Step 6 "Weather Condition Analysis Results"

Write-Host ""
Write-Host ("{0,-45} {1,-18} {2,-15}" -f "Weather Condition", "Total Accidents", "Avg Severity") -ForegroundColor White
Write-Host ("-" * 78) -ForegroundColor Gray

# Parse and sort by accident count descending
$parsed = @()
foreach ($line in $results -split "`n") {
    $line = $line.Trim()
    if ($line -and $line -match "^(.+)\t(\d+)\t([\d.]+)$") {
        $parsed += [PSCustomObject]@{
            Weather  = $Matches[1]
            Count    = [int]$Matches[2]
            Severity = [double]$Matches[3]
        }
    }
}

$sorted = $parsed | Sort-Object -Property Count -Descending
$totalAccidents = ($sorted | Measure-Object -Property Count -Sum).Sum

foreach ($row in $sorted) {
    $color = if ($row.Count -gt 10000) { "Yellow" } elseif ($row.Count -gt 1000) { "White" } else { "Gray" }
    Write-Host ("{0,-45} {1,-18} {2,-15}" -f $row.Weather, $row.Count.ToString("N0"), $row.Severity.ToString("F2")) -ForegroundColor $color
}

Write-Host ("-" * 78) -ForegroundColor Gray
Write-Host ""
Write-Host "  Total unique weather conditions: $($sorted.Count)" -ForegroundColor Green
Write-Host "  Total accidents analyzed:        $($totalAccidents.ToString('N0'))" -ForegroundColor Green

# ----------------------------------------------------------
# DONE
# ----------------------------------------------------------
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  PIPELINE COMPLETE" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Results saved to:" -ForegroundColor White
Write-Host "    - output/weather_analysis_results.txt  (raw tab-separated)" -ForegroundColor White
Write-Host "    - output/weather_analysis_results.csv  (CSV format)" -ForegroundColor White
Write-Host ""
Write-Host "  Web UIs:" -ForegroundColor White
Write-Host "    - HDFS NameNode:       http://localhost:9870" -ForegroundColor White
Write-Host "    - YARN ResourceManager: http://localhost:8088" -ForegroundColor White
Write-Host ""
