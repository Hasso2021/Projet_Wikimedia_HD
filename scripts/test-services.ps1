# Tests rapides des services Docker (Windows PowerShell)
# Usage : .\scripts\test-services.ps1

$ErrorActionPreference = "Stop"

Write-Host "`n=== Big Data Stack — Smoke Tests ===`n" -ForegroundColor Cyan

function Test-Http {
    param([string]$Name, [string]$Url)
    try {
        $r = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 15
        Write-Host "[OK]   $Name — $Url (HTTP $($r.StatusCode))" -ForegroundColor Green
    } catch {
        Write-Host "[FAIL] $Name — $Url" -ForegroundColor Red
        Write-Host "       $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

Write-Host "--- Web UIs ---"
Test-Http "Spark Master"   "http://localhost:8080"
Test-Http "Spark Worker"   "http://localhost:8081"
Test-Http "HDFS Namenode"  "http://localhost:9870"
Test-Http "HDFS Datanode"  "http://localhost:9864"
Test-Http "Airflow API"    "http://localhost:8088/api/v2/monitor/health"

Write-Host "`n--- Container health ---"
docker compose ps

Write-Host "`n--- Kafka ---"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] Kafka broker not responding" -ForegroundColor Red
} else {
    Write-Host "[OK]   Kafka broker responded" -ForegroundColor Green
}

Write-Host "`n--- HDFS ---"
docker exec hdfs-namenode hdfs dfs -ls / 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] HDFS namenode CLI failed" -ForegroundColor Red
} else {
    Write-Host "[OK]   HDFS namenode CLI works" -ForegroundColor Green
}

Write-Host "`n--- Airflow DAGs ---"
docker exec airflow-api-server airflow dags list 2>$null | Select-String "hello_world"
if ($LASTEXITCODE -ne 0) {
    Write-Host "[WARN] Could not list DAGs (services may still be starting)" -ForegroundColor Yellow
} else {
    Write-Host "[OK]   hello_world DAG is visible" -ForegroundColor Green
}

Write-Host "`n=== Done. Open http://localhost:8088 for Airflow UI ===`n" -ForegroundColor Cyan
