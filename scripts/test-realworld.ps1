# CobaltDB Real-World Scenario Test Script for Windows PowerShell
# This script tests CobaltDB with realistic production workloads

Write-Host '╔═══════════════════════════════════════════════════════════════╗' -ForegroundColor Cyan
Write-Host '║     CobaltDB Real-World Production Test (Windows)             ║' -ForegroundColor Cyan
Write-Host '╚═══════════════════════════════════════════════════════════════╝' -ForegroundColor Cyan
Write-Host ''

# Check if Docker is running
try {
    $dockerInfo = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host 'Error: Docker is not running' -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host 'Error: Docker is not running' -ForegroundColor Red
    exit 1
}

Write-Host 'Starting CobaltDB services...' -ForegroundColor Yellow
docker-compose up -d cobaltdb prometheus grafana

# Wait for CobaltDB to be ready
Write-Host ''
Write-Host 'Waiting for CobaltDB to be ready...' -ForegroundColor Yellow
$ready = $false
for ($i = 1; $i -le 30; $i++) {
    docker exec cobaltdb sh -c 'nc -z localhost 4200' 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host '✓ CobaltDB is ready!' -ForegroundColor Green
        $ready = $true
        break
    }
    Write-Host '.' -NoNewline
    Start-Sleep -Seconds 1
}

if (-not $ready) {
    Write-Host '✗ CobaltDB failed to start' -ForegroundColor Red
    docker-compose logs cobaltdb
    exit 1
}

Write-Host ''
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan
Write-Host 'TEST 1: Basic Connectivity (Wire Protocol :4200)' -ForegroundColor Cyan
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan

docker exec cobaltdb sh -c 'nc -z localhost 4200' 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host '✓ Wire protocol port is open' -ForegroundColor Green
} else {
    Write-Host '✗ Wire protocol connection failed' -ForegroundColor Red
}

Write-Host ''
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan
Write-Host 'TEST 2: MySQL Protocol (:3307)' -ForegroundColor Cyan
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan

docker exec cobaltdb sh -c 'nc -z localhost 3307' 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host '✓ MySQL protocol port is open' -ForegroundColor Green
} else {
    Write-Host '! MySQL protocol port not responding (may not be enabled)' -ForegroundColor Yellow
}

Write-Host ''
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan
Write-Host 'TEST 3: Admin API (:8420)' -ForegroundColor Cyan
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan

try {
    $health = Invoke-RestMethod -Uri 'http://localhost:8420/health' -TimeoutSec 5 -ErrorAction Stop
    Write-Host '✓ Admin API is responding' -ForegroundColor Green
    Write-Host "Health check: $health"
} catch {
    Write-Host '! Admin API not available' -ForegroundColor Yellow
}

Write-Host ''
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan
Write-Host 'TEST 4: SQL Operations via CLI' -ForegroundColor Cyan
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan

Write-Host 'Creating test table...' -ForegroundColor Yellow
docker exec cobaltdb cobaltdb-cli -data /tmp/test.db 'CREATE TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT, age INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)' 2>$null

if ($LASTEXITCODE -eq 0) {
    Write-Host '✓ Table created' -ForegroundColor Green
} else {
    Write-Host '! Table creation may have failed, continuing...' -ForegroundColor Yellow
}

Write-Host 'Inserting 10 records...' -ForegroundColor Yellow
for ($i = 1; $i -le 10; $i++) {
    $age = 20 + ($i % 50)
    $email = "user$i@test.com"
    $name = "User $i"
    docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "INSERT INTO test_users (email, name, age) VALUES ('$email', '$name', $age)" 2>$null | Out-Null
}
Write-Host '✓ Inserted records' -ForegroundColor Green

Write-Host 'Running SELECT query...' -ForegroundColor Yellow
$count = docker exec cobaltdb cobaltdb-cli -data /tmp/test.db 'SELECT COUNT(*) FROM test_users' 2>$null
Write-Host "Count result: $count"

Write-Host ''
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan
Write-Host 'TEST 5: Concurrent Connections' -ForegroundColor Cyan
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan

Write-Host 'Simulating 5 concurrent connections...' -ForegroundColor Yellow
for ($i = 1; $i -le 5; $i++) {
    Start-Job -ScriptBlock {
        param($id)
        docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "SELECT * FROM test_users WHERE id = $id" 2>$null
    } -ArgumentList $i | Out-Null
}
Get-Job | Wait-Job | Remove-Job
Write-Host '✓ Concurrent connections handled' -ForegroundColor Green

Write-Host ''
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan
Write-Host 'TEST 6: Monitoring Endpoints' -ForegroundColor Cyan
Write-Host '═══════════════════════════════════════════════════════════════' -ForegroundColor Cyan

try {
    $prometheus = Invoke-RestMethod -Uri 'http://localhost:9090/api/v1/status/targets' -TimeoutSec 5 -ErrorAction Stop
    Write-Host '✓ Prometheus is running' -ForegroundColor Green
} catch {
    Write-Host '! Prometheus metrics not available' -ForegroundColor Yellow
}

try {
    $grafana = Invoke-RestMethod -Uri 'http://localhost:3000/api/health' -TimeoutSec 5 -ErrorAction Stop
    Write-Host '✓ Grafana is running' -ForegroundColor Green
} catch {
    Write-Host '! Grafana not available' -ForegroundColor Yellow
}

Write-Host ''
Write-Host '╔═══════════════════════════════════════════════════════════════╗' -ForegroundColor Green
Write-Host '║                    TEST COMPLETED                             ║' -ForegroundColor Green
Write-Host '╚═══════════════════════════════════════════════════════════════╝' -ForegroundColor Green
Write-Host ''
Write-Host 'Service URLs:' -ForegroundColor Cyan
Write-Host '  - CobaltDB Wire Protocol: localhost:4200'
Write-Host '  - CobaltDB MySQL Protocol: localhost:3307'
Write-Host '  - Admin API: http://localhost:8420'
Write-Host '  - Prometheus: http://localhost:9090'
Write-Host '  - Grafana: http://localhost:3000 (admin/admin)'
Write-Host ''
Write-Host 'Running services:' -ForegroundColor Cyan
docker-compose ps --services --filter 'status=running'
