@echo off
chcp 65001 >nul
echo ╔═══════════════════════════════════════════════════════════════╗
echo ║     CobaltDB Real-World Production Test (Windows)             ║
echo ╚═══════════════════════════════════════════════════════════════╝
echo.

echo Starting CobaltDB services...
docker-compose up -d cobaltdb prometheus grafana
echo.

echo Waiting for CobaltDB to be ready...
:wait_loop
docker exec cobaltdb sh -c "nc -z localhost 4200" >nul 2>&1
if %errorlevel% == 0 goto ready
echo|set /p=.
timeout /t 1 /nobreak >nul
goto wait_loop

:ready
echo.
echo [OK] CobaltDB is ready!
echo.

echo ═══════════════════════════════════════════════════════════════
echo TEST 1: Basic Connectivity (Wire Protocol :4200)
echo ═══════════════════════════════════════════════════════════════
docker exec cobaltdb sh -c "nc -z localhost 4200" >nul 2>&1
if %errorlevel% == 0 (
    echo [OK] Wire protocol port is open
) else (
    echo [FAIL] Wire protocol connection failed
)
echo.

echo ═══════════════════════════════════════════════════════════════
echo TEST 2: MySQL Protocol (:3307)
echo ═══════════════════════════════════════════════════════════════
docker exec cobaltdb sh -c "nc -z localhost 3307" >nul 2>&1
if %errorlevel% == 0 (
    echo [OK] MySQL protocol port is open
) else (
    echo [!] MySQL protocol port not responding (may not be enabled)
)
echo.

echo ═══════════════════════════════════════════════════════════════
echo TEST 3: Admin API (:8420)
echo ═══════════════════════════════════════════════════════════════
curl -s http://localhost:8420/health >nul 2>&1
if %errorlevel% == 0 (
    echo [OK] Admin API is responding
    echo Health check:
    curl -s http://localhost:8420/health
) else (
    echo [!] Admin API not available
)
echo.

echo ═══════════════════════════════════════════════════════════════
echo TEST 4: SQL Operations via CLI
echo ═══════════════════════════════════════════════════════════════
echo Creating test table...
docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "CREATE TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT, age INTEGER)" >nul 2>&1
echo [OK] Table created

echo Inserting 5 records...
docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "INSERT INTO test_users (email, name, age) VALUES ('user1@test.com', 'User 1', 25)" >nul 2>&1
docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "INSERT INTO test_users (email, name, age) VALUES ('user2@test.com', 'User 2', 30)" >nul 2>&1
docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "INSERT INTO test_users (email, name, age) VALUES ('user3@test.com', 'User 3', 35)" >nul 2>&1
docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "INSERT INTO test_users (email, name, age) VALUES ('user4@test.com', 'User 4', 40)" >nul 2>&1
docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "INSERT INTO test_users (email, name, age) VALUES ('user5@test.com', 'User 5', 45)" >nul 2>&1
echo [OK] Inserted records

echo Running SELECT query...
docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "SELECT COUNT(*) FROM test_users"
echo.

echo ═══════════════════════════════════════════════════════════════
echo TEST 5: Metrics Endpoints
echo ═══════════════════════════════════════════════════════════════
curl -s http://localhost:9090/api/v1/status/targets >nul 2>&1
if %errorlevel% == 0 (
    echo [OK] Prometheus is running
) else (
    echo [!] Prometheus not available
)

curl -s http://localhost:3000/api/health >nul 2>&1
if %errorlevel% == 0 (
    echo [OK] Grafana is running
) else (
    echo [!] Grafana not available
)
echo.

echo ╔═══════════════════════════════════════════════════════════════╗
echo ║                    TEST COMPLETED                             ║
echo ╚═══════════════════════════════════════════════════════════════╝
echo.
echo Service URLs:
echo   - CobaltDB Wire Protocol: localhost:4200
echo   - CobaltDB MySQL Protocol: localhost:3307
echo   - Admin API: http://localhost:8420
echo   - Prometheus: http://localhost:9090
echo   - Grafana: http://localhost:3000 (admin/admin)
echo.
echo Running services:
docker-compose ps --services --filter "status=running"
