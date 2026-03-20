# CobaltDB Docker Setup

This directory contains Docker configuration for running CobaltDB in containerized environments.

## Prerequisites

- Docker Desktop 20.10+ (Windows 11 recommended)
- Docker Compose v2.0+
- At least 4GB RAM allocated to Docker

## Quick Start

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f cobaltdb

# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: deletes data)
docker-compose down -v
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| cobaltdb | 4200 | Wire protocol server |
| cobaltdb | 3307 | MySQL protocol server (connect with any MySQL client) |
| cobaltdb | 8420 | Health check / metrics API |
| prometheus | 9090 | Metrics collection |
| grafana | 3000 | Metrics visualization |
| backup | - | Automated backups |

## Connecting with MySQL Clients

Once the container is running, connect with any MySQL client:

```bash
# MySQL CLI
mysql -h 127.0.0.1 -P 3307 -u admin

# Python
import mysql.connector
conn = mysql.connector.connect(host='127.0.0.1', port=3307, user='admin')

# Node.js
const mysql = require('mysql2');
const conn = mysql.createConnection({ host: '127.0.0.1', port: 3307, user: 'admin' });
```

## Windows-Specific Notes

### File Sharing
Ensure Docker Desktop has file sharing enabled for:
- Your project directory
- The drive containing your project

### TLS/SSL Certificates
To enable TLS, create a `certs` directory and add your certificate files:
```bash
mkdir certs
cp server.crt certs/
cp server.key certs/
```

Then update `docker-compose.yml` to mount the certs:
```yaml
volumes:
  - ./certs:/etc/cobaltdb/certs:ro
```

### Line Endings
If you get "exec format" errors, the scripts may have Windows line endings. Convert them:
```bash
docker run --rm -v "${PWD}:/code" alpine sh -c "cd /code && sed -i 's/\\r$//' Dockerfile Dockerfile.backup"
```

### WSL2 Backend (Recommended)
For best performance on Windows 11:
1. Open Docker Desktop Settings
2. General → Use the WSL 2 based engine
3. Resources → WSL Integration → Enable integration with your distro

## Volumes

| Volume | Location | Purpose |
|--------|----------|---------|
| cobaltdb_data | /data/cobaltdb | Database files |
| prometheus_data | /prometheus | Metrics data |
| grafana_data | /var/lib/grafana | Dashboards & settings |
| ./backups | /backups | Backup files |

## Backup Service

Backups run automatically based on `BACKUP_SCHEDULE` (cron format). Default: daily at 2 AM.

```bash
# View backup logs
docker-compose logs -f backup

# List backups
docker exec cobaltdb_backup ls -la /backups

# Manual backup
docker exec cobaltdb_backup /scripts/backup.sh
```

## Monitoring

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
- **CobaltDB Metrics**: http://localhost:8420/metrics

## Troubleshooting

### "failed to solve: failed to read dockerfile"
Ensure `Dockerfile.backup` exists in the same directory as `docker-compose.yml`.

### Port already in use
Change ports in `docker-compose.yml`:
```yaml
ports:
  - "4201:4200"  # Use 4201 instead of 4200
```

### Permission denied on volumes
On Windows, Docker handles permissions automatically. On Linux/Mac:
```bash
sudo chown -R 1000:1000 ./backups
```

### Build fails on Windows
Try building with no cache:
```bash
docker-compose build --no-cache
docker-compose up -d
```

## Production Deployment

For production environments:

1. **Enable TLS**:
   ```yaml
   environment:
     - COBALTDB_TLS_ENABLED=true
   volumes:
     - ./certs:/etc/cobaltdb/certs
   ```

2. **Change default passwords**:
   ```yaml
   environment:
     - GF_SECURITY_ADMIN_PASSWORD=strongpassword
   ```

3. **Use external volumes**:
   ```yaml
   volumes:
     cobaltdb_data:
       driver: local
       driver_opts:
         type: none
         o: bind
         device: /mnt/cobaltdb_data
   ```

4. **Resource limits**:
   ```yaml
   deploy:
     resources:
       limits:
         cpus: '2'
         memory: 4G
   ```

## Environment Variables

### CobaltDB
| Variable | Default | Description |
|----------|---------|-------------|
| COBALTDB_DATA_DIR | /data/cobaltdb | Database storage path |
| COBALTDB_CONFIG | /etc/cobaltdb/cobaltdb.conf | Config file path |
| COBALTDB_LOG_LEVEL | info | Logging level |

### Backup
| Variable | Default | Description |
|----------|---------|-------------|
| BACKUP_SCHEDULE | 0 2 * * * | Cron schedule format (for reference) |
| BACKUP_INTERVAL_HOURS | 24 | Backup interval in hours |
| BACKUP_RETENTION_DAYS | 7 | Days to keep backups |

## Building Individual Images

```bash
# Main server
docker build -t cobaltdb:latest .

# Backup service
docker build -t cobaltdb-backup:latest -f Dockerfile.backup .

# Run server
docker run -d -p 4200:4200 -v cobaltdb_data:/data/cobaltdb cobaltdb:latest
```
