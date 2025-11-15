# Docker Development Environment

This guide explains how to run the LogicMonitor Data Pipeline locally using Docker Compose.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- 4GB+ RAM available for containers

## Quick Start

1. **Copy environment file:**
   ```bash
   cp .env.example .env
   ```

2. **Start all services:**
   ```bash
   docker-compose up -d
   ```

3. **Check service health:**
   ```bash
   docker-compose ps
   ```

4. **View logs:**
   ```bash
   docker-compose logs -f app
   ```

## Services

The Docker Compose stack includes:

### Core Services

| Service | Port | Description |
|---------|------|-------------|
| **postgres** | 5432 | PostgreSQL 17.5 database |
| **redis** | 6379 | Redis for pub/sub messaging |
| **app** | 7071, 8000 | LogicMonitor Data Pipeline |

### Optional Services

| Service | Port | Description |
|---------|------|-------------|
| **grafana** | 3000 | Grafana dashboard (admin/admin) |
| **prometheus** | 9090 | Prometheus metrics collector |

## Service Endpoints

Once running, access:

### Application Endpoints

- **Health Check:** http://localhost:8000/api/health
- **API Health:** http://localhost:8000/api/health
- **Metrics Summary:** http://localhost:8000/api/metrics/summary

### Data Export Endpoints

- **Prometheus Metrics:** http://localhost:8000/metrics/prometheus
- **Grafana Datasource:** http://localhost:8000/grafana
- **PowerBI OData:** http://localhost:8000/api/odata/metrics
- **CSV Export:** http://localhost:8000/export/csv?metrics=cpu.usage&hours=24
- **JSON Export:** http://localhost:8000/export/json?metrics=memory.bytes&hours=24

### Real-time Streaming

- **WebSocket:** ws://localhost:8000/ws
- **Server-Sent Events:** http://localhost:8000/sse?client_id=test

### OTLP Ingestion

- **HTTP Ingest:** http://localhost:7071/api/HttpIngest
- **Health:** http://localhost:7071/api/health

### Monitoring Tools

- **Grafana:** http://localhost:3000 (admin/admin)
- **Prometheus:** http://localhost:9090

## Database Management

### Run Migrations

```bash
docker-compose exec app uv run alembic upgrade head
```

### Create New Migration

```bash
docker-compose exec app uv run alembic revision --autogenerate -m "description"
```

### Connect to Database

```bash
docker-compose exec postgres psql -U postgres -d postgres
```

### Check Database Status

```bash
docker-compose exec postgres pg_isready -U postgres
```

## Development Workflow

### Live Code Reload

The `app` service mounts your source code as a volume, enabling live reload:

1. Edit code locally
2. Uvicorn automatically reloads
3. Changes reflected immediately

### Run Tests

```bash
# Run all tests
docker-compose exec app uv run pytest tests/ -v

# Run specific test file
docker-compose exec app uv run pytest tests/test_exporters.py -v

# Run with coverage
docker-compose exec app uv run pytest tests/ --cov=src --cov-report=html
```

### View Application Logs

```bash
# Follow all logs
docker-compose logs -f

# Follow specific service
docker-compose logs -f app

# Last 100 lines
docker-compose logs --tail=100 app
```

### Access Python Shell

```bash
docker-compose exec app uv run python
```

## Troubleshooting

### Services Won't Start

1. **Check Docker resources:**
   ```bash
   docker system df
   ```

2. **View detailed logs:**
   ```bash
   docker-compose logs
   ```

3. **Restart services:**
   ```bash
   docker-compose restart
   ```

### Database Connection Issues

1. **Verify PostgreSQL is healthy:**
   ```bash
   docker-compose exec postgres pg_isready
   ```

2. **Check connection string in .env:**
   ```
   POSTGRES_CONN_STR=postgresql://postgres:postgres@postgres:5432/postgres
   ```

3. **Restart database:**
   ```bash
   docker-compose restart postgres
   ```

### Redis Connection Issues

1. **Check Redis health:**
   ```bash
   docker-compose exec redis redis-cli ping
   ```

2. **View Redis logs:**
   ```bash
   docker-compose logs redis
   ```

### Application Won't Start

1. **Check dependency installation:**
   ```bash
   docker-compose exec app uv sync
   ```

2. **Rebuild image:**
   ```bash
   docker-compose build --no-cache app
   docker-compose up -d app
   ```

## Advanced Usage

### Custom Environment Variables

Edit `.env` to customize:

```bash
# Increase WebSocket connections
MAX_WEBSOCKET_CONNECTIONS=500

# Adjust rate limiting
RATE_LIMIT_MESSAGES_PER_SECOND=50

# Change background task intervals
DATA_PROCESSING_INTERVAL=10
```

### Scale Services

```bash
# Scale application instances (behind load balancer)
docker-compose up -d --scale app=3
```

### Disable Optional Services

Edit `docker-compose.yml` to comment out services you don't need:

```yaml
# services:
#   grafana:
#     ...
#   prometheus:
#     ...
```

### Production-like Build

```bash
# Build optimized image
docker build -t lm-pipeline:prod .

# Run without docker-compose
docker run -d \
  -e POSTGRES_CONN_STR="..." \
  -e REDIS_URL="redis://redis:6379" \
  -p 8000:8000 \
  lm-pipeline:prod
```

## Data Persistence

Volumes are created automatically for:

- `postgres_data` - Database files
- `grafana_data` - Grafana dashboards and config
- `prometheus_data` - Prometheus metrics storage
- `python_cache` - Python package cache

### Backup Data

```bash
# Backup PostgreSQL
docker-compose exec postgres pg_dump -U postgres postgres > backup.sql

# Backup volumes
docker run --rm \
  -v httpingest_postgres_data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/postgres_backup.tar.gz -C /data .
```

### Restore Data

```bash
# Restore PostgreSQL
cat backup.sql | docker-compose exec -T postgres psql -U postgres postgres
```

### Clean Up Volumes

```bash
# Remove all volumes (DANGER: data loss!)
docker-compose down -v
```

## Grafana Setup

1. **Access Grafana:** http://localhost:3000
2. **Login:** admin/admin
3. **Add SimpleJSON Datasource:**
   - Configuration → Data Sources → Add data source
   - Select "SimpleJSON"
   - URL: `http://app:8000/grafana`
   - Save & Test

4. **Create Dashboard:**
   - Create → Dashboard
   - Add panel
   - Select your datasource
   - Query metrics

## Prometheus Setup

1. **Access Prometheus:** http://localhost:9090
2. **Verify targets:** Status → Targets
3. **Query metrics:**
   ```promql
   rate(http_requests_total[5m])
   ```

## Stopping Services

```bash
# Stop all services
docker-compose stop

# Stop and remove containers
docker-compose down

# Stop, remove containers, and volumes (DANGER: data loss!)
docker-compose down -v
```

## Performance Tuning

### PostgreSQL

Edit `docker-compose.yml` to add:

```yaml
postgres:
  command:
    - "postgres"
    - "-c"
    - "max_connections=200"
    - "-c"
    - "shared_buffers=256MB"
```

### Redis

```yaml
redis:
  command: redis-server --maxmemory 512mb --maxmemory-policy allkeys-lru
```

### Application

Adjust environment variables:

```bash
# Increase worker processes
WORKERS=4

# Adjust connection pools
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=10
```

## Monitoring

### Health Checks

All services include health checks:

```bash
# Check all service health
docker-compose ps

# Watch health status
watch -n 2 docker-compose ps
```

### Resource Usage

```bash
# Real-time stats
docker stats

# Container resource limits
docker-compose config
```

## Integration Testing

```bash
# Run end-to-end tests
docker-compose exec app uv run pytest tests/test_infrastructure.py -v

# Test OTLP ingestion
curl -X POST http://localhost:7071/api/HttpIngest \
  -H "Content-Type: application/json" \
  -d '{"resourceMetrics": []}'

# Test WebSocket connection
wscat -c ws://localhost:8000/ws

# Test SSE stream
curl -N http://localhost:8000/sse?client_id=test
```

## Next Steps

- Review [Integration Guide](integrations.md) for connecting external tools
- See [Migration Guide](../MIGRATION_QUICK_START.md) for database schema
- Check [Plan](../plan.md) for architecture details
