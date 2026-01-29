# Description: Integration notes for Precursor ML project.
# Description: Reference when working on predictive-insights.

# Precursor Integration Notes

These notes should be applied when working on the Precursor (predictive-insights) project.

## Data Source Changes (HttpIngest v32+)

HttpIngest has migrated from PostgreSQL to Azure Data Lake Gen2 for primary storage.

### Old Approach (Deprecated)
```python
# Direct PostgreSQL queries
conn = psycopg2.connect(...)
cursor.execute("SELECT * FROM metric_data WHERE ...")
```

### New Approach (v32+)
```python
# HTTP API calls to ML endpoints
import requests

# Get training data
response = requests.get(
    "https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io/api/ml/training-data",
    params={
        "start_time": "2026-01-01T00:00:00Z",
        "end_time": "2026-01-25T00:00:00Z",
        "profile": "collector",
        "limit": 100000
    }
)
data = response.json()["data"]

# Get inventory
response = requests.get(
    "https://ca-cta-lm-ingest.../api/ml/inventory"
)
inventory = response.json()

# Check profile coverage
response = requests.get(
    "https://ca-cta-lm-ingest.../api/ml/profile-coverage",
    params={"profile": "collector"}
)
coverage = response.json()
```

## Required Changes in Precursor

### 1. Update DataFetcher

Replace direct database queries with HTTP API calls:

```python
class DataFetcher:
    def __init__(self, httpingest_url: str):
        self.base_url = httpingest_url

    def get_training_data(
        self,
        start_time: datetime,
        end_time: datetime,
        profile: str = None,
        limit: int = 100000
    ) -> pd.DataFrame:
        response = requests.get(
            f"{self.base_url}/api/ml/training-data",
            params={
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "profile": profile,
                "limit": limit
            }
        )
        response.raise_for_status()
        return pd.DataFrame(response.json()["data"])
```

### 2. Update Configuration

```yaml
# config/data_sources.yaml
httpingest:
  url: "https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io"
  endpoints:
    training_data: "/api/ml/training-data"
    inventory: "/api/ml/inventory"
    profiles: "/api/ml/profiles"
    coverage: "/api/ml/profile-coverage"
```

### 3. Remove PostgreSQL Dependencies

- Remove direct database connection code
- Remove psycopg2/asyncpg dependencies if not needed elsewhere
- Update environment variable requirements

## Data Schema Changes

The training data format has changed slightly:

| Old Field | New Field | Notes |
|-----------|-----------|-------|
| resource_id | resource_hash | SHA256 hash instead of DB ID |
| host_name | (in attributes) | Parse from attributes JSON |
| service_name | (in attributes) | Parse from attributes JSON |
| datasource_instance | (in attributes) | Parse from attributes JSON |

## ML Endpoints Available

| Endpoint | Purpose | Use Case |
|----------|---------|----------|
| `/api/ml/inventory` | List available data | Pre-training validation |
| `/api/ml/training-data` | Get historical data | Model training |
| `/api/ml/profiles` | List feature profiles | Profile selection |
| `/api/ml/profile-coverage` | Check metric coverage | Data quality check |

## Cost Considerations

- Synapse Serverless SQL: ~$5 per TB scanned
- Use time-bounded queries to minimize costs
- Profile filtering reduces data volume
- Consider caching responses for development

## Testing

1. Check health: `curl .../api/health` (expect synapse: healthy, datalake: healthy)
2. Test training data: `curl ".../api/ml/training-data?limit=5&start_time=2026-01-28T00:00:00Z&end_time=2026-01-30T00:00:00Z"`
3. Check profiles: `curl .../api/ml/profiles`
4. Note: /api/ml/inventory may timeout on large datasets (full Synapse scan, no partition pruning)
