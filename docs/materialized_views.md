# Materialized Views Documentation

## Overview

Materialized views provide pre-computed query results for common data access patterns, significantly improving query performance for frequently accessed data.

## Available Views

### 1. latest_metrics
**Purpose:** Get the most recent value for each metric per resource

**Columns:**
- `metric_data_id` - ID of the metric data point
- `resource_id` - Resource ID
- `resource_hash` - Resource hash for deduplication
- `resource_attributes` - JSONB resource attributes
- `metric_definition_id` - Metric definition ID
- `metric_name` - Name of the metric
- `metric_unit` - Unit of measurement
- `metric_type` - Type (gauge, sum, etc.)
- `datasource_name` - Datasource name
- `datasource_version` - Datasource version
- `timestamp` - Timestamp of the data point
- `value_double` - Double value (if applicable)
- `value_int` - Integer value (if applicable)
- `metric_attributes` - JSONB metric-specific attributes

**Example Query:**
```sql
-- Get latest CPU usage for all resources
SELECT
    resource_hash,
    metric_name,
    timestamp,
    value_double
FROM latest_metrics
WHERE metric_name = 'cpu.usage'
ORDER BY timestamp DESC;
```

**Indexes:**
- `ix_latest_metrics_resource_id` - Fast lookups by resource
- `ix_latest_metrics_metric_definition_id` - Fast lookups by metric
- `ix_latest_metrics_datasource_name` - Fast lookups by datasource
- `ix_latest_metrics_timestamp` - Fast time-based queries

---

### 2. hourly_aggregates
**Purpose:** Min/max/avg/count statistics per hour for each metric

**Columns:**
- `resource_id` - Resource ID
- `resource_hash` - Resource hash
- `metric_definition_id` - Metric definition ID
- `metric_name` - Name of the metric
- `metric_unit` - Unit of measurement
- `datasource_name` - Datasource name
- `datasource_version` - Datasource version
- `hour` - Hour bucket (truncated timestamp)
- `data_point_count` - Number of data points in this hour
- `min_value` - Minimum value in this hour
- `max_value` - Maximum value in this hour
- `avg_value` - Average value in this hour
- `stddev_value` - Standard deviation in this hour

**Example Query:**
```sql
-- Get hourly CPU usage statistics for the last 24 hours
SELECT
    hour,
    avg_value,
    min_value,
    max_value,
    data_point_count
FROM hourly_aggregates
WHERE metric_name = 'cpu.usage'
AND hour > NOW() - INTERVAL '24 hours'
ORDER BY hour DESC;
```

**Indexes:**
- `ix_hourly_aggregates_hour` - Fast time-based queries
- `ix_hourly_aggregates_resource_metric` - Fast lookups by resource and metric
- `ix_hourly_aggregates_datasource` - Fast lookups by datasource

---

### 3. resource_summary
**Purpose:** Overview of metrics activity per resource

**Columns:**
- `resource_id` - Resource ID
- `resource_hash` - Resource hash
- `attributes` - JSONB resource attributes
- `created_at` - Resource creation timestamp
- `updated_at` - Resource last update timestamp
- `metric_count` - Number of unique metrics for this resource
- `total_data_points` - Total number of data points
- `last_metric_timestamp` - Timestamp of most recent data point
- `first_metric_timestamp` - Timestamp of oldest data point
- `datasource_names` - Array of datasource names

**Example Query:**
```sql
-- Find most active resources
SELECT
    resource_hash,
    attributes->>'service.name' as service_name,
    metric_count,
    total_data_points,
    last_metric_timestamp
FROM resource_summary
ORDER BY total_data_points DESC
LIMIT 10;
```

**Indexes:**
- `ix_resource_summary_resource_hash` - Fast lookups by hash
- `ix_resource_summary_last_metric_timestamp` - Fast time-based queries
- `ix_resource_summary_metric_count` - Fast sorting by activity

---

### 4. datasource_metrics
**Purpose:** Catalog of all metrics for each datasource

**Columns:**
- `datasource_id` - Datasource ID
- `datasource_name` - Datasource name
- `datasource_version` - Datasource version
- `datasource_created_at` - Datasource creation timestamp
- `metric_definition_id` - Metric definition ID
- `metric_name` - Metric name
- `metric_unit` - Unit of measurement
- `metric_type` - Metric type
- `metric_description` - Metric description
- `resource_count` - Number of resources reporting this metric
- `total_data_points` - Total data points for this metric
- `last_data_point_timestamp` - Most recent data point
- `first_data_point_timestamp` - Oldest data point

**Example Query:**
```sql
-- Get all metrics for a specific datasource
SELECT
    metric_name,
    metric_unit,
    metric_type,
    resource_count,
    last_data_point_timestamp
FROM datasource_metrics
WHERE datasource_name = 'CPU_Usage'
ORDER BY resource_count DESC;
```

**Indexes:**
- `ix_datasource_metrics_datasource_name` - Fast lookups by datasource
- `ix_datasource_metrics_metric_name` - Fast lookups by metric
- `ix_datasource_metrics_resource_count` - Fast sorting by popularity

---

## Refresh Functions

### refresh_all_materialized_views()
Refreshes all materialized views concurrently.

```sql
SELECT refresh_all_materialized_views();
```

**When to use:**
- After bulk data loading
- As part of scheduled maintenance
- When views are significantly out of date

### refresh_latest_metrics()
Refreshes only the latest_metrics view.

```sql
SELECT refresh_latest_metrics();
```

**When to use:**
- When you need up-to-date latest values
- More frequently than full refresh
- After processing new metric data

### refresh_hourly_aggregates()
Refreshes only the hourly_aggregates view.

```sql
SELECT refresh_hourly_aggregates();
```

**When to use:**
- At the top of each hour
- After processing historical data
- For dashboard updates

---

## Performance Considerations

### Concurrent Refresh
All refresh operations use `REFRESH MATERIALIZED VIEW CONCURRENTLY`, which:
-  Allows queries while refreshing
-  Doesn't block readers
-  Maintains availability
-   Requires unique indexes (already created)

### Refresh Strategy
Recommended refresh schedule:
- **latest_metrics**: Every 5-15 minutes
- **hourly_aggregates**: Every hour at minute 5
- **resource_summary**: Every 30-60 minutes
- **datasource_metrics**: Every 2-4 hours (changes infrequently)

### Example Cron Schedule
```bash
# Refresh latest metrics every 15 minutes
*/15 * * * * psql -c "SELECT refresh_latest_metrics()"

# Refresh hourly aggregates at 5 past the hour
5 * * * * psql -c "SELECT refresh_hourly_aggregates()"

# Refresh all views daily at 2 AM
0 2 * * * psql -c "SELECT refresh_all_materialized_views()"
```

---

## Query Examples

### Example 1: Dashboard - Latest Metrics
```sql
-- Get latest values for all metrics on a specific resource
SELECT
    metric_name,
    value_double,
    value_int,
    metric_unit,
    timestamp
FROM latest_metrics
WHERE resource_hash = 'abc123...'
ORDER BY metric_name;
```

### Example 2: Time Series - Hourly Trends
```sql
-- Get hourly CPU usage trend for last week
SELECT
    hour,
    avg_value as cpu_percent,
    min_value,
    max_value
FROM hourly_aggregates
WHERE metric_name = 'cpu.usage'
AND resource_hash = 'abc123...'
AND hour > NOW() - INTERVAL '7 days'
ORDER BY hour;
```

### Example 3: Resource Discovery
```sql
-- Find resources with high metric activity
SELECT
    resource_hash,
    attributes->>'service.name' as service,
    attributes->>'host.name' as host,
    metric_count,
    last_metric_timestamp,
    datasource_names
FROM resource_summary
WHERE last_metric_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY metric_count DESC
LIMIT 20;
```

### Example 4: Metric Catalog
```sql
-- List all available metrics with their usage stats
SELECT
    datasource_name,
    metric_name,
    metric_type,
    metric_unit,
    resource_count,
    total_data_points
FROM datasource_metrics
ORDER BY datasource_name, metric_name;
```

---

## Troubleshooting

### View is Empty
If a materialized view is empty:
1. Check if base tables have data:
   ```sql
   SELECT COUNT(*) FROM metric_data;
   ```
2. Manually refresh the view:
   ```sql
   REFRESH MATERIALIZED VIEW latest_metrics;
   ```

### Slow Queries
If queries are slow despite using views:
1. Check if indexes exist:
   ```sql
   SELECT indexname FROM pg_indexes
   WHERE tablename = 'latest_metrics';
   ```
2. Analyze query plan:
   ```sql
   EXPLAIN ANALYZE
   SELECT * FROM latest_metrics WHERE resource_id = 123;
   ```

### Refresh Taking Too Long
If refresh is slow:
1. Check data volume:
   ```sql
   SELECT COUNT(*) FROM metric_data;
   ```
2. Consider using regular refresh (without CONCURRENTLY) during maintenance windows
3. Increase PostgreSQL memory settings

---

## Migration

### Applying the Migration
```bash
uv run alembic upgrade head
```

### Rolling Back
```bash
uv run alembic downgrade -1
```

This will remove all materialized views, their indexes, and refresh functions.
