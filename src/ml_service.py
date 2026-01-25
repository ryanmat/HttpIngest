# Description: ML data service for Precursor integration.
# Description: Provides inventory, training data, and profile coverage endpoints.

"""
ML Data Service for Precursor Integration.

This module provides endpoints that Precursor uses to:
1. Discover available metrics and resources (/api/ml/inventory)
2. Fetch training data in the expected format (/api/ml/training-data)
3. Check coverage against feature profiles (/api/ml/profile-coverage)
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
import asyncpg


# Feature profiles matching Precursor's config/features.yaml
FEATURE_PROFILES = {
    "collector": {
        "description": "LogicMonitor Collector self-monitoring metrics",
        "numerical_features": [
            "ExecuteTime",
            "ThreadCount",
            "CpuUsage",
            "MemoryUsage",
            "HeapMemoryUsage",
            "NonHeapMemoryUsage",
            "GCTime",
            "GCCount",
            "OpenFileDescriptors",
            "ProcessCpuLoad",
        ],
        "categorical_features": [
            "CollectorStatus",
            "DebugEnabled",
            "AutoUpdateEnabled",
        ],
    },
    "kubernetes": {
        "description": "Container orchestration workloads (K8s, ECS, Docker Swarm)",
        "numerical_features": [
            "cpuUsageNanoCores",
            "cpuLimits",
            "memoryUsageBytes",
            "memoryWorkingSetBytes",
            "memoryRssBytes",
            "memoryLimits",
            "networkRxBytes",
            "networkTxBytes",
            "kubePodContainerStatusRestartsTotal",
        ],
        "categorical_features": [
            "podConditionPhase",
            "kubePodStatusReady",
            "kubePodStatusScheduled",
            "kubePodContainerStatusRunning",
            "kubePodContainerStatusWaiting",
            "kubePodContainerStatusTerminated",
            "healthRunning",
            "healthWaiting",
        ],
    },
    "cloud_compute": {
        "description": "Cloud virtual machines and compute instances",
        "numerical_features": [
            "cpuUtilization",
            "memoryUtilization",
            "diskReadBytes",
            "diskWriteBytes",
            "diskReadOps",
            "diskWriteOps",
            "networkInBytes",
            "networkOutBytes",
            "networkPacketsIn",
            "networkPacketsOut",
        ],
        "categorical_features": [
            "instanceState",
            "statusCheckFailed",
            "statusCheckFailedInstance",
            "statusCheckFailedSystem",
            "maintenanceScheduled",
        ],
    },
    "network": {
        "description": "Network devices, switches, routers, firewalls",
        "numerical_features": [
            "ifInOctets",
            "ifOutOctets",
            "ifInErrors",
            "ifOutErrors",
            "ifInDiscards",
            "ifOutDiscards",
            "ifInUcastPkts",
            "ifOutUcastPkts",
            "cpuBusyPercent",
            "memoryUsedPercent",
        ],
        "categorical_features": [
            "ifOperStatus",
            "ifAdminStatus",
            "sysUpTimeChanged",
            "linkDown",
            "linkUp",
        ],
    },
    "database": {
        "description": "Database servers and managed database services",
        "numerical_features": [
            "activeConnections",
            "connectionPoolUsage",
            "queryExecutionTime",
            "queryThroughput",
            "cacheHitRatio",
            "bufferPoolUsage",
            "lockWaitTime",
            "deadlockCount",
            "replicationLag",
            "diskSpaceUsed",
        ],
        "categorical_features": [
            "serverState",
            "replicationStatus",
            "backupStatus",
            "highAvailabilityState",
            "maintenanceMode",
        ],
    },
    "application": {
        "description": "Application performance monitoring metrics",
        "numerical_features": [
            "requestRate",
            "errorRate",
            "responseTime",
            "responseTimeP50",
            "responseTimeP95",
            "responseTimeP99",
            "activeRequests",
            "queueDepth",
            "threadPoolUsage",
            "heapUsage",
        ],
        "categorical_features": [
            "serviceHealth",
            "circuitBreakerState",
            "deploymentStatus",
            "featureFlagEnabled",
            "maintenanceWindow",
        ],
    },
}


@dataclass
class InventoryResponse:
    """Response structure for /api/ml/inventory."""

    metrics: List[Dict[str, Any]] = field(default_factory=list)
    resources: List[Dict[str, Any]] = field(default_factory=list)
    datasources: List[Dict[str, Any]] = field(default_factory=list)
    time_range: Dict[str, Any] = field(default_factory=dict)
    total_data_points: int = 0


@dataclass
class ProfileCoverage:
    """Coverage information for a single profile."""

    name: str
    description: str
    coverage_percent: float
    available: List[str]
    missing: List[str]
    total_expected: int
    total_available: int


class MLDataService:
    """Service for ML data operations."""

    def __init__(self, pool: asyncpg.Pool):
        """Initialize with database connection pool."""
        self.pool = pool

    async def get_inventory(
        self,
        datasource: Optional[str] = None,
        resource_type: Optional[str] = None,
    ) -> InventoryResponse:
        """
        Get inventory of available metrics, resources, and time ranges.

        Args:
            datasource: Optional filter by datasource name
            resource_type: Optional filter by resource type

        Returns:
            InventoryResponse with available data summary
        """
        async with self.pool.acquire() as conn:
            # Get metrics with counts
            metrics_query = """
                SELECT
                    md.name,
                    md.unit,
                    md.metric_type,
                    d.name as datasource,
                    COUNT(m.id) as data_points
                FROM metric_definitions md
                JOIN datasources d ON md.datasource_id = d.id
                LEFT JOIN metric_data m ON m.metric_definition_id = md.id
            """
            params = []
            where_clauses = []

            if datasource:
                where_clauses.append(f"d.name ILIKE ${len(params) + 1}")
                params.append(f"%{datasource}%")

            if where_clauses:
                metrics_query += " WHERE " + " AND ".join(where_clauses)

            metrics_query += " GROUP BY md.id, md.name, md.unit, md.metric_type, d.name ORDER BY data_points DESC"

            metrics = await conn.fetch(metrics_query, *params)

            # Get resources with data point counts
            resources_query = """
                SELECT
                    r.id,
                    r.attributes->>'host.name' as host_name,
                    r.attributes->>'service.name' as service_name,
                    r.attributes as attributes,
                    COUNT(m.id) as data_points
                FROM resources r
                LEFT JOIN metric_data m ON m.resource_id = r.id
                GROUP BY r.id, r.attributes
                ORDER BY data_points DESC
                LIMIT 100
            """
            resources = await conn.fetch(resources_query)

            # Get datasources
            datasources_query = """
                SELECT
                    d.id,
                    d.name,
                    d.version,
                    COUNT(DISTINCT md.id) as metric_count,
                    COUNT(m.id) as data_points
                FROM datasources d
                LEFT JOIN metric_definitions md ON md.datasource_id = d.id
                LEFT JOIN metric_data m ON m.metric_definition_id = md.id
                GROUP BY d.id, d.name, d.version
                ORDER BY data_points DESC
            """
            datasources = await conn.fetch(datasources_query)

            # Get time range
            time_range_query = """
                SELECT
                    MIN(timestamp) as min_ts,
                    MAX(timestamp) as max_ts
                FROM metric_data
            """
            time_range = await conn.fetchrow(time_range_query)

            # Get total data points
            total_query = "SELECT COUNT(*) FROM metric_data"
            total = await conn.fetchval(total_query)

            return InventoryResponse(
                metrics=[
                    {
                        "name": m["name"],
                        "unit": m["unit"],
                        "type": m["metric_type"],
                        "datasource": m["datasource"],
                        "data_points": m["data_points"],
                    }
                    for m in metrics
                ],
                resources=[
                    {
                        "id": r["id"],
                        "host_name": r["host_name"],
                        "service_name": r["service_name"],
                        "data_points": r["data_points"],
                    }
                    for r in resources
                ],
                datasources=[
                    {
                        "id": d["id"],
                        "name": d["name"],
                        "version": d["version"],
                        "metric_count": d["metric_count"],
                        "data_points": d["data_points"],
                    }
                    for d in datasources
                ],
                time_range={
                    "start": time_range["min_ts"].isoformat() if time_range and time_range["min_ts"] else None,
                    "end": time_range["max_ts"].isoformat() if time_range and time_range["max_ts"] else None,
                },
                total_data_points=total or 0,
            )

    async def get_training_data(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        profile: Optional[str] = None,
        resource_id: Optional[int] = None,
        limit: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Get training data in Precursor-compatible format.

        Args:
            start_time: Start of time range (default: 24 hours ago)
            end_time: End of time range (default: now)
            profile: Feature profile to filter metrics
            resource_id: Optional resource ID filter
            limit: Maximum records to return
            offset: Pagination offset

        Returns:
            Training data records with metadata
        """
        if not start_time:
            start_time = datetime.now(timezone.utc) - timedelta(hours=24)
        if not end_time:
            end_time = datetime.now(timezone.utc)

        async with self.pool.acquire() as conn:
            # Build query with filters
            query = """
                SELECT
                    r.id AS resource_id,
                    r.attributes->>'host.name' AS host_name,
                    r.attributes->>'service.name' AS service_name,
                    md.name AS metric_name,
                    m.timestamp,
                    COALESCE(m.value_double, m.value_int::float) AS value,
                    m.attributes->>'dataSourceInstanceName' AS datasource_instance,
                    d.name AS datasource_name
                FROM metric_data m
                JOIN resources r ON m.resource_id = r.id
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                JOIN datasources d ON md.datasource_id = d.id
                WHERE m.timestamp >= $1 AND m.timestamp <= $2
            """
            params = [start_time, end_time]
            param_idx = 3

            # Filter by profile metrics
            if profile and profile in FEATURE_PROFILES:
                profile_metrics = (
                    FEATURE_PROFILES[profile]["numerical_features"]
                    + FEATURE_PROFILES[profile]["categorical_features"]
                )
                query += f" AND md.name = ANY(${param_idx})"
                params.append(profile_metrics)
                param_idx += 1

            # Filter by resource
            if resource_id:
                query += f" AND r.id = ${param_idx}"
                params.append(resource_id)
                param_idx += 1

            query += f" ORDER BY r.id, m.timestamp, md.name LIMIT ${param_idx} OFFSET ${param_idx + 1}"
            params.extend([limit, offset])

            data = await conn.fetch(query, *params)

            # Get total count for pagination
            count_query = """
                SELECT COUNT(*) FROM metric_data m
                WHERE m.timestamp >= $1 AND m.timestamp <= $2
            """
            total = await conn.fetchval(count_query, start_time, end_time)

            return {
                "data": [
                    {
                        "resource_id": row["resource_id"],
                        "host_name": row["host_name"],
                        "service_name": row["service_name"],
                        "metric_name": row["metric_name"],
                        "timestamp": row["timestamp"].isoformat(),
                        "value": row["value"],
                        "datasource_instance": row["datasource_instance"],
                        "datasource_name": row["datasource_name"],
                    }
                    for row in data
                ],
                "meta": {
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "profile": profile,
                    "resource_id": resource_id,
                    "limit": limit,
                    "offset": offset,
                    "returned": len(data),
                    "total": total,
                },
            }

    async def get_profile_coverage(
        self,
        profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Check coverage of available metrics against feature profiles.

        Args:
            profile: Optional single profile to check (default: all profiles)

        Returns:
            Coverage information for each profile
        """
        async with self.pool.acquire() as conn:
            # Get all available metric names
            metrics_query = "SELECT DISTINCT name FROM metric_definitions"
            available_metrics = {row["name"] for row in await conn.fetch(metrics_query)}

            profiles_to_check = (
                {profile: FEATURE_PROFILES[profile]}
                if profile and profile in FEATURE_PROFILES
                else FEATURE_PROFILES
            )

            coverages = []
            for profile_name, profile_def in profiles_to_check.items():
                expected = set(
                    profile_def["numerical_features"]
                    + profile_def["categorical_features"]
                )
                available = expected & available_metrics
                missing = expected - available_metrics

                coverage_pct = (len(available) / len(expected) * 100) if expected else 0

                coverages.append(
                    ProfileCoverage(
                        name=profile_name,
                        description=profile_def["description"],
                        coverage_percent=round(coverage_pct, 1),
                        available=sorted(available),
                        missing=sorted(missing),
                        total_expected=len(expected),
                        total_available=len(available),
                    )
                )

            return {
                "profiles": [
                    {
                        "name": c.name,
                        "description": c.description,
                        "coverage_percent": c.coverage_percent,
                        "available": c.available,
                        "missing": c.missing,
                        "total_expected": c.total_expected,
                        "total_available": c.total_available,
                    }
                    for c in coverages
                ],
                "available_metrics": sorted(available_metrics),
                "total_metrics": len(available_metrics),
            }
