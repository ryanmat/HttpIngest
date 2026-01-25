# Description: ML data service for Precursor integration.
# Description: Provides training data extraction and feature profile management.

"""
MLDataService for serving training data to Precursor ML models.

This module provides endpoints for:
- Inventory of available metrics, resources, and datasources
- Training data extraction with profile-based filtering
- Profile coverage analysis
- Feature profile definitions (mirrors Precursor's config/features.yaml)
"""

import math
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
import asyncpg


# Feature profiles matching Precursor's config/features.yaml (single source of truth).
# All metric names are actual LogicMonitor metric names from production data.
FEATURE_PROFILES = {
    # LogicMonitor Collector Self-Monitoring
    # Datasources: LogicMonitor_Collector_*
    "collector": {
        "description": "LogicMonitor Collector self-monitoring metrics",
        "numerical_features": [
            # Execution performance
            "ExecuteTime",
            "AvgExecTime",
            "MaxExecTime",
            "MinExecTime",
            "CollectTime",
            "DispatchTime",
            "ProcessExecuteTime",
            # Resource utilization
            "CpuUsage",
            "cpuUsage",
            "HeapUsage",
            "NonHeapUsage",
            "HeapCommit",
            "NonHeapCommit",
            "MemoryUsed",
            "CollectorMemory",
            # Thread health
            "ThreadCount",
            "DaemonThreadCount",
            "RunningThreads",
            "CurrentThreads",
            "RunnableThreadCnt",
            # Queue metrics
            "QueueSize",
            "QueueLength",
            "BigQueueSize",
            "TasksCountInQueue",
            # Failure indicators
            "FailRate",
            "FailureRate",
            "Failure",
            "FailExecuteTime",
            "ExecuteFailed",
            "EnqueudFailed",
            # System health
            "Uptime",
            "RunningCount",
            "ReportTaskCount",
            "ProcessCount",
            "InstanceCount",
        ],
        "categorical_features": [
            "Active",
            "santabaConnection",
            "persistentQueueStatus",
        ],
    },
    # Kubernetes/Container Workloads
    # Datasources: Kubernetes_*, Argus_*, KubeVirt_*
    "kubernetes": {
        "description": "Container orchestration workloads (K8s, ECS, Docker Swarm)",
        "numerical_features": [
            # CPU metrics
            "cpuUsageNanoCores",
            "cpuUsageCoreNanoSeconds",
            "cpuLimits",
            "cpuRequests",
            "cpu_usage_seconds",
            "cpu_system_seconds",
            "cpu_user_seconds",
            # Memory metrics
            "memoryUsageBytes",
            "memoryWorkingSetBytes",
            "memoryRssBytes",
            "memoryLimits",
            "memoryRequests",
            "memoryAvailableBytes",
            "memoryMajorPageFaults",
            "memoryPageFaults",
            "memory_usage_percent",
            "memory_used_bytes",
            "memory_available_bytes",
            "memory_resident_bytes",
            # Network metrics
            "networkRxBytes",
            "networkTxBytes",
            "networkRxErrors",
            "networkTxErrors",
            "rx_bytes",
            "tx_bytes",
            "rx_packets",
            "tx_packets",
            "rx_errors",
            "tx_errors",
            # Storage metrics
            "fsUsedBytes",
            "fsAvailableBytes",
            "fsCapacityBytes",
            "volumeUsedBytes",
            "volumeAvailableBytes",
            "read_bytes",
            "write_bytes",
            "read_iops",
            "write_iops",
            # Pod/Container status
            "kubePodContainerStatusRestartsTotal",
            "statusRestartCount",
            "kubeDeploymentStatusReplicas",
            "kubeDeploymentStatusReplicasAvailable",
            "kubeDeploymentStatusReplicasReady",
            # Node metrics
            "kubeNodeStatusAllocatableCpu",
            "kubeNodeStatusAllocatableMemory",
            "kubeNodeStatusCapacityCpu",
            "kubeNodeStatusCapacityMemory",
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
            "kubeNodeStatusConditionReady",
            "kubeDeploymentStatusAvailableTrue",
            "status",
        ],
    },
    # Cloud Compute (Windows/Linux VMs)
    # Datasources: Win*, Microsoft_Windows_*, Linux_OpenMetrics_*
    "cloud_compute": {
        "description": "Cloud virtual machines and compute instances",
        "numerical_features": [
            # CPU metrics
            "PercentProcessorTime",
            "PercentIdleTime",
            "ProcessorQueueLength",
            "cpuBusySeconds",
            "cpuIdleSeconds",
            "cpuUserSeconds",
            "cpuSystemSeconds",
            "cpuIowaitSeconds",
            "load_1",
            "load_5",
            "load_15",
            # Memory metrics
            "FreePhysicalMemory",
            "FreeVirtualMemory",
            "TotalVisibleMemorySize",
            "TotalVirtualMemorySize",
            "MemTotal",
            "MemFree",
            "MemAvailable",
            "Cached",
            "Buffers",
            "SwapFree",
            "SwapTotal",
            "CacheBytes",
            "PoolNonpagedBytes",
            "PoolPagedBytes",
            # Disk metrics
            "DiskReadBytesPerSec",
            "DiskWriteBytesPerSec",
            "DiskReadsPerSec",
            "DiskWritesPerSec",
            "AvgDiskSecPerRead",
            "AvgDiskSecPerWrite",
            "PercentDiskReadTime",
            "PercentDiskWriteTime",
            "CurrentDiskQueueLength",
            "FreeSpace",
            "Capacity",
            # Network metrics
            "BytesReceivedPerSec",
            "BytesSentPerSec",
            "PacketsReceivedUnicastPerSec",
            "PacketsSentUnicastPerSec",
            "ConnectionsEstablished",
            "ConnectionsActive",
            # System metrics
            "SystemUpTime",
            "NumberOfProcesses",
        ],
        "categorical_features": [
            "Active",
            "isAlive",
        ],
    },
    # Network Devices (SNMP)
    # Datasources: SNMP*, WinIf-*, Cisco_*
    "network": {
        "description": "Network devices, switches, routers, firewalls",
        "numerical_features": [
            # Interface traffic (SNMP standard names)
            "BytesReceivedPerSec",
            "BytesSentPerSec",
            "PacketsReceivedUnicastPerSec",
            "PacketsSentUnicastPerSec",
            "PacketsReceivedNonUnicastPerSec",
            "PacketsSentNonUnicastPerSec",
            # Interface errors
            "PacketsReceivedDiscarded",
            "PacketsOutboundDiscarded",
            # TCP/UDP metrics
            "SegmentsReceivedPerSec",
            "SegmentsSentPerSec",
            "SegmentsRetransmittedPerSec",
            "DatagramsReceivedPerSec",
            "DatagramsSentPerSec",
            "DatagramsReceivedErrors",
            "ConnectionsEstablished",
            "ConnectionsActive",
            "ConnectionsPassive",
            "ConnectionsReset",
            "ConnectionFailures",
        ],
        "categorical_features": [
            "Active",
            "isAlive",
        ],
    },
    # Database (SQL/NoSQL)
    # Placeholder - add actual LM database datasource metrics when available
    "database": {
        "description": "Database servers and managed database services",
        "numerical_features": [
            # Connection metrics
            "ConnectionsEstablished",
            "ConnectionsActive",
            # Query metrics (from collector script cache as proxy)
            "QueryTime",
            "AvgExecTime",
            "MaxExecTime",
            # Cache metrics
            "CacheBytes",
            "cachedEntries",
            "expiredEntries",
        ],
        "categorical_features": [
            "Active",
            "isAlive",
        ],
    },
    # Application/Service Monitoring (.NET, IIS, Java)
    # Datasources: .NetCLR*, Application Pools-, Microsoft_IIS_*
    "application": {
        "description": "Application performance monitoring metrics",
        "numerical_features": [
            # Memory metrics
            "NumBytesinallHeaps",
            "BytesinLoaderHeap",
            # GC metrics
            "PercentTimeinGC",
            "NumberGen0Collections",
            "NumberGen1Collections",
            "NumberGen2Collections",
            # Thread metrics
            "NumberofcurrentlogicalThreads",
            "NumberofcurrentphysicalThreads",
            "Numberofcurrentrecognizedthreads",
            "Numberoftotalrecognizedthreads",
            "TotalNumberofContentions",
            # Exception metrics
            "NumExcepsThrownSec",
            # IIS metrics
            "CurrentConnections",
            "BytesReceivedPerSec",
            "BytesSentPerSec",
            "TotalGetRequests",
            "TotalMethodRequests",
            "NotFoundErrorsPerSec",
            "ServiceUptime",
            # Worker process metrics
            "CurrentWorkerProcesses",
            "WorkerProcessesCreated",
            "TotalWorkerProcessFailures",
            "CurrentApplicationPoolUptime",
        ],
        "categorical_features": [
            "CurrentApplicationPoolState",
            "Active",
        ],
    },
}


@dataclass
class InventoryResponse:
    """Response model for inventory endpoint."""

    metrics: List[Dict[str, Any]]
    resources: List[Dict[str, Any]]
    datasources: List[Dict[str, Any]]
    time_range: Dict[str, Any]
    total_data_points: int


class MLDataService:
    """Service for ML data operations."""

    def __init__(self, pool: asyncpg.Pool):
        """Initialize with database pool."""
        self.pool = pool

    async def get_inventory(
        self,
        datasource: Optional[str] = None,
        resource_type: Optional[str] = None,
    ) -> InventoryResponse:
        """Get inventory of available metrics, resources, and time ranges."""
        async with self.pool.acquire() as conn:
            # Get metrics with counts
            metrics_query = """
                SELECT
                    md.name,
                    md.unit,
                    md.metric_type,
                    ds.name as datasource,
                    COUNT(*) as data_points
                FROM metric_data m
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                JOIN datasources ds ON md.datasource_id = ds.id
                WHERE 1=1
            """
            params = []
            if datasource:
                params.append(datasource)
                metrics_query += f" AND ds.name ILIKE ${len(params)}"

            metrics_query += " GROUP BY md.name, md.unit, md.metric_type, ds.name"
            metrics = await conn.fetch(metrics_query, *params)

            # Get resources
            resources_query = """
                SELECT
                    r.id,
                    r.attributes->>'hostName' as host_name,
                    r.attributes->>'serviceName' as service_name,
                    COUNT(m.id) as data_points
                FROM resources r
                LEFT JOIN metric_data m ON r.id = m.resource_id
                GROUP BY r.id
            """
            resources = await conn.fetch(resources_query)

            # Get datasources
            datasources_query = """
                SELECT DISTINCT ds.name, COUNT(m.id) as data_points
                FROM datasources ds
                LEFT JOIN metric_definitions md ON ds.id = md.datasource_id
                LEFT JOIN metric_data m ON md.id = m.metric_definition_id
                GROUP BY ds.name
                ORDER BY data_points DESC
            """
            datasources = await conn.fetch(datasources_query)

            # Get time range
            time_range = await conn.fetchrow("""
                SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts
                FROM metric_data
            """)

            # Get total count
            total = await conn.fetchval("SELECT COUNT(*) FROM metric_data")

            return InventoryResponse(
                metrics=[dict(m) for m in metrics],
                resources=[dict(r) for r in resources],
                datasources=[dict(d) for d in datasources],
                time_range={
                    "start": time_range["min_ts"].isoformat() if time_range["min_ts"] else None,
                    "end": time_range["max_ts"].isoformat() if time_range["max_ts"] else None,
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
        """Get training data in Precursor-compatible format."""
        if start_time is None:
            start_time = datetime.now(timezone.utc) - timedelta(days=7)
        if end_time is None:
            end_time = datetime.now(timezone.utc)

        async with self.pool.acquire() as conn:
            # Build query
            query = """
                SELECT
                    m.resource_id,
                    r.attributes->>'hostName' as host_name,
                    r.attributes->>'serviceName' as service_name,
                    md.name as metric_name,
                    m.timestamp,
                    COALESCE(m.value_double, m.value_int::float) as value,
                    m.attributes->>'dataSourceInstanceName' as datasource_instance,
                    ds.name as datasource_name
                FROM metric_data m
                JOIN resources r ON m.resource_id = r.id
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                JOIN datasources ds ON md.datasource_id = ds.id
                WHERE m.timestamp >= $1 AND m.timestamp <= $2
            """
            params = [start_time, end_time]

            # Filter by profile metrics if specified
            if profile and profile in FEATURE_PROFILES:
                profile_def = FEATURE_PROFILES[profile]
                all_features = (
                    profile_def["numerical_features"] +
                    profile_def["categorical_features"]
                )
                params.append(all_features)
                query += f" AND md.name = ANY(${len(params)})"

            if resource_id:
                params.append(resource_id)
                query += f" AND m.resource_id = ${len(params)}"

            query += " ORDER BY m.resource_id, m.timestamp, md.name"
            query += f" LIMIT {limit} OFFSET {offset}"

            rows = await conn.fetch(query, *params)

            # Get total count for pagination
            count_query = """
                SELECT COUNT(*) FROM metric_data m
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                WHERE m.timestamp >= $1 AND m.timestamp <= $2
            """
            count_params = [start_time, end_time]
            if profile and profile in FEATURE_PROFILES:
                profile_def = FEATURE_PROFILES[profile]
                all_features = (
                    profile_def["numerical_features"] +
                    profile_def["categorical_features"]
                )
                count_params.append(all_features)
                count_query += f" AND md.name = ANY(${len(count_params)})"
            total = await conn.fetchval(count_query, *count_params)

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
                    for row in rows
                ],
                "meta": {
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "profile": profile,
                },
            }

    async def get_profile_coverage(
        self,
        profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Check coverage of available metrics against feature profiles."""
        async with self.pool.acquire() as conn:
            # Get all unique metric names in the database
            available_metrics = await conn.fetch(
                "SELECT DISTINCT name FROM metric_definitions"
            )
            available_set = {m["name"] for m in available_metrics}

            profiles_to_check = (
                {profile: FEATURE_PROFILES[profile]}
                if profile and profile in FEATURE_PROFILES
                else FEATURE_PROFILES
            )

            result = {"profiles": []}

            for name, profile_def in profiles_to_check.items():
                all_features = (
                    profile_def["numerical_features"] +
                    profile_def["categorical_features"]
                )
                available = [f for f in all_features if f in available_set]
                missing = [f for f in all_features if f not in available_set]

                result["profiles"].append({
                    "name": name,
                    "description": profile_def["description"],
                    "coverage_percent": (
                        len(available) / len(all_features) * 100
                        if all_features else 0
                    ),
                    "available": available,
                    "missing": missing,
                    "total_expected": len(all_features),
                    "total_available": len(available),
                })

            return result

    async def get_data_quality(
        self,
        profile: Optional[str] = None,
        hours: int = 24,
    ) -> Dict[str, Any]:
        """
        Assess data quality for ML training readiness.

        Returns:
        - freshness: Time since last data point per resource
        - gaps: Detected gaps in time series data
        - ranges: Value range statistics and anomalies
        - summary: Overall quality score
        """
        async with self.pool.acquire() as conn:
            now = datetime.now(timezone.utc)
            lookback = now - timedelta(hours=hours)

            # Get profile metrics if specified
            metric_filter = None
            if profile and profile in FEATURE_PROFILES:
                profile_def = FEATURE_PROFILES[profile]
                metric_filter = (
                    profile_def["numerical_features"] +
                    profile_def["categorical_features"]
                )

            # Freshness: Last update time per resource
            freshness_query = """
                SELECT
                    r.id as resource_id,
                    r.attributes->>'hostName' as host_name,
                    MAX(m.timestamp) as last_update,
                    COUNT(*) as data_points
                FROM metric_data m
                JOIN resources r ON m.resource_id = r.id
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                WHERE m.timestamp >= $1
            """
            params: List[Any] = [lookback]
            if metric_filter:
                params.append(metric_filter)
                freshness_query += f" AND md.name = ANY(${len(params)})"
            freshness_query += " GROUP BY r.id ORDER BY last_update DESC"

            freshness_rows = await conn.fetch(freshness_query, *params)

            freshness_data = []
            stale_resources = 0
            for row in freshness_rows:
                last_update = row["last_update"]
                age_minutes = (now - last_update).total_seconds() / 60
                is_stale = age_minutes > 10  # Stale if no data in 10 minutes
                if is_stale:
                    stale_resources += 1
                freshness_data.append({
                    "resource_id": int(row["resource_id"]),
                    "host_name": row["host_name"],
                    "last_update": last_update.isoformat(),
                    "age_minutes": round(age_minutes, 1),
                    "data_points": int(row["data_points"]),
                    "is_stale": is_stale,
                })

            # Gap detection: Find time periods with missing data
            gap_query = """
                WITH time_diffs AS (
                    SELECT
                        r.id as resource_id,
                        r.attributes->>'hostName' as host_name,
                        m.timestamp,
                        LAG(m.timestamp) OVER (
                            PARTITION BY r.id ORDER BY m.timestamp
                        ) as prev_timestamp
                    FROM metric_data m
                    JOIN resources r ON m.resource_id = r.id
                    JOIN metric_definitions md ON m.metric_definition_id = md.id
                    WHERE m.timestamp >= $1
            """
            gap_params: List[Any] = [lookback]
            if metric_filter:
                gap_params.append(metric_filter)
                gap_query += f" AND md.name = ANY(${len(gap_params)})"
            gap_query += """
                )
                SELECT
                    resource_id,
                    host_name,
                    prev_timestamp as gap_start,
                    timestamp as gap_end,
                    EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 60 as gap_minutes
                FROM time_diffs
                WHERE prev_timestamp IS NOT NULL
                  AND EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) > 600
                ORDER BY gap_minutes DESC
                LIMIT 50
            """

            gap_rows = await conn.fetch(gap_query, *gap_params)

            gaps_data = [
                {
                    "resource_id": int(row["resource_id"]),
                    "host_name": row["host_name"],
                    "gap_start": row["gap_start"].isoformat(),
                    "gap_end": row["gap_end"].isoformat(),
                    "gap_minutes": round(float(row["gap_minutes"]), 1),
                }
                for row in gap_rows
            ]

            # Value ranges: Statistics per metric
            range_query = """
                SELECT
                    md.name as metric_name,
                    COUNT(*) as sample_count,
                    AVG(COALESCE(m.value_double, m.value_int::float)) as avg_value,
                    MIN(COALESCE(m.value_double, m.value_int::float)) as min_value,
                    MAX(COALESCE(m.value_double, m.value_int::float)) as max_value,
                    STDDEV(COALESCE(m.value_double, m.value_int::float)) as stddev
                FROM metric_data m
                JOIN metric_definitions md ON m.metric_definition_id = md.id
                WHERE m.timestamp >= $1
            """
            range_params: List[Any] = [lookback]
            if metric_filter:
                range_params.append(metric_filter)
                range_query += f" AND md.name = ANY(${len(range_params)})"
            range_query += " GROUP BY md.name ORDER BY sample_count DESC"

            range_rows = await conn.fetch(range_query, *range_params)

            def safe_round(val: Any, decimals: int = 4) -> Optional[float]:
                """Round a value, returning None for NaN, Inf, or None."""
                if val is None:
                    return None
                try:
                    # Convert to float first (handles Decimal, int, etc.)
                    fval = float(val)
                    if math.isnan(fval) or math.isinf(fval):
                        return None
                    return round(fval, decimals)
                except (TypeError, ValueError, OverflowError):
                    return None

            ranges_data = [
                {
                    "metric_name": row["metric_name"],
                    "sample_count": int(row["sample_count"]),
                    "avg_value": safe_round(row["avg_value"]),
                    "min_value": safe_round(row["min_value"]),
                    "max_value": safe_round(row["max_value"]),
                    "stddev": safe_round(row["stddev"]),
                }
                for row in range_rows
            ]

            # Calculate quality score
            total_resources = len(freshness_data)
            fresh_resources = total_resources - stale_resources
            freshness_score = (fresh_resources / total_resources * 100) if total_resources > 0 else 0

            total_gaps = len(gaps_data)
            gap_score = max(0, 100 - (total_gaps * 5))  # -5 points per gap, min 0

            total_metrics = len(ranges_data)
            metrics_with_data = sum(1 for r in ranges_data if r["sample_count"] >= 10)
            coverage_score = (metrics_with_data / total_metrics * 100) if total_metrics > 0 else 0

            overall_score = (freshness_score + gap_score + coverage_score) / 3

            return {
                "summary": {
                    "overall_score": round(overall_score, 1),
                    "freshness_score": round(freshness_score, 1),
                    "gap_score": round(gap_score, 1),
                    "coverage_score": round(coverage_score, 1),
                    "total_resources": total_resources,
                    "stale_resources": stale_resources,
                    "total_gaps": total_gaps,
                    "total_metrics": total_metrics,
                    "lookback_hours": hours,
                    "profile": profile,
                    "checked_at": now.isoformat(),
                },
                "freshness": freshness_data[:20],  # Top 20 most recent
                "gaps": gaps_data,
                "ranges": ranges_data[:30],  # Top 30 by sample count
            }
