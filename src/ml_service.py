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
                JOIN datasources ds ON m.datasource_id = ds.id
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
                LEFT JOIN metric_data m ON ds.id = m.datasource_id
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
                JOIN datasources ds ON m.datasource_id = ds.id
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
