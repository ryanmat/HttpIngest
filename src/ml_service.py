# Description: ML data service for Precursor integration.
# Description: Provides training data extraction and feature profile management.

"""
MLDataService for serving training data to Precursor ML models.

This module provides endpoints for:
- Inventory of available metrics, resources, and datasources
- Training data extraction with profile-based filtering
- Profile coverage analysis
- Feature profile definitions (mirrors Precursor's config/features.yaml)

Data Sources:
- PostgreSQL hot cache: Recent data (last 48h) for real-time queries
- Synapse/Data Lake: Historical data for ML training (queries Parquet files)
"""

import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass
import asyncpg

if TYPE_CHECKING:
    from src.synapse_client import SynapseClient

logger = logging.getLogger(__name__)

# Hot cache retention period (queries within this window use PostgreSQL)
HOT_CACHE_HOURS = int(os.getenv("HOT_CACHE_RETENTION_HOURS", "48"))

# ML API call metrics (exposed via /metrics Prometheus endpoint)
ml_api_metrics: Dict[str, Any] = {
    "requests_total": 0,
    "query_duration_seconds_total": 0.0,
    "rows_returned_total": 0,
    "errors_total": 0,
}


# Feature profiles matching Precursor's config/features.yaml (single source of truth).
# All metric names are actual LogicMonitor metric names from production data.
FEATURE_PROFILES = {
    # LogicMonitor Collector Self-Monitoring
    # Datasources: LogicMonitor_Collector_*
    # Metric names validated against Data Lake (stlmingestdatalake) 2026-01-28
    "collector": {
        "description": "LogicMonitor Collector self-monitoring metrics",
        "numerical_features": [
            # Execution performance (DataCollectingTasks, ConfigCollectingTask, ReporterTask)
            "ExecuteTime",
            "AvgExecTime",
            "MaxExecTime",
            "MinExecTime",
            "CollectTime",
            "DispatchTime",
            "ProcessExecuteTime",
            "SuccessExecuteTime",
            "SuccessRate",
            "PostProcessTime",
            "PrepareTime",
            "SendtoReporterTime",
            # CPU and JVM resources (ThreadCPUUsage, JVMMemoryPools, JVMGarbageCollection)
            "CpuUsage",
            "ProcessorCount",
            "Used",
            "Committed",
            "MaximumMemory",
            "CollectionTime",
            # Thread health (ThreadUsage, ThreadCPUUsage)
            "ThreadCount",
            "ThreadCnt",
            "RunningThreads",
            "RunnableThreadCnt",
            # Queue metrics (Throttler, BufferDataReporter, LMLogs)
            "QueueSize",
            "BigQueueSize",
            "TasksCountInQueue",
            "SizeOfBigQueue",
            "ItemsInMemoryQueue",
            # Failure indicators (DataCollectingTasks, ReporterTask, Heartbeat)
            "FailRate",
            "FailureRate",
            "Fail",
            "FailExecuteTime",
            "ExecuteFailed",
            "EnqueudFailed",
            "NanTaskRate",
            "HangCount",
            # Reporter and data pipeline (BufferDataReporter, ReporterTask)
            "ReportTaskCount",
            "UnReported",
            "EntryFailureCount",
            "EntryFailureRate",
            "EntrySuccessCount",
            "EntrySuccessRate",
            "ComposeFailedCount",
            # System health (DataCollectingTasks, ConfigCollectingTask)
            "RunningCount",
            "ProcessCount",
            "InstanceCount",
            "ExecutingTasks",
        ],
        "categorical_features": [
            "Active",
            "santabaConnection",
            "persistentQueueStatus",
        ],
    },
    # Kubernetes/Container Workloads
    # Datasources: Kubernetes_KSM_*, Kubernetes_PingK8s, Kubernetes_Service, Argus_*
    # Metric names validated against Data Lake (stlmingestdatalake) 2026-01-28
    "kubernetes": {
        "description": "Container orchestration workloads (K8s, ECS, Docker Swarm)",
        "numerical_features": [
            # CPU metrics (KSM_Pods)
            "cpuUsageNanoCores",
            "cpuUsageCoreNanoSeconds",
            "cpuLimits",
            "cpuRequests",
            # Memory metrics (KSM_Pods)
            "memoryUsageBytes",
            "memoryWorkingSetBytes",
            "memoryRssBytes",
            "memoryLimits",
            "memoryRequests",
            "memoryAvailableBytes",
            "memoryMajorPageFaults",
            "memoryPageFaults",
            # Network metrics (KSM_Pods)
            "networkRxBytes",
            "networkTxBytes",
            "networkRxErrors",
            "networkTxErrors",
            # Storage metrics (KSM_Pods)
            "fsUsedBytes",
            "fsAvailableBytes",
            "fsCapacityBytes",
            "volumeUsedBytes",
            "volumeAvailableBytes",
            "volumeCapacityBytes",
            "volumeInodes",
            "volumeInodesFree",
            # Pod/Container status (KSM_Pods)
            "kubePodContainerStatusRestartsTotal",
            "kubePodStartTime",
            # Deployment metrics (KSM_Deployments)
            "kubeDeploymentStatusReplicas",
            "kubeDeploymentStatusReplicasAvailable",
            "kubeDeploymentStatusReplicasReady",
            "kubeDeploymentStatusReplicasUnavailable",
            "kubeDeploymentStatusReplicasUpdated",
            "kubeDeploymentSpecReplicas",
            # Node metrics (KSM_Pods)
            "kubeNodeStatusAllocatableCpu",
            "kubeNodeStatusAllocatableMemory",
            "kubeNodeStatusCapacityCpu",
            "kubeNodeStatusCapacityMemory",
            # Node health signals (KSM_Pods)
            "kubeNodeStatusConditionDiskPressure",
            "kubeNodeStatusConditionMemoryPressure",
            "kubeNodeStatusConditionPIDPressure",
            # StatefulSet metrics (KSM_Pods)
            "kubeStatefulsetStatusReplicas",
            "kubeStatefulsetStatusReplicasReady",
            # DaemonSet metrics (KSM_Pods)
            "kubeDaemonsetStatusNumberReady",
            "kubeDaemonsetStatusNumberUnavailable",
            # Ping/Network (PingK8s)
            "average",
            "maxrtt",
            "minrtt",
        ],
        "categorical_features": [
            "podConditionPhase",
            "kubePodStatusReady",
            "kubePodStatusScheduled",
            "kubePodContainerStatusRunning",
            "kubePodContainerStatusWaiting",
            "kubePodContainerStatusTerminated",
            "kubePodContainerStatusReady",
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
    """
    Service for ML data operations.

    Supports hybrid query routing:
    - Recent data (within HOT_CACHE_HOURS): Query PostgreSQL hot cache
    - Historical data: Query Synapse Serverless (Data Lake Parquet files)
    """

    def __init__(
        self,
        pool: Optional[asyncpg.Pool] = None,
        synapse_client: Optional["SynapseClient"] = None,
    ):
        """
        Initialize with data sources.

        Args:
            pool: PostgreSQL connection pool for hot cache queries
            synapse_client: Synapse client for historical Data Lake queries
        """
        self.pool = pool
        self.synapse_client = synapse_client

    def _get_hot_cache_cutoff(self) -> datetime:
        """Get the cutoff time for hot cache queries."""
        return datetime.now(timezone.utc) - timedelta(hours=HOT_CACHE_HOURS)

    def _should_use_synapse(self, start_time: Optional[datetime]) -> bool:
        """
        Determine if query should use Synapse (historical data).

        Returns True if:
        - Synapse client is available AND
        - Start time is before the hot cache cutoff
        """
        if not self.synapse_client:
            return False
        if start_time is None:
            return False
        # Ensure timezone-aware comparison (start_time may be naive from URL params)
        cutoff = self._get_hot_cache_cutoff()
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        return start_time < cutoff

    async def get_inventory(
        self,
        datasource: Optional[str] = None,
        resource_type: Optional[str] = None,
    ) -> InventoryResponse:
        """
        Get inventory of available metrics, resources, and time ranges.

        Combines data from both hot cache (PostgreSQL) and Data Lake (Synapse)
        if both are available.
        """
        ml_api_metrics["requests_total"] += 1
        start_ts = time.monotonic()
        try:
            return await self._get_inventory_impl(datasource, resource_type)
        except Exception:
            ml_api_metrics["errors_total"] += 1
            raise
        finally:
            ml_api_metrics["query_duration_seconds_total"] += time.monotonic() - start_ts

    async def _get_inventory_impl(
        self,
        datasource: Optional[str] = None,
        resource_type: Optional[str] = None,
    ) -> InventoryResponse:
        """Internal implementation of get_inventory."""
        # If no hot cache, try Synapse only
        if not self.pool:
            if self.synapse_client:
                try:
                    synapse_inv = await self.synapse_client.get_inventory()
                    return InventoryResponse(
                        metrics=synapse_inv.get("metrics", []),
                        resources=synapse_inv.get("resources", []),
                        datasources=[],  # Synapse doesn't track datasources separately
                        time_range=synapse_inv.get("time_range", {}),
                        total_data_points=synapse_inv.get("total_data_points", 0),
                    )
                except Exception as e:
                    logger.error(f"Synapse inventory error: {e}")
                    return InventoryResponse(
                        metrics=[],
                        resources=[],
                        datasources=[],
                        time_range={"start": None, "end": None},
                        total_data_points=0,
                    )
            else:
                return InventoryResponse(
                    metrics=[],
                    resources=[],
                    datasources=[],
                    time_range={"start": None, "end": None},
                    total_data_points=0,
                )

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
        """
        Get training data in Precursor-compatible format.

        Automatically routes to the appropriate data source:
        - PostgreSQL hot cache: If start_time is within HOT_CACHE_HOURS
        - Synapse Data Lake: If start_time is older (historical data)
        """
        ml_api_metrics["requests_total"] += 1
        start_ts = time.monotonic()
        try:
            result = await self._get_training_data_impl(
                start_time, end_time, profile, resource_id, limit, offset,
            )
            ml_api_metrics["rows_returned_total"] += len(result.get("data", []))
            return result
        except Exception:
            ml_api_metrics["errors_total"] += 1
            raise
        finally:
            ml_api_metrics["query_duration_seconds_total"] += time.monotonic() - start_ts

    async def _get_training_data_impl(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        profile: Optional[str] = None,
        resource_id: Optional[int] = None,
        limit: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Internal implementation of get_training_data."""
        if start_time is None:
            start_time = datetime.now(timezone.utc) - timedelta(days=7)
        if end_time is None:
            end_time = datetime.now(timezone.utc)

        # Get metric names for profile filtering
        metric_names = None
        if profile and profile in FEATURE_PROFILES:
            profile_def = FEATURE_PROFILES[profile]
            metric_names = (
                profile_def["numerical_features"] +
                profile_def["categorical_features"]
            )

        # Route to Synapse for historical queries OR when hot cache is disabled
        # If hot cache (pool) is not available, always use Synapse regardless of time range
        use_synapse = self._should_use_synapse(start_time) or (self.synapse_client and not self.pool)
        logger.info(
            f"Training data routing: synapse_client={self.synapse_client is not None}, "
            f"pool={self.pool is not None}, use_synapse={use_synapse}, start_time={start_time}"
        )
        if use_synapse:
            logger.info(f"Routing training data query to Synapse (start_time={start_time})")
            return await self._get_training_data_from_synapse(
                start_time=start_time,
                end_time=end_time,
                metric_names=metric_names,
                profile=profile,
                limit=limit,
                offset=offset,
            )

        # Route to PostgreSQL hot cache for recent queries
        if not self.pool:
            return {
                "data": [],
                "meta": {
                    "total": 0,
                    "limit": limit,
                    "offset": offset,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "profile": profile,
                    "error": "No data source available (hot cache disabled, Synapse not configured)",
                },
            }

        logger.info(f"Routing training data query to PostgreSQL hot cache (start_time={start_time})")
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
                    "source": "postgresql_hot_cache",
                },
            }

    async def _get_training_data_from_synapse(
        self,
        start_time: datetime,
        end_time: datetime,
        metric_names: Optional[List[str]] = None,
        profile: Optional[str] = None,
        limit: int = 10000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Get training data from Synapse (Data Lake Parquet files).

        Used for historical queries beyond the hot cache window.
        """
        if not self.synapse_client:
            return {
                "data": [],
                "meta": {
                    "total": 0,
                    "limit": limit,
                    "offset": offset,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "profile": profile,
                    "error": "Synapse client not configured",
                },
            }

        try:
            result = await self.synapse_client.get_training_data(
                start_time=start_time,
                end_time=end_time,
                metric_names=metric_names,
                limit=limit,
                offset=offset,
            )

            # Add profile to metadata
            result["meta"]["profile"] = profile

            return result

        except Exception as e:
            logger.error(f"Synapse query error: {e}", exc_info=True)
            return {
                "data": [],
                "meta": {
                    "total": 0,
                    "limit": limit,
                    "offset": offset,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "profile": profile,
                    "source": "synapse_datalake",
                    "error": str(e),
                },
            }

    async def get_profile_coverage(
        self,
        profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Check coverage of available metrics against feature profiles."""
        ml_api_metrics["requests_total"] += 1
        start_ts = time.monotonic()
        try:
            return await self._get_profile_coverage_impl(profile)
        except Exception:
            ml_api_metrics["errors_total"] += 1
            raise
        finally:
            ml_api_metrics["query_duration_seconds_total"] += time.monotonic() - start_ts

    async def _get_profile_coverage_impl(
        self,
        profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Internal implementation of get_profile_coverage."""
        if not self.pool:
            return {
                "profiles": [],
                "error": "Hot cache not available - profile coverage requires PostgreSQL",
            }

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

        Note: This endpoint requires PostgreSQL hot cache for real-time quality checks.
        """
        ml_api_metrics["requests_total"] += 1
        start_ts = time.monotonic()
        try:
            return await self._get_data_quality_impl(profile, hours)
        except Exception:
            ml_api_metrics["errors_total"] += 1
            raise
        finally:
            ml_api_metrics["query_duration_seconds_total"] += time.monotonic() - start_ts

    async def _get_data_quality_impl(
        self,
        profile: Optional[str] = None,
        hours: int = 24,
    ) -> Dict[str, Any]:
        """Internal implementation of get_data_quality."""
        if not self.pool:
            return {
                "summary": {
                    "overall_score": 0,
                    "error": "Hot cache not available - data quality checks require PostgreSQL",
                },
                "freshness": [],
                "gaps": [],
                "ranges": [],
            }

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
