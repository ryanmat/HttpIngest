# Description: Ingestion router for OTLP data to Azure Data Lake Gen2
# Description: Handles parsing and routing of OTLP data to Parquet storage

"""
Ingestion Router for Azure Data Lake Gen2.

Routes parsed OTLP data to Azure Data Lake Gen2 as partitioned Parquet files
for ML training and historical queries.
"""

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from src.datalake_writer import DataLakeWriter
from src.otlp_parser import (
    deduplicate_datasources,
    deduplicate_metric_definitions,
    deduplicate_resources,
    parse_otlp,
)

logger = logging.getLogger(__name__)


@dataclass
class IngestionConfig:
    """Configuration for ingestion routing."""

    write_to_datalake: bool = True

    @classmethod
    def from_env(cls) -> "IngestionConfig":
        """Create config from environment variables."""
        return cls(
            write_to_datalake=os.getenv("WRITE_TO_DATALAKE", "true").lower() == "true",
        )


@dataclass
class IngestionStats:
    """Statistics from an ingestion operation."""

    resources: int
    datasources: int
    metric_definitions: int
    metric_data: int
    datalake_written: int
    errors: list

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "resources": self.resources,
            "datasources": self.datasources,
            "metric_definitions": self.metric_definitions,
            "metric_data": self.metric_data,
            "datalake_written": self.datalake_written,
            "errors": self.errors,
        }


class IngestionRouter:
    """Routes ingested OTLP data to Azure Data Lake Gen2.

    This is the main entry point for data ingestion, writing parsed and
    deduplicated OTLP data to partitioned Parquet files in ADLS Gen2.
    """

    def __init__(
        self,
        datalake_writer: Optional[DataLakeWriter] = None,
        config: Optional[IngestionConfig] = None,
    ):
        self.datalake_writer = datalake_writer
        self.config = config or IngestionConfig.from_env()

    async def ingest(self, payload: Dict[str, Any]) -> IngestionStats:
        """Ingest OTLP payload to Data Lake.

        Args:
            payload: Raw OTLP JSON payload

        Returns:
            IngestionStats with counts and any errors
        """
        errors = []

        # Parse OTLP payload
        try:
            parsed = parse_otlp(payload)
        except Exception as e:
            logger.error(f"Failed to parse OTLP payload: {e}")
            return IngestionStats(
                resources=0,
                datasources=0,
                metric_definitions=0,
                metric_data=0,
                datalake_written=0,
                errors=[f"Parse error: {str(e)}"],
            )

        # Deduplicate
        unique_resources = deduplicate_resources(parsed.resources)
        unique_datasources = deduplicate_datasources(parsed.datasources)
        unique_metric_defs = deduplicate_metric_definitions(parsed.metric_definitions)

        stats = IngestionStats(
            resources=len(unique_resources),
            datasources=len(unique_datasources),
            metric_definitions=len(unique_metric_defs),
            metric_data=len(parsed.metric_data),
            datalake_written=0,
            errors=[],
        )

        # Write to Data Lake
        if self.config.write_to_datalake and self.datalake_writer:
            try:
                stats.datalake_written = await self.datalake_writer.write_metrics(
                    parsed
                )
            except Exception as e:
                error_msg = f"Data Lake write error: {str(e)}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)

        stats.errors = errors

        if stats.datalake_written > 0:
            logger.info(
                f"Ingested: {stats.metric_data} data points "
                f"(datalake: {stats.datalake_written})"
            )

        return stats

    async def get_status(self) -> Dict[str, Any]:
        """Get current status of the ingestion router."""
        status = {
            "config": {
                "write_to_datalake": self.config.write_to_datalake,
            },
            "datalake": None,
        }

        if self.datalake_writer:
            status["datalake"] = self.datalake_writer.get_buffer_stats()

        return status
