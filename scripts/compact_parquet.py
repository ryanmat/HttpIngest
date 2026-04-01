# Description: One-time ADLS Parquet compaction script
# Description: Merges many small Parquet files into one file per day partition

"""Compact ADLS Parquet files from hour-level partitions to day-level.

Reads all Parquet files for each day partition, writes a single compacted
file, verifies row counts, then deletes the originals. Uses DuckDB for
fast Parquet read/write and azure-storage-file-datalake for file management.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
from datetime import datetime

import duckdb
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_ACCOUNT = "stlmingestdatalake"
DEFAULT_CONTAINER = "metrics"
DEFAULT_BASE_PATH = "otlp/metric_data"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Compact ADLS Parquet files")
    parser.add_argument(
        "--storage-account",
        default=DEFAULT_ACCOUNT,
        help=f"ADLS storage account (default: {DEFAULT_ACCOUNT})",
    )
    parser.add_argument(
        "--container",
        default=DEFAULT_CONTAINER,
        help=f"ADLS container (default: {DEFAULT_CONTAINER})",
    )
    parser.add_argument(
        "--base-path",
        default=DEFAULT_BASE_PATH,
        help=f"Base path for metric_data (default: {DEFAULT_BASE_PATH})",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=10,
        help="Minimum file count to trigger compaction (default: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files and counts without modifying anything",
    )
    return parser.parse_args()


def get_datalake_client(account_name: str) -> DataLakeServiceClient:
    """Create ADLS service client with Azure CLI credentials."""
    credential = DefaultAzureCredential()
    return DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential,
    )


def list_day_partitions(
    client: DataLakeServiceClient, container: str, base_path: str
) -> list[dict]:
    """List all day partitions and count Parquet files in each.

    Returns list of dicts with keys: path, year, month, day, file_count, files.
    """
    fs_client = client.get_file_system_client(container)
    partitions = {}

    for path in fs_client.get_paths(path=base_path, recursive=True):
        if not path.name.endswith(".parquet"):
            continue

        parts = path.name.split("/")
        day_key = None
        year = month = day = None

        for part in parts:
            if part.startswith("year="):
                year = part.split("=")[1]
            elif part.startswith("month="):
                month = part.split("=")[1]
            elif part.startswith("day="):
                day = part.split("=")[1]

        if year and month and day:
            day_key = f"year={year}/month={month}/day={day}"
            if day_key not in partitions:
                partitions[day_key] = {
                    "path": f"{base_path}/{day_key}",
                    "year": year,
                    "month": month,
                    "day": day,
                    "file_count": 0,
                    "files": [],
                }
            partitions[day_key]["file_count"] += 1
            partitions[day_key]["files"].append(path.name)

    result = sorted(partitions.values(), key=lambda p: (p["year"], p["month"], p["day"]))
    return result


def setup_duckdb_azure(account_name: str) -> duckdb.DuckDBPyConnection:
    """Set up DuckDB with Azure extension for ADLS reads."""
    conn = duckdb.connect()
    conn.execute("INSTALL azure; LOAD azure;")
    conn.execute("SET azure_transport_option_type = 'curl';")

    # CA bundle for SSL
    ca_paths = [
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/usr/share/ca-certificates/mozilla",
    ]
    for ca_path in ca_paths:
        if os.path.exists(ca_path):
            if not os.environ.get("CURL_CA_BUNDLE"):
                os.environ["CURL_CA_BUNDLE"] = ca_path
            break

    conn_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if conn_string:
        conn.execute(f"SET azure_storage_connection_string = '{conn_string}';")
    else:
        conn.execute(f"SET azure_account_name = '{account_name}';")
        conn.execute("SET azure_credential_chain = 'cli;env;managed_identity';")

    return conn


def compact_partition(
    conn: duckdb.DuckDBPyConnection,
    adls_client: DataLakeServiceClient,
    container: str,
    partition: dict,
    dry_run: bool = False,
) -> int:
    """Compact a single day partition into one Parquet file.

    Returns the number of rows in the compacted file, or 0 if skipped.
    """
    day_path = partition["path"]
    file_count = partition["file_count"]
    files = partition["files"]

    # Build az:// URIs for all files in this partition
    az_uri = f"az://{container}/{day_path}/**/*.parquet"

    # Count rows in source
    try:
        source_count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{az_uri}', "
            f"hive_partitioning=true, union_by_name=true)"
        ).fetchone()[0]
    except Exception as e:
        logger.error(f"Failed to read {az_uri}: {e}")
        return 0

    logger.info(
        f"  {day_path}: {file_count} files, {source_count} rows"
    )

    if dry_run:
        return source_count

    # Write compacted file to temp, then upload
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
        tmp_path = tmp.name
        conn.execute(
            f"COPY (SELECT * FROM read_parquet('{az_uri}', "
            f"hive_partitioning=true, union_by_name=true)) "
            f"TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

        # Verify compacted row count
        verify_count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{tmp_path}')"
        ).fetchone()[0]

        if verify_count != source_count:
            logger.error(
                f"Row count mismatch for {day_path}: "
                f"source={source_count}, compacted={verify_count}. Skipping."
            )
            return 0

        # Upload compacted file
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        compacted_name = f"{day_path}/compacted_{timestamp}.parquet"

        fs_client = adls_client.get_file_system_client(container)
        file_client = fs_client.get_file_client(compacted_name)

        with open(tmp_path, "rb") as f:
            file_client.upload_data(f.read(), overwrite=True)

        logger.info(f"  Uploaded {compacted_name} ({verify_count} rows)")

    # Delete original files (not the compacted one we just uploaded)
    deleted = 0
    for file_path in files:
        try:
            file_client = fs_client.get_file_client(file_path)
            file_client.delete_file()
            deleted += 1
        except Exception as e:
            logger.warning(f"  Failed to delete {file_path}: {e}")

    logger.info(f"  Deleted {deleted}/{len(files)} original files")

    return verify_count


def main() -> int:
    """Run compaction."""
    args = parse_args()

    logger.info(
        f"ADLS Parquet Compaction: account={args.storage_account}, "
        f"container={args.container}, threshold={args.threshold}, "
        f"dry_run={args.dry_run}"
    )

    adls_client = get_datalake_client(args.storage_account)
    conn = setup_duckdb_azure(args.storage_account)

    logger.info("Listing day partitions...")
    partitions = list_day_partitions(adls_client, args.container, args.base_path)

    if not partitions:
        logger.info("No partitions found.")
        return 0

    logger.info(f"Found {len(partitions)} day partitions")

    # Filter to partitions above threshold
    to_compact = [p for p in partitions if p["file_count"] >= args.threshold]
    skip_count = len(partitions) - len(to_compact)

    if skip_count > 0:
        logger.info(f"Skipping {skip_count} partitions below threshold ({args.threshold} files)")

    if not to_compact:
        logger.info("No partitions need compaction.")
        return 0

    total_files_before = sum(p["file_count"] for p in to_compact)
    total_rows = 0
    compacted_count = 0

    for partition in to_compact:
        rows = compact_partition(conn, adls_client, args.container, partition, args.dry_run)
        if rows > 0:
            total_rows += rows
            compacted_count += 1

    action = "Would compact" if args.dry_run else "Compacted"
    logger.info(
        f"{action} {compacted_count} partitions: "
        f"{total_files_before} files -> {compacted_count} files, "
        f"{total_rows} total rows"
    )

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
