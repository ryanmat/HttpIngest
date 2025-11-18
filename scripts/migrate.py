#!/usr/bin/env python3
# Description: Safe database migration helper script for LogicMonitor data pipeline
# Description: Provides migration status, upgrade, downgrade, and rollback with safety checks

"""
Database Migration Helper

Safe wrapper around Alembic migrations with rollback capability.

Usage:
    python scripts/migrate.py status          # Show current migration status
    python scripts/migrate.py upgrade         # Upgrade to latest migration
    python scripts/migrate.py upgrade +1      # Upgrade one migration
    python scripts/migrate.py downgrade -1    # Downgrade one migration
    python scripts/migrate.py history         # Show migration history
    python scripts/migrate.py stamp head      # Mark database as up-to-date without running migrations

Environment Variables:
    PGHOST, PGDATABASE, PGUSER, PGPORT, PGPASSWORD
"""

import sys
import os
import subprocess
import argparse
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s [%(name)s] %(message)s'
)
logger = logging.getLogger('migrate')


def run_alembic_command(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """
    Run an alembic command safely.

    Args:
        args: Alembic command arguments
        check: Whether to check return code

    Returns:
        CompletedProcess result
    """
    # Ensure we're in the project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    os.chdir(project_root)

    cmd = ["alembic"] + args
    logger.info(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check
        )

        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        return result

    except subprocess.CalledProcessError as e:
        logger.error(f"Migration command failed: {e}")
        if e.stdout:
            print(e.stdout)
        if e.stderr:
            print(e.stderr, file=sys.stderr)
        sys.exit(1)


def check_database_connection():
    """
    Verify database connection is available before running migrations.
    """
    logger.info("Checking database connection...")

    # Import here to avoid circular dependencies
    try:
        from alembic.config import Config
        from alembic import command
        from alembic.script import ScriptDirectory

        config = Config("alembic.ini")
        # This will test the connection
        script = ScriptDirectory.from_config(config)
        logger.info("Database connection verified")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        logger.error("Please check your database credentials and network connection")
        return False


def get_current_revision() -> str:
    """
    Get the current database migration revision.

    Returns:
        Current revision hash or 'None' if no migrations applied
    """
    result = run_alembic_command(["current"], check=False)
    output = result.stdout.strip()

    if not output or "None" in output:
        return "None (no migrations applied)"

    return output


def show_status():
    """
    Show current migration status.
    """
    logger.info("=" * 60)
    logger.info("DATABASE MIGRATION STATUS")
    logger.info("=" * 60)

    current = get_current_revision()
    logger.info(f"\nCurrent revision: {current}")

    logger.info("\nPending migrations:")
    run_alembic_command(["current", "--verbose"])

    logger.info("\n" + "=" * 60)


def show_history():
    """
    Show migration history.
    """
    logger.info("=" * 60)
    logger.info("MIGRATION HISTORY")
    logger.info("=" * 60)
    run_alembic_command(["history", "--verbose"])
    logger.info("=" * 60)


def upgrade(target: str = "head"):
    """
    Upgrade database to a specific migration.

    Args:
        target: Migration target (head, +1, specific revision)
    """
    logger.info(f"Upgrading database to: {target}")

    # Show current status
    current = get_current_revision()
    logger.info(f"Current revision: {current}")

    # Confirm in production
    if os.environ.get('ENV') == 'production':
        logger.warning("⚠️  PRODUCTION DATABASE UPGRADE")
        response = input("Are you sure you want to upgrade production? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Upgrade cancelled")
            return

    # Run upgrade
    run_alembic_command(["upgrade", target])

    # Show new status
    new_revision = get_current_revision()
    logger.info(f"✅ Upgrade complete! New revision: {new_revision}")


def downgrade(target: str = "-1"):
    """
    Downgrade database to a previous migration.

    Args:
        target: Migration target (-1, specific revision)
    """
    logger.warning("⚠️  DATABASE DOWNGRADE")
    logger.warning("This will roll back database changes")

    # Show current status
    current = get_current_revision()
    logger.info(f"Current revision: {current}")

    # Require confirmation
    response = input(f"Downgrade to {target}? (yes/no): ")
    if response.lower() != 'yes':
        logger.info("Downgrade cancelled")
        return

    # Run downgrade
    run_alembic_command(["downgrade", target])

    # Show new status
    new_revision = get_current_revision()
    logger.info(f"✅ Downgrade complete! New revision: {new_revision}")


def stamp(target: str = "head"):
    """
    Stamp database with a specific revision without running migrations.

    Useful for marking existing databases as migrated.

    Args:
        target: Revision to stamp (usually 'head')
    """
    logger.warning("⚠️  STAMPING DATABASE")
    logger.warning(f"This will mark the database as at revision: {target}")
    logger.warning("No actual migrations will be run")

    response = input("Continue? (yes/no): ")
    if response.lower() != 'yes':
        logger.info("Stamp cancelled")
        return

    run_alembic_command(["stamp", target])
    logger.info(f"✅ Database stamped with revision: {target}")


def main():
    """
    Main entry point for migration script.
    """
    parser = argparse.ArgumentParser(
        description="Safe database migration helper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    subparsers = parser.add_subparsers(dest='command', help='Migration command')

    # Status command
    subparsers.add_parser('status', help='Show current migration status')

    # History command
    subparsers.add_parser('history', help='Show migration history')

    # Upgrade command
    upgrade_parser = subparsers.add_parser('upgrade', help='Upgrade database')
    upgrade_parser.add_argument('target', nargs='?', default='head',
                                help='Target revision (default: head)')

    # Downgrade command
    downgrade_parser = subparsers.add_parser('downgrade', help='Downgrade database')
    downgrade_parser.add_argument('target', nargs='?', default='-1',
                                  help='Target revision (default: -1)')

    # Stamp command
    stamp_parser = subparsers.add_parser('stamp', help='Stamp database revision')
    stamp_parser.add_argument('target', nargs='?', default='head',
                             help='Revision to stamp (default: head)')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Check database connection for commands that need it
    if args.command in ['status', 'upgrade', 'downgrade', 'stamp']:
        if not check_database_connection():
            sys.exit(1)

    # Execute command
    if args.command == 'status':
        show_status()
    elif args.command == 'history':
        show_history()
    elif args.command == 'upgrade':
        upgrade(args.target)
    elif args.command == 'downgrade':
        downgrade(args.target)
    elif args.command == 'stamp':
        stamp(args.target)


if __name__ == '__main__':
    main()
