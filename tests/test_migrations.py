# Description: Tests for database migration system using Alembic
# Description: Verifies migration up/down functionality and safety checks

import pytest
import subprocess
import os
from pathlib import Path
from alembic.config import Config
from alembic import command
from alembic.script import ScriptDirectory
import sqlalchemy as sa


@pytest.fixture(scope="module")
def alembic_config():
    """
    Alembic configuration object.

    Returns Config object for running migrations in tests.
    """
    project_root = Path(__file__).parent.parent
    config_path = project_root / "alembic.ini"

    config = Config(str(config_path))
    return config


@pytest.fixture(scope="function")
def clean_alembic_version(db_connection):
    """
    Clean alembic_version table before and after test.

    Ensures each test starts with a clean migration state.
    """
    with db_connection.cursor() as cur:
        # Drop alembic_version if it exists
        cur.execute("DROP TABLE IF EXISTS alembic_version")
        db_connection.commit()

    yield

    # Cleanup after test
    # Rollback any failed transaction before cleanup
    db_connection.rollback()
    with db_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS alembic_version")
        db_connection.commit()


def test_alembic_config_exists():
    """Verify alembic.ini configuration file exists."""
    project_root = Path(__file__).parent.parent
    config_path = project_root / "alembic.ini"

    assert config_path.exists(), "alembic.ini should exist"


def test_alembic_env_exists():
    """Verify alembic/env.py exists and is configured."""
    project_root = Path(__file__).parent.parent
    env_path = project_root / "alembic" / "env.py"

    assert env_path.exists(), "alembic/env.py should exist"

    # Verify it contains our custom Azure AD logic
    content = env_path.read_text()
    assert "get_azure_token" in content, "env.py should have get_azure_token function"
    assert "get_database_url" in content, "env.py should have get_database_url function"


def test_alembic_versions_directory():
    """Verify alembic versions directory exists with migrations."""
    project_root = Path(__file__).parent.parent
    versions_dir = project_root / "alembic" / "versions"

    assert versions_dir.exists(), "alembic/versions directory should exist"

    # Check for initial migration
    migrations = list(versions_dir.glob("*.py"))
    assert len(migrations) > 0, "Should have at least one migration"

    # Verify initial migration exists
    initial_migration = [m for m in migrations if "initial_schema_baseline" in m.name]
    assert len(initial_migration) == 1, "Should have initial_schema_baseline migration"


def test_initial_migration_content():
    """Verify initial migration has proper content."""
    project_root = Path(__file__).parent.parent
    versions_dir = project_root / "alembic" / "versions"

    migrations = list(versions_dir.glob("*initial_schema_baseline.py"))
    assert len(migrations) == 1

    content = migrations[0].read_text()

    # Verify it has upgrade/downgrade functions
    assert "def upgrade()" in content
    assert "def downgrade()" in content

    # Verify it mentions lm_metrics
    assert "lm_metrics" in content

    # Verify it has safety checks
    assert "inspector" in content or "existing" in content.lower()


def test_migration_upgrade_creates_table(db_connection, clean_alembic_version, alembic_config):
    """Test that migration upgrade creates lm_metrics table if it doesn't exist."""
    # Drop all migration tables if they exist
    with db_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS processing_status CASCADE")
        cur.execute("DROP TABLE IF EXISTS metric_data CASCADE")
        cur.execute("DROP TABLE IF EXISTS metric_definitions CASCADE")
        cur.execute("DROP TABLE IF EXISTS datasources CASCADE")
        cur.execute("DROP TABLE IF EXISTS resources CASCADE")
        cur.execute("DROP TABLE IF EXISTS lm_metrics CASCADE")
        db_connection.commit()

    # Run migration upgrade
    command.upgrade(alembic_config, "head")

    # Verify lm_metrics table now exists
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'lm_metrics'
            )
        """)
        exists = cur.fetchone()[0]
        assert exists is True, "Migration should create lm_metrics table"

    # Verify table structure
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'lm_metrics'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()

        column_names = [col[0] for col in columns]
        assert 'id' in column_names
        assert 'payload' in column_names
        assert 'ingested_at' in column_names


def test_migration_upgrade_preserves_existing_table(db_connection, clean_alembic_version, alembic_config):
    """Test that migration upgrade preserves existing lm_metrics table."""
    # Drop all migration tables first
    with db_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS processing_status CASCADE")
        cur.execute("DROP TABLE IF EXISTS metric_data CASCADE")
        cur.execute("DROP TABLE IF EXISTS metric_definitions CASCADE")
        cur.execute("DROP TABLE IF EXISTS datasources CASCADE")
        cur.execute("DROP TABLE IF EXISTS resources CASCADE")
        cur.execute("DROP TABLE IF EXISTS lm_metrics CASCADE")
        db_connection.commit()

    # Create lm_metrics table with test data
    with db_connection.cursor() as cur:
        cur.execute("""
            CREATE TABLE lm_metrics (
                id SERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                ingested_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            INSERT INTO lm_metrics (payload)
            VALUES ('{"test": "data"}')
            RETURNING id
        """)
        test_id = cur.fetchone()[0]
        db_connection.commit()

    # Run migration upgrade
    command.upgrade(alembic_config, "head")

    # Verify table still exists with data
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lm_metrics")
        count = cur.fetchone()[0]
        assert count == 1, "Migration should preserve existing data"

        cur.execute("SELECT id, payload FROM lm_metrics WHERE id = %s", (test_id,))
        row = cur.fetchone()
        assert row is not None, "Original data should still exist"
        assert row[1]['test'] == 'data', "Payload should be unchanged"


def test_migration_downgrade_preserves_data(db_connection, clean_alembic_version, alembic_config):
    """Test that migration downgrade does NOT drop lm_metrics table."""
    # Drop all migration tables first
    with db_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS processing_status CASCADE")
        cur.execute("DROP TABLE IF EXISTS metric_data CASCADE")
        cur.execute("DROP TABLE IF EXISTS metric_definitions CASCADE")
        cur.execute("DROP TABLE IF EXISTS datasources CASCADE")
        cur.execute("DROP TABLE IF EXISTS resources CASCADE")
        cur.execute("DROP TABLE IF EXISTS lm_metrics CASCADE")
        db_connection.commit()

    # Run migration upgrade
    command.upgrade(alembic_config, "head")

    # Insert test data
    with db_connection.cursor() as cur:
        cur.execute("""
            INSERT INTO lm_metrics (payload)
            VALUES ('{"test": "downgrade"}')
            RETURNING id
        """)
        test_id = cur.fetchone()[0]
        db_connection.commit()

    # Run migration downgrade
    command.downgrade(alembic_config, "base")

    # Verify lm_metrics table STILL exists (safety feature)
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'lm_metrics'
            )
        """)
        exists = cur.fetchone()[0]
        assert exists is True, "Downgrade should NOT drop lm_metrics table"

        # Verify data is preserved
        cur.execute("SELECT COUNT(*) FROM lm_metrics")
        count = cur.fetchone()[0]
        assert count >= 1, "Data should be preserved after downgrade"


def test_migration_history(alembic_config):
    """Test that migration history can be retrieved."""
    script = ScriptDirectory.from_config(alembic_config)
    revisions = list(script.walk_revisions())

    assert len(revisions) > 0, "Should have at least one migration in history"

    # Verify initial migration is in history
    initial = [r for r in revisions if "initial" in (r.doc or "").lower()]
    assert len(initial) > 0, "Initial migration should be in history"


def test_migration_current_shows_none_before_upgrade(db_connection, clean_alembic_version):
    """Test that current revision is None before any migrations run."""
    # Check alembic_version table doesn't exist
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'alembic_version'
            )
        """)
        exists = cur.fetchone()[0]

        assert exists is False, "alembic_version table should not exist before upgrade"


def test_migration_current_shows_head_after_upgrade(db_connection, clean_alembic_version, alembic_config):
    """Test that current revision is set after upgrade."""
    # Drop all migration tables first
    with db_connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS processing_status CASCADE")
        cur.execute("DROP TABLE IF EXISTS metric_data CASCADE")
        cur.execute("DROP TABLE IF EXISTS metric_definitions CASCADE")
        cur.execute("DROP TABLE IF EXISTS datasources CASCADE")
        cur.execute("DROP TABLE IF EXISTS resources CASCADE")
        cur.execute("DROP TABLE IF EXISTS lm_metrics CASCADE")
        db_connection.commit()

    # Run upgrade
    command.upgrade(alembic_config, "head")

    # Check alembic_version table exists and has a version
    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'alembic_version'
            )
        """)
        exists = cur.fetchone()[0]
        assert exists is True, "alembic_version table should exist after upgrade"

        # Get current version from table
        cur.execute("SELECT version_num FROM alembic_version")
        current = cur.fetchone()[0]

        assert current is not None, "Should have current revision after upgrade"

        # Verify it matches expected revision
        script = ScriptDirectory.from_config(alembic_config)
        head = script.get_current_head()
        assert current == head, "Current revision should match head"


def test_migrate_script_exists():
    """Verify migration helper script exists and is executable."""
    project_root = Path(__file__).parent.parent
    script_path = project_root / "scripts" / "migrate.py"

    assert script_path.exists(), "scripts/migrate.py should exist"

    # Check if executable
    assert os.access(script_path, os.X_OK), "migrate.py should be executable"


def test_migrate_script_shows_help():
    """Test that migration script shows help."""
    project_root = Path(__file__).parent.parent
    script_path = project_root / "scripts" / "migrate.py"

    result = subprocess.run(
        ["python", str(script_path), "--help"],
        capture_output=True,
        text=True
    )

    assert result.returncode == 0
    assert "migration" in result.stdout.lower()
    assert "status" in result.stdout
    assert "upgrade" in result.stdout
    assert "downgrade" in result.stdout
