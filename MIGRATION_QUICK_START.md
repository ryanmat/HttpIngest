# Migration Quick Start Guide

## ⚠️ CRITICAL - First Time Setup

**For the existing production database with 1191+ records:**

```bash
# Mark the database as migrated WITHOUT running migrations
uv run python scripts/migrate.py stamp head
```

This tells Alembic that the database is already at the baseline version.

## Common Commands

### Check Status
```bash
uv run python scripts/migrate.py status
```

### View History
```bash
uv run python scripts/migrate.py history
```

### Upgrade Database
```bash
# Upgrade to latest
uv run python scripts/migrate.py upgrade

# Upgrade one migration
uv run python scripts/migrate.py upgrade +1
```

### Rollback (Use with Caution)
```bash
# Downgrade one migration (asks for confirmation)
uv run python scripts/migrate.py downgrade -1
```

## Create New Migration

```bash
# Create new migration file
uv run alembic revision -m "add_normalized_metrics_table"

# Edit the file in alembic/versions/
# Implement upgrade() and downgrade() functions

# Test it
uv run pytest tests/test_migrations.py -v
```

## Safety Features

✅ **Preserves Production Data**: Downgrade does NOT drop tables
✅ **Azure AD Authentication**: Auto-acquires tokens via az CLI
✅ **Transaction Safety**: Failed migrations auto-rollback
✅ **Production Warnings**: Confirms before modifying production

## Environment Setup

```bash
# Required for database access
az login

# Optional: Set explicitly
export PGHOST="rm-postgres.postgres.database.azure.com"
export PGDATABASE="postgres"
export PGUSER="ryan.matuszewski@logicmonitor.com"
```

## Testing

```bash
# Run all migration tests
uv run pytest tests/test_migrations.py -v

# Run all tests
uv run pytest tests/ -v
```

## Get Help

```bash
uv run python scripts/migrate.py --help
```

## Full Documentation

See [docs/migrations.md](docs/migrations.md) for complete documentation.
