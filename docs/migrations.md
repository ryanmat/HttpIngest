# Database Migrations

Database migration system for the LogicMonitor Data Pipeline using Alembic.

## Overview

This project uses Alembic for database schema migrations with the following features:

- **Azure AD Authentication**: Automatic token acquisition for PostgreSQL
- **Safe Migrations**: Preserves production data with rollback capability
- **Environment Flexibility**: Works locally and in Azure Container Apps
- **Test Coverage**: Full test suite for migration up/down functionality

## Quick Start

### Check Migration Status

```bash
# Using helper script (recommended)
uv run python scripts/migrate.py status

# Using alembic directly
uv run alembic current
```

### View Migration History

```bash
uv run python scripts/migrate.py history
```

### Run Migrations

```bash
# Upgrade to latest
uv run python scripts/migrate.py upgrade

# Upgrade one step
uv run python scripts/migrate.py upgrade +1

# Downgrade one step (with confirmation)
uv run python scripts/migrate.py downgrade -1
```

## Initial Setup

### For Existing Production Database

The production database already has the `lm_metrics` table with data. To set up migrations:

```bash
# Stamp the database as being at the baseline migration
uv run python scripts/migrate.py stamp head
```

This marks the database as migrated without actually running migrations.

### For New/Test Databases

For a fresh database, simply run:

```bash
uv run python scripts/migrate.py upgrade
```

This will create the `lm_metrics` table and set up the migration tracking.

## Creating New Migrations

### Generate a New Migration

```bash
# Create a new migration file
uv run alembic revision -m "description_of_change"

# Or with autogenerate (requires SQLAlchemy models)
uv run alembic revision --autogenerate -m "description_of_change"
```

This creates a timestamped migration file in `alembic/versions/`.

### Edit the Migration

Edit the generated file in `alembic/versions/`:

```python
def upgrade() -> None:
    """Upgrade schema."""
    # Add your upgrade logic here
    op.create_table(
        'new_table',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(50), nullable=False),
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Add your downgrade logic here
    op.drop_table('new_table')
```

### Test the Migration

```bash
# Run tests
uv run pytest tests/test_migrations.py -v

# Test upgrade
uv run python scripts/migrate.py upgrade

# Test downgrade
uv run python scripts/migrate.py downgrade -1

# Re-upgrade
uv run python scripts/migrate.py upgrade
```

## Environment Configuration

### Environment Variables

The migration system uses these environment variables:

```bash
# Database connection
export PGHOST="rm-postgres.postgres.database.azure.com"
export PGDATABASE="postgres"
export PGUSER="ryan.matuszewski@logicmonitor.com"
export PGPORT="5432"

# Optional: Pre-set password/token (otherwise acquired via az CLI)
export PGPASSWORD="your_azure_ad_token"
```

### Azure AD Authentication

The system automatically acquires Azure AD tokens using:

```bash
az account get-access-token --resource https://ossrdbms-aad.database.windows.net
```

Ensure you're logged in to Azure CLI:

```bash
az login
az account show
```

## Migration Files

### Structure

```
HttpIngest/
├── alembic/
│   ├── versions/           # Migration scripts
│   │   └── YYYYMMDD_HHMM-{hash}_{description}.py
│   ├── env.py             # Alembic environment config
│   ├── script.py.mako     # Migration template
│   └── README             # Alembic readme
├── alembic.ini            # Alembic configuration
├── scripts/
│   └── migrate.py         # Migration helper script
└── tests/
    └── test_migrations.py # Migration tests
```

### Initial Migration

The initial migration (`initial_schema_baseline`) is special:

- **Preserves Existing Data**: Does NOT recreate `lm_metrics` if it exists
- **Safe for Production**: Can be run on live database without data loss
- **Idempotent**: Can be run multiple times safely
- **No Downgrade**: Downgrade does NOT drop tables (safety feature)

## Safety Features

### Production Warnings

The migration script warns before:

- Upgrading production databases
- Downgrading any database
- Stamping revisions

### Rollback Protection

The initial migration's downgrade does NOT drop `lm_metrics`:

```python
def downgrade() -> None:
    # Do not drop lm_metrics - it contains production data
    pass
```

To drop tables, you must do so manually with explicit confirmation.

### Database Connection Testing

The helper script verifies database connectivity before running migrations:

```bash
uv run python scripts/migrate.py status
# INFO [migrate] Checking database connection...
# INFO [migrate] Database connection verified
```

## Azure Container App Deployment

### In Container App Environment

The migration system works in Azure Container Apps by:

1. Using environment variables for database config
2. Acquiring Azure AD tokens via managed identity
3. Running migrations during deployment

### Example Deployment Flow

```bash
# In Azure Container App startup
cd /app
python scripts/migrate.py upgrade
```

### Container App Environment Variables

Set these in your Container App configuration:

```yaml
env:
  - name: PGHOST
    value: "rm-postgres.postgres.database.azure.com"
  - name: PGDATABASE
    value: "postgres"
  - name: PGUSER
    value: "your-user@logicmonitor.com"
  - name: PGPASSWORD
    secretRef: postgres-token
```

## Common Operations

### Check What Will Be Migrated

```bash
# Show pending migrations
uv run alembic current
uv run alembic history
```

### Upgrade to Specific Revision

```bash
# Upgrade to specific revision
uv run alembic upgrade abc123

# Or using helper
uv run python scripts/migrate.py upgrade abc123
```

### Rollback Last Migration

```bash
uv run python scripts/migrate.py downgrade -1
```

### View SQL Without Executing

```bash
# Generate SQL for upgrade
uv run alembic upgrade head --sql

# Generate SQL for downgrade
uv run alembic downgrade -1 --sql
```

## Testing

### Run Migration Tests

```bash
# All migration tests
uv run pytest tests/test_migrations.py -v

# Specific test
uv run pytest tests/test_migrations.py::test_migration_upgrade_creates_table -v
```

### Test Coverage

The test suite verifies:

-  Configuration files exist
-  Migration files are valid
-  Upgrade creates tables correctly
-  Upgrade preserves existing data
-  Downgrade preserves data (safety)
-  Migration history tracking
-  Helper script functionality

## Troubleshooting

### "No Azure AD token available"

Ensure you're logged into Azure CLI:

```bash
az login
az account show
```

Or set `PGPASSWORD` environment variable.

### "Connection refused"

Check:
- VPN connection (if required)
- Firewall rules
- Database server is running
- Correct host/port in environment variables

### "alembic_version table already exists"

The database has been migrated. Check current revision:

```bash
uv run alembic current
```

### "Migration fails with schema error"

Check:
- Database has correct permissions
- Migration SQL is valid
- No conflicting tables/columns exist

## Best Practices

1. **Always Test Migrations**
   - Test on development database first
   - Run full test suite
   - Verify upgrade AND downgrade

2. **Write Reversible Migrations**
   - Implement both upgrade() and downgrade()
   - Test rollback functionality
   - Document any irreversible changes

3. **Use Transactions**
   - Alembic uses transactions by default
   - Failed migrations rollback automatically
   - Test transaction behavior

4. **Document Changes**
   - Add clear docstrings to migrations
   - Note any data transformations
   - Document safety considerations

5. **Preserve Data**
   - Never drop tables in downgrade
   - Use backups before major migrations
   - Test data preservation

## Additional Resources

- [Alembic Documentation](https://alembic.sqlalchemy.org/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Azure Database for PostgreSQL](https://docs.microsoft.com/en-us/azure/postgresql/)
