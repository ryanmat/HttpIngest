# ABOUTME: Alembic environment configuration for LogicMonitor data pipeline
# ABOUTME: Handles PostgreSQL connection with Azure AD token authentication

from logging.config import fileConfig
import os
import subprocess
import logging

from sqlalchemy import engine_from_config, create_engine
from sqlalchemy import pool

from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

logger = logging.getLogger('alembic.env')

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None


def get_azure_token():
    """
    Get Azure AD access token for PostgreSQL authentication.

    Returns None if token acquisition fails.
    """
    password = os.environ.get('PGPASSWORD', '')

    if password:
        logger.info("Using PGPASSWORD from environment")
        return password

    try:
        logger.info("Acquiring Azure AD token via az CLI")
        result = subprocess.run(
            ["az", "account", "get-access-token",
             "--resource", "https://ossrdbms-aad.database.windows.net",
             "--query", "accessToken", "--output", "tsv"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            token = result.stdout.strip()
            logger.info("Successfully acquired Azure AD token")
            return token
        else:
            logger.error(f"Failed to get Azure AD token: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"Token acquisition failed: {e}")
        return None


def get_database_url():
    """
    Construct database URL from environment variables and Azure AD token.

    Falls back to sqlalchemy.url from alembic.ini if environment not configured.
    """
    host = os.environ.get('PGHOST', 'rm-postgres.postgres.database.azure.com')
    database = os.environ.get('PGDATABASE', 'postgres')
    user = os.environ.get('PGUSER', 'ryan.matuszewski@logicmonitor.com')
    port = os.environ.get('PGPORT', '5432')

    # Get Azure AD token
    password = get_azure_token()

    if not password:
        logger.warning("No Azure AD token available, checking alembic.ini")
        url = config.get_main_option("sqlalchemy.url")
        if url:
            return url
        raise RuntimeError(
            "No database credentials available. "
            "Set PGPASSWORD environment variable or ensure Azure CLI is authenticated."
        )

    # Construct PostgreSQL URL with Azure AD token
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require"
    logger.info(f"Connecting to {host}:{port}/{database} as {user}")
    return url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Get database URL dynamically
    url = get_database_url()

    # Create engine with the URL
    connectable = create_engine(
        url,
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,  # Detect column type changes
            compare_server_default=True,  # Detect default value changes
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
