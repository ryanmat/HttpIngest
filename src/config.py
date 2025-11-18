# Description: Environment-specific configuration management for LogicMonitor Data Pipeline
# Description: Supports dev, staging, and production environments with validation

import os
from typing import Optional, Literal
from dataclasses import dataclass
from enum import Enum


class Environment(str, Enum):
    """Supported deployment environments."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str
    port: int
    database: str
    user: str
    password: Optional[str] = None
    use_azure_ad: bool = False
    ssl_mode: str = "require"
    pool_size: int = 10
    max_overflow: int = 20

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        if self.password:
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode={self.ssl_mode}"
        else:
            return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}?sslmode={self.ssl_mode}"


@dataclass
class RedisConfig:
    """Redis configuration."""
    url: str
    use_redis: bool = True
    max_connections: int = 50
    socket_timeout: int = 5
    socket_connect_timeout: int = 5

    @property
    def is_enabled(self) -> bool:
        """Check if Redis is enabled."""
        return self.use_redis and bool(self.url)


@dataclass
class StreamingConfig:
    """Real-time streaming configuration."""
    max_websocket_connections: int = 100
    rate_limit_messages_per_second: int = 10
    rate_limit_burst_size: int = 20
    message_buffer_size: int = 1000
    client_state_retention_hours: int = 24


@dataclass
class BackgroundTasksConfig:
    """Background task intervals."""
    data_processing_interval: int = 30
    metric_publishing_interval: int = 10
    health_monitoring_interval: int = 60


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_sql_queries: bool = False


@dataclass
class ApplicationInsightsConfig:
    """Application Insights configuration."""
    connection_string: Optional[str] = None
    instrumentation_key: Optional[str] = None
    enabled: bool = False

    @property
    def is_enabled(self) -> bool:
        """Check if Application Insights is enabled."""
        return self.enabled and (bool(self.connection_string) or bool(self.instrumentation_key))


@dataclass
class SecurityConfig:
    """Security configuration."""
    api_key_header: str = "X-API-Key"
    api_keys: list[str] = None
    cors_origins: list[str] = None
    require_https: bool = True
    webhook_secret: Optional[str] = None

    def __post_init__(self):
        if self.api_keys is None:
            self.api_keys = []
        if self.cors_origins is None:
            self.cors_origins = ["*"]


@dataclass
class AppConfig:
    """Complete application configuration."""
    environment: Environment
    database: DatabaseConfig
    redis: RedisConfig
    streaming: StreamingConfig
    background_tasks: BackgroundTasksConfig
    logging: LoggingConfig
    app_insights: ApplicationInsightsConfig
    security: SecurityConfig

    # Application metadata
    app_name: str = "LogicMonitor Data Pipeline"
    version: str = "12.0.0"
    debug: bool = False


def get_env(key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """
    Get environment variable with validation.

    Args:
        key: Environment variable name
        default: Default value if not found
        required: Raise error if not found and no default

    Returns:
        Environment variable value or default

    Raises:
        ValueError: If required variable is missing
    """
    value = os.getenv(key, default)
    if required and value is None:
        raise ValueError(f"Required environment variable '{key}' is not set")
    return value


def get_env_bool(key: str, default: bool = False) -> bool:
    """Get boolean environment variable."""
    value = os.getenv(key, str(default)).lower()
    return value in ("true", "1", "yes", "on")


def get_env_int(key: str, default: int) -> int:
    """Get integer environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def get_env_list(key: str, default: Optional[list] = None, separator: str = ",") -> list:
    """Get list from environment variable."""
    value = os.getenv(key)
    if value is None:
        return default or []
    return [item.strip() for item in value.split(separator) if item.strip()]


def load_config() -> AppConfig:
    """
    Load configuration from environment variables.

    Returns:
        AppConfig instance

    Environment Variables:
        # Environment
        ENVIRONMENT: development|staging|production (default: development)

        # Database
        POSTGRES_HOST: PostgreSQL host
        POSTGRES_PORT: PostgreSQL port (default: 5432)
        POSTGRES_DB: Database name
        POSTGRES_USER: Database user
        POSTGRES_PASSWORD: Database password (optional if using Azure AD)
        USE_AZURE_AD_AUTH: Use Azure AD authentication (default: false)

        # Redis
        REDIS_URL: Redis connection URL
        USE_REDIS: Enable Redis (default: true)

        # Streaming
        MAX_WEBSOCKET_CONNECTIONS: Max WebSocket connections (default: 100)
        RATE_LIMIT_MESSAGES_PER_SECOND: Rate limit per client (default: 10)
        RATE_LIMIT_BURST_SIZE: Burst size (default: 20)
        MESSAGE_BUFFER_SIZE: Buffer size per client (default: 1000)

        # Background Tasks
        DATA_PROCESSING_INTERVAL: Seconds between processing (default: 30)
        METRIC_PUBLISHING_INTERVAL: Seconds between publishing (default: 10)
        HEALTH_MONITORING_INTERVAL: Seconds between health checks (default: 60)

        # Logging
        LOG_LEVEL: Logging level (default: INFO)
        LOG_SQL_QUERIES: Log SQL queries (default: false)

        # Application Insights
        APPLICATIONINSIGHTS_CONNECTION_STRING: App Insights connection string
        APPINSIGHTS_INSTRUMENTATION_KEY: App Insights instrumentation key
        ENABLE_APP_INSIGHTS: Enable Application Insights (default: false)

        # Security
        API_KEYS: Comma-separated API keys
        CORS_ORIGINS: Comma-separated CORS origins (default: *)
        REQUIRE_HTTPS: Require HTTPS (default: true)
        WEBHOOK_SECRET: Secret for webhook signatures
    """
    # Determine environment
    env_str = get_env("ENVIRONMENT", "development").lower()
    try:
        environment = Environment(env_str)
    except ValueError:
        environment = Environment.DEVELOPMENT

    # Database configuration
    database = DatabaseConfig(
        host=get_env("POSTGRES_HOST", required=True),
        port=get_env_int("POSTGRES_PORT", 5432),
        database=get_env("POSTGRES_DB", required=True),
        user=get_env("POSTGRES_USER", required=True),
        password=get_env("POSTGRES_PASSWORD"),
        use_azure_ad=get_env_bool("USE_AZURE_AD_AUTH", False),
        ssl_mode=get_env("POSTGRES_SSL_MODE", "require"),
        pool_size=get_env_int("DB_POOL_SIZE", 10),
        max_overflow=get_env_int("DB_MAX_OVERFLOW", 20)
    )

    # Redis configuration
    redis = RedisConfig(
        url=get_env("REDIS_URL", "redis://localhost:6379"),
        use_redis=get_env_bool("USE_REDIS", True),
        max_connections=get_env_int("REDIS_MAX_CONNECTIONS", 50)
    )

    # Streaming configuration
    streaming = StreamingConfig(
        max_websocket_connections=get_env_int("MAX_WEBSOCKET_CONNECTIONS", 100),
        rate_limit_messages_per_second=get_env_int("RATE_LIMIT_MESSAGES_PER_SECOND", 10),
        rate_limit_burst_size=get_env_int("RATE_LIMIT_BURST_SIZE", 20),
        message_buffer_size=get_env_int("MESSAGE_BUFFER_SIZE", 1000),
        client_state_retention_hours=get_env_int("CLIENT_STATE_RETENTION_HOURS", 24)
    )

    # Background tasks configuration
    background_tasks = BackgroundTasksConfig(
        data_processing_interval=get_env_int("DATA_PROCESSING_INTERVAL", 30),
        metric_publishing_interval=get_env_int("METRIC_PUBLISHING_INTERVAL", 10),
        health_monitoring_interval=get_env_int("HEALTH_MONITORING_INTERVAL", 60)
    )

    # Logging configuration
    logging_config = LoggingConfig(
        level=get_env("LOG_LEVEL", "INFO").upper(),
        log_sql_queries=get_env_bool("LOG_SQL_QUERIES", False)
    )

    # Application Insights configuration
    app_insights = ApplicationInsightsConfig(
        connection_string=get_env("APPLICATIONINSIGHTS_CONNECTION_STRING"),
        instrumentation_key=get_env("APPINSIGHTS_INSTRUMENTATION_KEY"),
        enabled=get_env_bool("ENABLE_APP_INSIGHTS", False)
    )

    # Security configuration
    security = SecurityConfig(
        api_keys=get_env_list("API_KEYS", default=[]),
        cors_origins=get_env_list("CORS_ORIGINS", default=["*"]),
        require_https=get_env_bool("REQUIRE_HTTPS", True),
        webhook_secret=get_env("WEBHOOK_SECRET")
    )

    # Environment-specific overrides
    if environment == Environment.PRODUCTION:
        # Production defaults
        security.require_https = True
        if not app_insights.enabled:
            print("WARNING: Application Insights not enabled in production")
    elif environment == Environment.DEVELOPMENT:
        # Development defaults
        security.require_https = False
        logging_config.log_sql_queries = True
        database.pool_size = 5

    return AppConfig(
        environment=environment,
        database=database,
        redis=redis,
        streaming=streaming,
        background_tasks=background_tasks,
        logging=logging_config,
        app_insights=app_insights,
        security=security,
        version=get_env("APP_VERSION", "12.0.0"),
        debug=get_env_bool("DEBUG", environment == Environment.DEVELOPMENT)
    )


def validate_config(config: AppConfig) -> list[str]:
    """
    Validate configuration and return list of warnings/errors.

    Args:
        config: Application configuration

    Returns:
        List of validation messages
    """
    issues = []

    # Production checks
    if config.environment == Environment.PRODUCTION:
        if not config.security.require_https:
            issues.append("WARNING: HTTPS not required in production")

        if "*" in config.security.cors_origins:
            issues.append("WARNING: CORS allows all origins in production")

        if not config.security.api_keys:
            issues.append("WARNING: No API keys configured in production")

        if not config.app_insights.is_enabled:
            issues.append("WARNING: Application Insights not enabled in production")

        if config.logging.log_sql_queries:
            issues.append("WARNING: SQL query logging enabled in production (performance impact)")

    # Redis checks
    if config.streaming.max_websocket_connections > 100 and not config.redis.is_enabled:
        issues.append("WARNING: High WebSocket limit without Redis may cause issues with multiple replicas")

    # Database checks
    if config.database.use_azure_ad and config.database.password:
        issues.append("WARNING: Both Azure AD and password configured (Azure AD will be used)")

    return issues


# Global configuration instance
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get or create global configuration instance."""
    global _config
    if _config is None:
        _config = load_config()

        # Validate and log issues
        issues = validate_config(_config)
        if issues:
            for issue in issues:
                print(f"CONFIG: {issue}")

    return _config
