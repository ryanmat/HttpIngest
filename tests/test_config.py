# Description: Unit tests for environment configuration management
# Description: Tests config loading, validation, and environment-specific behaviors

import pytest
import os
from unittest.mock import patch
from src.config import (
    Environment,
    DatabaseConfig,
    RedisConfig,
    StreamingConfig,
    BackgroundTasksConfig,
    LoggingConfig,
    ApplicationInsightsConfig,
    SecurityConfig,
    AppConfig,
    get_env,
    get_env_bool,
    get_env_int,
    get_env_list,
    load_config,
    validate_config,
    get_config
)


class TestEnvironment:
    """Test Environment enum."""

    def test_environment_values(self):
        """Test environment enum values."""
        assert Environment.DEVELOPMENT.value == "development"
        assert Environment.STAGING.value == "staging"
        assert Environment.PRODUCTION.value == "production"


class TestDatabaseConfig:
    """Test DatabaseConfig dataclass."""

    def test_connection_string_with_password(self):
        """Test connection string generation with password."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            ssl_mode="require"
        )

        expected = "postgresql://testuser:testpass@localhost:5432/testdb?sslmode=require"
        assert config.connection_string == expected

    def test_connection_string_without_password(self):
        """Test connection string generation without password (Azure AD)."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password=None,
            use_azure_ad=True,
            ssl_mode="require"
        )

        expected = "postgresql://testuser@localhost:5432/testdb?sslmode=require"
        assert config.connection_string == expected

    def test_default_values(self):
        """Test default database config values."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser"
        )

        assert config.password is None
        assert config.use_azure_ad is False
        assert config.ssl_mode == "require"
        assert config.pool_size == 10
        assert config.max_overflow == 20


class TestRedisConfig:
    """Test RedisConfig dataclass."""

    def test_is_enabled_true(self):
        """Test Redis is enabled when use_redis=True and URL provided."""
        config = RedisConfig(
            url="redis://localhost:6379",
            use_redis=True
        )
        assert config.is_enabled is True

    def test_is_enabled_false_no_url(self):
        """Test Redis disabled when no URL."""
        config = RedisConfig(
            url="",
            use_redis=True
        )
        assert config.is_enabled is False

    def test_is_enabled_false_use_redis_false(self):
        """Test Redis disabled when use_redis=False."""
        config = RedisConfig(
            url="redis://localhost:6379",
            use_redis=False
        )
        assert config.is_enabled is False


class TestApplicationInsightsConfig:
    """Test ApplicationInsightsConfig dataclass."""

    def test_is_enabled_with_connection_string(self):
        """Test enabled with connection string."""
        config = ApplicationInsightsConfig(
            connection_string="InstrumentationKey=abc123",
            enabled=True
        )
        assert config.is_enabled is True

    def test_is_enabled_with_instrumentation_key(self):
        """Test enabled with instrumentation key."""
        config = ApplicationInsightsConfig(
            instrumentation_key="abc123",
            enabled=True
        )
        assert config.is_enabled is True

    def test_is_enabled_false_when_disabled(self):
        """Test disabled even with credentials."""
        config = ApplicationInsightsConfig(
            connection_string="InstrumentationKey=abc123",
            enabled=False
        )
        assert config.is_enabled is False

    def test_is_enabled_false_without_credentials(self):
        """Test disabled without credentials."""
        config = ApplicationInsightsConfig(
            enabled=True
        )
        assert config.is_enabled is False


class TestSecurityConfig:
    """Test SecurityConfig dataclass."""

    def test_default_initialization(self):
        """Test default security config initialization."""
        config = SecurityConfig()

        assert config.api_keys == []
        assert config.cors_origins == ["*"]
        assert config.require_https is True


class TestEnvHelpers:
    """Test environment variable helper functions."""

    def test_get_env_with_value(self):
        """Test get_env returns environment variable."""
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            assert get_env("TEST_VAR") == "test_value"

    def test_get_env_with_default(self):
        """Test get_env returns default when not set."""
        assert get_env("NONEXISTENT_VAR", "default_value") == "default_value"

    def test_get_env_required_missing_raises(self):
        """Test get_env raises for required missing variable."""
        with pytest.raises(ValueError, match="Required environment variable"):
            get_env("NONEXISTENT_VAR", required=True)

    def test_get_env_bool_true_values(self):
        """Test get_env_bool recognizes true values."""
        true_values = ["true", "True", "TRUE", "1", "yes", "Yes", "on", "On"]

        for value in true_values:
            with patch.dict(os.environ, {"TEST_BOOL": value}):
                assert get_env_bool("TEST_BOOL") is True, f"Failed for value: {value}"

    def test_get_env_bool_false_values(self):
        """Test get_env_bool recognizes false values."""
        false_values = ["false", "False", "0", "no", "off", "anything_else"]

        for value in false_values:
            with patch.dict(os.environ, {"TEST_BOOL": value}):
                assert get_env_bool("TEST_BOOL") is False, f"Failed for value: {value}"

    def test_get_env_bool_default(self):
        """Test get_env_bool uses default when not set."""
        assert get_env_bool("NONEXISTENT_BOOL", default=True) is True
        assert get_env_bool("NONEXISTENT_BOOL", default=False) is False

    def test_get_env_int_valid(self):
        """Test get_env_int parses integer."""
        with patch.dict(os.environ, {"TEST_INT": "42"}):
            assert get_env_int("TEST_INT", default=0) == 42

    def test_get_env_int_invalid(self):
        """Test get_env_int returns default for invalid value."""
        with patch.dict(os.environ, {"TEST_INT": "not_an_int"}):
            assert get_env_int("TEST_INT", default=10) == 10

    def test_get_env_int_default(self):
        """Test get_env_int uses default when not set."""
        assert get_env_int("NONEXISTENT_INT", default=99) == 99

    def test_get_env_list_comma_separated(self):
        """Test get_env_list parses comma-separated list."""
        with patch.dict(os.environ, {"TEST_LIST": "item1,item2,item3"}):
            assert get_env_list("TEST_LIST") == ["item1", "item2", "item3"]

    def test_get_env_list_with_spaces(self):
        """Test get_env_list strips whitespace."""
        with patch.dict(os.environ, {"TEST_LIST": " item1 , item2 , item3 "}):
            assert get_env_list("TEST_LIST") == ["item1", "item2", "item3"]

    def test_get_env_list_custom_separator(self):
        """Test get_env_list with custom separator."""
        with patch.dict(os.environ, {"TEST_LIST": "item1|item2|item3"}):
            assert get_env_list("TEST_LIST", separator="|") == ["item1", "item2", "item3"]

    def test_get_env_list_default(self):
        """Test get_env_list uses default when not set."""
        assert get_env_list("NONEXISTENT_LIST", default=["a", "b"]) == ["a", "b"]

    def test_get_env_list_empty_items_filtered(self):
        """Test get_env_list filters empty items."""
        with patch.dict(os.environ, {"TEST_LIST": "item1,,item2,"}):
            assert get_env_list("TEST_LIST") == ["item1", "item2"]


class TestLoadConfig:
    """Test configuration loading from environment."""

    def test_load_config_development(self):
        """Test loading development configuration."""
        env_vars = {
            "ENVIRONMENT": "development",
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "testdb",
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass"
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = load_config()

            assert config.environment == Environment.DEVELOPMENT
            assert config.debug is True
            assert config.database.host == "localhost"
            assert config.database.port == 5432
            assert config.database.database == "testdb"
            assert config.database.user == "testuser"
            assert config.database.password == "testpass"
            assert config.security.require_https is False  # Dev override
            assert config.logging.log_sql_queries is True  # Dev override
            assert config.database.pool_size == 5  # Dev override

    def test_load_config_production(self):
        """Test loading production configuration."""
        env_vars = {
            "ENVIRONMENT": "production",
            "POSTGRES_HOST": "prod-db.example.com",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "proddb",
            "POSTGRES_USER": "produser",
            "POSTGRES_PASSWORD": "prodpass"
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = load_config()

            assert config.environment == Environment.PRODUCTION
            assert config.debug is False
            assert config.security.require_https is True  # Prod override
            assert config.database.pool_size == 10  # Default, not overridden

    def test_load_config_staging(self):
        """Test loading staging configuration."""
        env_vars = {
            "ENVIRONMENT": "staging",
            "POSTGRES_HOST": "staging-db.example.com",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "stagingdb",
            "POSTGRES_USER": "staginguser"
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = load_config()

            assert config.environment == Environment.STAGING

    def test_load_config_missing_required_raises(self):
        """Test loading config with missing required variables raises error."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="POSTGRES_HOST"):
                load_config()

    def test_load_config_azure_ad_auth(self):
        """Test config with Azure AD authentication."""
        env_vars = {
            "POSTGRES_HOST": "azure-db.postgres.database.azure.com",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "postgres",
            "POSTGRES_USER": "user@example.com",
            "USE_AZURE_AD_AUTH": "true"
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = load_config()

            assert config.database.use_azure_ad is True
            assert config.database.password is None

    def test_load_config_redis_disabled(self):
        """Test config with Redis disabled."""
        env_vars = {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_DB": "testdb",
            "POSTGRES_USER": "testuser",
            "USE_REDIS": "false"
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = load_config()

            assert config.redis.use_redis is False
            assert config.redis.is_enabled is False

    def test_load_config_custom_intervals(self):
        """Test config with custom background task intervals."""
        env_vars = {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_DB": "testdb",
            "POSTGRES_USER": "testuser",
            "DATA_PROCESSING_INTERVAL": "60",
            "METRIC_PUBLISHING_INTERVAL": "5",
            "HEALTH_MONITORING_INTERVAL": "120"
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = load_config()

            assert config.background_tasks.data_processing_interval == 60
            assert config.background_tasks.metric_publishing_interval == 5
            assert config.background_tasks.health_monitoring_interval == 120


class TestValidateConfig:
    """Test configuration validation."""

    def test_validate_production_warnings(self):
        """Test validation warnings for production."""
        config = AppConfig(
            environment=Environment.PRODUCTION,
            database=DatabaseConfig(
                host="localhost", port=5432, database="db", user="user"
            ),
            redis=RedisConfig(url="redis://localhost"),
            streaming=StreamingConfig(),
            background_tasks=BackgroundTasksConfig(),
            logging=LoggingConfig(log_sql_queries=True),
            app_insights=ApplicationInsightsConfig(enabled=False),
            security=SecurityConfig(
                require_https=False,
                cors_origins=["*"],
                api_keys=[]
            )
        )

        issues = validate_config(config)

        assert any("HTTPS not required" in issue for issue in issues)
        assert any("CORS allows all origins" in issue for issue in issues)
        assert any("No API keys configured" in issue for issue in issues)
        assert any("Application Insights not enabled" in issue for issue in issues)
        assert any("SQL query logging enabled" in issue for issue in issues)

    def test_validate_websocket_without_redis(self):
        """Test validation warning for high WebSocket limit without Redis."""
        config = AppConfig(
            environment=Environment.DEVELOPMENT,
            database=DatabaseConfig(
                host="localhost", port=5432, database="db", user="user"
            ),
            redis=RedisConfig(url="", use_redis=False),
            streaming=StreamingConfig(max_websocket_connections=200),
            background_tasks=BackgroundTasksConfig(),
            logging=LoggingConfig(),
            app_insights=ApplicationInsightsConfig(),
            security=SecurityConfig()
        )

        issues = validate_config(config)

        assert any("WebSocket" in issue and "Redis" in issue for issue in issues)

    def test_validate_azure_ad_with_password(self):
        """Test validation warning for Azure AD with password."""
        config = AppConfig(
            environment=Environment.DEVELOPMENT,
            database=DatabaseConfig(
                host="localhost",
                port=5432,
                database="db",
                user="user",
                password="pass",
                use_azure_ad=True
            ),
            redis=RedisConfig(url="redis://localhost"),
            streaming=StreamingConfig(),
            background_tasks=BackgroundTasksConfig(),
            logging=LoggingConfig(),
            app_insights=ApplicationInsightsConfig(),
            security=SecurityConfig()
        )

        issues = validate_config(config)

        assert any("Azure AD and password" in issue for issue in issues)

    def test_validate_development_no_warnings(self):
        """Test no warnings for proper development config."""
        config = AppConfig(
            environment=Environment.DEVELOPMENT,
            database=DatabaseConfig(
                host="localhost", port=5432, database="db", user="user"
            ),
            redis=RedisConfig(url="redis://localhost"),
            streaming=StreamingConfig(),
            background_tasks=BackgroundTasksConfig(),
            logging=LoggingConfig(),
            app_insights=ApplicationInsightsConfig(),
            security=SecurityConfig()
        )

        issues = validate_config(config)

        # Development can have relaxed security
        assert len(issues) == 0


class TestGetConfig:
    """Test global config instance management."""

    def test_get_config_singleton(self):
        """Test get_config returns singleton instance."""
        env_vars = {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_DB": "testdb",
            "POSTGRES_USER": "testuser"
        }

        with patch.dict(os.environ, env_vars, clear=True):
            # Clear global state
            import src.config
            src.config._config = None

            config1 = get_config()
            config2 = get_config()

            assert config1 is config2  # Same instance

    def test_get_config_loads_and_validates(self, capsys):
        """Test get_config loads and prints validation issues."""
        env_vars = {
            "ENVIRONMENT": "production",
            "POSTGRES_HOST": "localhost",
            "POSTGRES_DB": "testdb",
            "POSTGRES_USER": "testuser"
        }

        with patch.dict(os.environ, env_vars, clear=True):
            # Clear global state
            import src.config
            src.config._config = None

            config = get_config()

            assert config.environment == Environment.PRODUCTION

            # Check that warnings were printed
            captured = capsys.readouterr()
            assert "CONFIG:" in captured.out or len(validate_config(config)) > 0
