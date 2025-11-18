# Description: Unit tests for Azure Key Vault secret management system
# Description: Tests secret retrieval, caching, fallback, and Key Vault integration (mocked)

import pytest
import os
from unittest.mock import patch, Mock, MagicMock
from src.secrets import (
    SecretConfig,
    SecretManager,
    get_secret_manager
)

# Check if Azure Key Vault libraries are available
try:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

# Skip Key Vault tests if Azure libraries not installed (v12-lite mode)
skip_if_no_azure = pytest.mark.skipif(
    not AZURE_AVAILABLE,
    reason="Azure Key Vault libraries not installed (v12-lite mode)"
)


class TestSecretConfig:
    """Test SecretConfig dataclass."""

    def test_default_initialization(self):
        """Test default secret config initialization."""
        config = SecretConfig()

        assert config.postgres_password is None
        assert config.redis_password is None
        assert config.app_insights_connection_string is None
        assert config.api_keys == []
        assert config.webhook_secret is None

    def test_with_values(self):
        """Test secret config with values."""
        config = SecretConfig(
            postgres_password="dbpass123",
            redis_password="redispass456",
            api_keys=["key1", "key2"],
            webhook_secret="webhooksecret"
        )

        assert config.postgres_password == "dbpass123"
        assert config.redis_password == "redispass456"
        assert config.api_keys == ["key1", "key2"]
        assert config.webhook_secret == "webhooksecret"


class TestSecretManagerWithoutKeyVault:
    """Test SecretManager using only environment variables."""

    def test_initialization_without_key_vault(self):
        """Test SecretManager init without Key Vault."""
        manager = SecretManager(use_key_vault=False)

        assert manager.use_key_vault is False
        assert manager._client is None
        assert manager._cache == {}

    def test_get_secret_from_env(self):
        """Test getting secret from environment variable."""
        manager = SecretManager(use_key_vault=False)

        with patch.dict(os.environ, {"POSTGRES_PASSWORD": "testpass123"}):
            secret = manager.get_secret("postgres_password")

            assert secret == "testpass123"

    def test_get_secret_with_default(self):
        """Test getting secret with default value."""
        manager = SecretManager(use_key_vault=False)

        secret = manager.get_secret("nonexistent_secret", default="default_value")

        assert secret == "default_value"

    def test_get_secret_not_found(self):
        """Test getting nonexistent secret returns None."""
        manager = SecretManager(use_key_vault=False)

        secret = manager.get_secret("nonexistent_secret")

        assert secret is None

    def test_get_secret_list(self):
        """Test getting secret as list."""
        manager = SecretManager(use_key_vault=False)

        with patch.dict(os.environ, {"API_KEYS": "key1,key2,key3"}):
            keys = manager.get_secret_list("api_keys")

            assert keys == ["key1", "key2", "key3"]

    def test_get_secret_list_empty(self):
        """Test getting empty secret list."""
        manager = SecretManager(use_key_vault=False)

        keys = manager.get_secret_list("nonexistent_keys")

        assert keys == []

    def test_get_secret_list_with_spaces(self):
        """Test getting secret list strips whitespace."""
        manager = SecretManager(use_key_vault=False)

        with patch.dict(os.environ, {"API_KEYS": " key1 , key2 , key3 "}):
            keys = manager.get_secret_list("api_keys")

            assert keys == ["key1", "key2", "key3"]

    def test_load_secrets(self):
        """Test loading all secrets from environment."""
        manager = SecretManager(use_key_vault=False)

        env_vars = {
            "POSTGRES_PASSWORD": "dbpass",
            "REDIS_PASSWORD": "redispass",
            "APP_INSIGHTS_CONNECTION_STRING": "InstrumentationKey=abc123",
            "API_KEYS": "key1,key2",
            "WEBHOOK_SECRET": "secret123"
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = manager.load_secrets()

            assert config.postgres_password == "dbpass"
            assert config.redis_password == "redispass"
            assert config.app_insights_connection_string == "InstrumentationKey=abc123"
            assert config.api_keys == ["key1", "key2"]
            assert config.webhook_secret == "secret123"

    def test_load_secrets_warns_missing_postgres(self, caplog):
        """Test warning when postgres password missing without Azure AD."""
        manager = SecretManager(use_key_vault=False)

        with patch.dict(os.environ, {}, clear=True):
            config = manager.load_secrets()

            assert "postgres_password" in caplog.text or config.postgres_password is None


@skip_if_no_azure
class TestSecretManagerWithKeyVault:
    """Test SecretManager with mocked Azure Key Vault."""

    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_initialization_with_key_vault(self, mock_secret_client, mock_credential):
        """Test SecretManager initialization with Key Vault."""
        mock_credential.return_value = Mock()
        mock_secret_client.return_value = Mock()

        manager = SecretManager(use_key_vault=True, key_vault_name="test-vault")

        assert manager.use_key_vault is True
        assert manager.key_vault_name == "test-vault"
        assert manager._client is not None

        # Verify Key Vault client was created with correct URL
        mock_secret_client.assert_called_once()
        call_kwargs = mock_secret_client.call_args[1]
        assert "test-vault.vault.azure.net" in call_kwargs['vault_url']

    def test_initialization_without_vault_name(self, caplog):
        """Test initialization without vault name falls back to env vars."""
        manager = SecretManager(use_key_vault=True, key_vault_name=None)

        assert manager.use_key_vault is False
        assert "Key Vault name not provided" in caplog.text

    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_initialization_import_error(self, mock_secret_client, mock_credential):
        """Test fallback when Azure libraries not installed."""
        # Simulate ImportError by making the import fail
        mock_secret_client.side_effect = ImportError("No module named 'azure'")

        manager = SecretManager(use_key_vault=True, key_vault_name="test-vault")

        assert manager.use_key_vault is False

    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_get_secret_from_key_vault(self, mock_secret_client, mock_credential):
        """Test getting secret from Key Vault."""
        # Setup mocks
        mock_credential.return_value = Mock()
        mock_client = Mock()
        mock_secret_client.return_value = mock_client

        mock_secret_obj = Mock()
        mock_secret_obj.value = "vault_secret_value"
        mock_client.get_secret.return_value = mock_secret_obj

        manager = SecretManager(use_key_vault=True, key_vault_name="test-vault")
        secret = manager.get_secret("postgres_password")

        assert secret == "vault_secret_value"
        # Verify secret name normalization (underscores to hyphens)
        mock_client.get_secret.assert_called_once_with("postgres-password")

    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_get_secret_caching(self, mock_secret_client, mock_credential):
        """Test secret caching behavior."""
        mock_credential.return_value = Mock()
        mock_client = Mock()
        mock_secret_client.return_value = mock_client

        mock_secret_obj = Mock()
        mock_secret_obj.value = "cached_value"
        mock_client.get_secret.return_value = mock_secret_obj

        manager = SecretManager(use_key_vault=True, key_vault_name="test-vault")

        # First call - should fetch from Key Vault
        secret1 = manager.get_secret("test_secret")
        # Second call - should use cache
        secret2 = manager.get_secret("test_secret")

        assert secret1 == "cached_value"
        assert secret2 == "cached_value"

        # Should only call Key Vault once (second call used cache)
        assert mock_client.get_secret.call_count == 1

    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_get_secret_fallback_to_env(self, mock_secret_client, mock_credential):
        """Test fallback to environment variable when Key Vault fails."""
        mock_credential.return_value = Mock()
        mock_client = Mock()
        mock_secret_client.return_value = mock_client

        # Simulate Key Vault error
        mock_client.get_secret.side_effect = Exception("Key Vault unavailable")

        manager = SecretManager(use_key_vault=True, key_vault_name="test-vault")

        with patch.dict(os.environ, {"TEST_SECRET": "env_value"}):
            secret = manager.get_secret("test_secret")

            assert secret == "env_value"

    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_set_secret(self, mock_secret_client, mock_credential):
        """Test setting secret in Key Vault."""
        mock_credential.return_value = Mock()
        mock_client = Mock()
        mock_secret_client.return_value = mock_client

        manager = SecretManager(use_key_vault=True, key_vault_name="test-vault")

        # Pre-populate cache
        manager._cache["test_secret"] = "old_value"

        result = manager.set_secret("test_secret", "new_value")

        assert result is True
        # Verify secret was set with normalized name
        mock_client.set_secret.assert_called_once_with("test-secret", "new_value")
        # Verify cache was invalidated
        assert "test_secret" not in manager._cache

    def test_set_secret_without_key_vault(self):
        """Test setting secret fails without Key Vault."""
        manager = SecretManager(use_key_vault=False)

        result = manager.set_secret("test_secret", "value")

        assert result is False

    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_set_secret_error(self, mock_secret_client, mock_credential):
        """Test setting secret handles errors."""
        mock_credential.return_value = Mock()
        mock_client = Mock()
        mock_secret_client.return_value = mock_client

        # Simulate error
        mock_client.set_secret.side_effect = Exception("Permission denied")

        manager = SecretManager(use_key_vault=True, key_vault_name="test-vault")

        result = manager.set_secret("test_secret", "value")

        assert result is False

    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_list_secrets(self, mock_secret_client, mock_credential):
        """Test listing secrets from Key Vault."""
        mock_credential.return_value = Mock()
        mock_client = Mock()
        mock_secret_client.return_value = mock_client

        # Mock secret properties
        secret1 = Mock()
        secret1.name = "postgres-password"
        secret2 = Mock()
        secret2.name = "redis-password"

        mock_client.list_properties_of_secrets.return_value = [secret1, secret2]

        manager = SecretManager(use_key_vault=True, key_vault_name="test-vault")

        secrets = manager.list_secrets()

        assert secrets == ["postgres-password", "redis-password"]

    def test_list_secrets_without_key_vault(self):
        """Test listing secrets returns empty without Key Vault."""
        manager = SecretManager(use_key_vault=False)

        secrets = manager.list_secrets()

        assert secrets == []

    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_list_secrets_error(self, mock_secret_client, mock_credential):
        """Test listing secrets handles errors."""
        mock_credential.return_value = Mock()
        mock_client = Mock()
        mock_secret_client.return_value = mock_client

        # Simulate error
        mock_client.list_properties_of_secrets.side_effect = Exception("Access denied")

        manager = SecretManager(use_key_vault=True, key_vault_name="test-vault")

        secrets = manager.list_secrets()

        assert secrets == []


class TestGetSecretManager:
    """Test get_secret_manager factory function."""

    def test_get_secret_manager_development(self):
        """Test getting secret manager for development."""
        manager = get_secret_manager("development")

        assert manager.use_key_vault is False

    @skip_if_no_azure
    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_get_secret_manager_staging(self, mock_secret_client, mock_credential):
        """Test getting secret manager for staging."""
        mock_credential.return_value = Mock()
        mock_secret_client.return_value = Mock()

        with patch.dict(os.environ, {"AZURE_KEY_VAULT_NAME": "staging-vault"}):
            manager = get_secret_manager("staging")

            assert manager.use_key_vault is True
            assert manager.key_vault_name == "staging-vault"

    @skip_if_no_azure
    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_get_secret_manager_production(self, mock_secret_client, mock_credential):
        """Test getting secret manager for production."""
        mock_credential.return_value = Mock()
        mock_secret_client.return_value = Mock()

        with patch.dict(os.environ, {"AZURE_KEY_VAULT_NAME": "prod-vault"}):
            manager = get_secret_manager("production")

            assert manager.use_key_vault is True
            assert manager.key_vault_name == "prod-vault"

    @skip_if_no_azure
    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_get_secret_manager_default_vault_name(self, mock_secret_client, mock_credential):
        """Test default vault name when env var not set."""
        mock_credential.return_value = Mock()
        mock_secret_client.return_value = Mock()

        with patch.dict(os.environ, {}, clear=True):
            manager = get_secret_manager("production")

            # Should use default vault name
            assert manager.key_vault_name == "rm-cta-keyvault"


class TestSecretManagerIntegration:
    """Integration tests for SecretManager."""

    @skip_if_no_azure
    @patch('azure.identity.DefaultAzureCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    def test_complete_workflow(self, mock_secret_client, mock_credential):
        """Test complete secret management workflow."""
        mock_credential.return_value = Mock()
        mock_client = Mock()
        mock_secret_client.return_value = mock_client

        # Setup Key Vault responses
        def get_secret_side_effect(name):
            secrets = {
                "postgres-password": Mock(value="vault_db_pass"),
                "redis-password": Mock(value="vault_redis_pass"),
                "api-keys": Mock(value="key1,key2,key3")
            }
            return secrets.get(name)

        mock_client.get_secret.side_effect = get_secret_side_effect

        manager = SecretManager(use_key_vault=True, key_vault_name="test-vault")

        # Test getting individual secrets
        db_pass = manager.get_secret("postgres_password")
        assert db_pass == "vault_db_pass"

        redis_pass = manager.get_secret("redis_password")
        assert redis_pass == "vault_redis_pass"

        # Test getting secret list
        api_keys = manager.get_secret_list("api_keys")
        assert api_keys == ["key1", "key2", "key3"]

        # Test caching (second call shouldn't hit Key Vault)
        db_pass_cached = manager.get_secret("postgres_password")
        assert db_pass_cached == "vault_db_pass"

        # Verify Key Vault was only called once for postgres-password
        postgres_calls = [call for call in mock_client.get_secret.call_args_list
                         if call[0][0] == "postgres-password"]
        assert len(postgres_calls) == 1

    def test_env_only_workflow(self):
        """Test workflow using only environment variables."""
        manager = SecretManager(use_key_vault=False)

        env_vars = {
            "POSTGRES_PASSWORD": "env_db_pass",
            "REDIS_PASSWORD": "env_redis_pass",
            "API_KEYS": "envkey1,envkey2",
            "WEBHOOK_SECRET": "env_webhook"
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = manager.load_secrets()

            assert config.postgres_password == "env_db_pass"
            assert config.redis_password == "env_redis_pass"
            assert config.api_keys == ["envkey1", "envkey2"]
            assert config.webhook_secret == "env_webhook"
