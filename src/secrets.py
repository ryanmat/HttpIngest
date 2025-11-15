# ABOUTME: Secret management system using Azure Key Vault for production credentials
# ABOUTME: Supports local development with environment variables and production with Key Vault

import os
import logging
from typing import Optional, Dict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SecretConfig:
    """Secret configuration."""
    # Database
    postgres_password: Optional[str] = None

    # Redis
    redis_password: Optional[str] = None

    # Application Insights
    app_insights_connection_string: Optional[str] = None
    app_insights_instrumentation_key: Optional[str] = None

    # Security
    api_keys: list[str] = None
    webhook_secret: Optional[str] = None

    # External services
    grafana_api_key: Optional[str] = None
    prometheus_remote_write_url: Optional[str] = None

    def __post_init__(self):
        if self.api_keys is None:
            self.api_keys = []


class SecretManager:
    """
    Secret management with Azure Key Vault integration.

    Supports:
    - Azure Key Vault for production
    - Environment variables for development
    - Local .env files for development
    """

    def __init__(self, use_key_vault: bool = False, key_vault_name: Optional[str] = None):
        """
        Initialize secret manager.

        Args:
            use_key_vault: Whether to use Azure Key Vault
            key_vault_name: Name of Azure Key Vault (e.g., 'rm-cta-keyvault')
        """
        self.use_key_vault = use_key_vault
        self.key_vault_name = key_vault_name
        self._client = None
        self._cache: Dict[str, str] = {}

        if self.use_key_vault:
            self._initialize_key_vault()

    def _initialize_key_vault(self):
        """Initialize Azure Key Vault client."""
        if not self.key_vault_name:
            logger.warning("Key Vault name not provided, falling back to environment variables")
            self.use_key_vault = False
            return

        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient

            vault_url = f"https://{self.key_vault_name}.vault.azure.net"
            credential = DefaultAzureCredential()
            self._client = SecretClient(vault_url=vault_url, credential=credential)

            logger.info(f"✅ Connected to Azure Key Vault: {self.key_vault_name}")

        except ImportError:
            logger.warning("Azure Key Vault libraries not installed, falling back to environment variables")
            logger.warning("Install with: pip install azure-identity azure-keyvault-secrets")
            self.use_key_vault = False

        except Exception as e:
            logger.error(f"Failed to initialize Key Vault: {e}")
            logger.warning("Falling back to environment variables")
            self.use_key_vault = False

    def get_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get secret from Key Vault or environment variable.

        Args:
            secret_name: Secret name (will be normalized for Key Vault)
            default: Default value if secret not found

        Returns:
            Secret value or default

        Key Vault Secret Names:
            - postgres-password
            - redis-password
            - app-insights-connection-string
            - app-insights-instrumentation-key
            - api-keys
            - webhook-secret
            - grafana-api-key
        """
        # Check cache first
        if secret_name in self._cache:
            return self._cache[secret_name]

        secret_value = None

        if self.use_key_vault and self._client:
            try:
                # Normalize secret name for Key Vault (replace _ with -)
                kv_secret_name = secret_name.replace("_", "-").lower()

                secret = self._client.get_secret(kv_secret_name)
                secret_value = secret.value

                # Cache the secret
                self._cache[secret_name] = secret_value

                logger.info(f"✅ Retrieved secret '{secret_name}' from Key Vault")

            except Exception as e:
                logger.warning(f"Failed to get secret '{secret_name}' from Key Vault: {e}")
                logger.info("Falling back to environment variable")

        # Fall back to environment variable
        if secret_value is None:
            env_var_name = secret_name.upper()
            secret_value = os.getenv(env_var_name, default)

            if secret_value:
                logger.info(f"Retrieved secret '{secret_name}' from environment variable")
            else:
                logger.debug(f"Secret '{secret_name}' not found")

        return secret_value

    def get_secret_list(self, secret_name: str, separator: str = ",") -> list[str]:
        """
        Get secret as list (e.g., API keys).

        Args:
            secret_name: Secret name
            separator: Separator for list items

        Returns:
            List of secret values
        """
        secret_value = self.get_secret(secret_name)
        if not secret_value:
            return []

        return [item.strip() for item in secret_value.split(separator) if item.strip()]

    def load_secrets(self) -> SecretConfig:
        """
        Load all application secrets.

        Returns:
            SecretConfig with all secrets loaded
        """
        logger.info("Loading application secrets...")

        config = SecretConfig(
            # Database
            postgres_password=self.get_secret("postgres_password"),

            # Redis
            redis_password=self.get_secret("redis_password"),

            # Application Insights
            app_insights_connection_string=self.get_secret("app_insights_connection_string"),
            app_insights_instrumentation_key=self.get_secret("app_insights_instrumentation_key"),

            # Security
            api_keys=self.get_secret_list("api_keys"),
            webhook_secret=self.get_secret("webhook_secret"),

            # External services
            grafana_api_key=self.get_secret("grafana_api_key"),
            prometheus_remote_write_url=self.get_secret("prometheus_remote_write_url")
        )

        # Validate critical secrets
        missing_secrets = []

        if not config.postgres_password and not os.getenv("USE_AZURE_AD_AUTH", "false").lower() == "true":
            missing_secrets.append("postgres_password (or USE_AZURE_AD_AUTH=true)")

        if missing_secrets:
            logger.warning(f"Missing critical secrets: {', '.join(missing_secrets)}")

        logger.info(f"✅ Loaded {sum([1 for v in vars(config).values() if v])} secrets")

        return config

    def set_secret(self, secret_name: str, secret_value: str) -> bool:
        """
        Set secret in Key Vault (admin operation).

        Args:
            secret_name: Secret name
            secret_value: Secret value

        Returns:
            True if successful, False otherwise
        """
        if not self.use_key_vault or not self._client:
            logger.error("Key Vault not initialized, cannot set secret")
            return False

        try:
            kv_secret_name = secret_name.replace("_", "-").lower()
            self._client.set_secret(kv_secret_name, secret_value)

            # Invalidate cache
            if secret_name in self._cache:
                del self._cache[secret_name]

            logger.info(f"✅ Set secret '{secret_name}' in Key Vault")
            return True

        except Exception as e:
            logger.error(f"Failed to set secret '{secret_name}': {e}")
            return False

    def list_secrets(self) -> list[str]:
        """
        List all secret names in Key Vault (admin operation).

        Returns:
            List of secret names
        """
        if not self.use_key_vault or not self._client:
            return []

        try:
            secrets = self._client.list_properties_of_secrets()
            return [secret.name for secret in secrets]

        except Exception as e:
            logger.error(f"Failed to list secrets: {e}")
            return []


def get_secret_manager(environment: str = "development") -> SecretManager:
    """
    Get appropriate secret manager for environment.

    Args:
        environment: Environment name (development, staging, production)

    Returns:
        SecretManager instance
    """
    use_key_vault = environment in ("staging", "production")
    key_vault_name = os.getenv("AZURE_KEY_VAULT_NAME", "rm-cta-keyvault")

    return SecretManager(
        use_key_vault=use_key_vault,
        key_vault_name=key_vault_name if use_key_vault else None
    )


# Example usage and CLI for secret management
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m src.secrets get <secret-name>")
        print("  python -m src.secrets set <secret-name> <secret-value>")
        print("  python -m src.secrets list")
        sys.exit(1)

    command = sys.argv[1]
    manager = get_secret_manager(os.getenv("ENVIRONMENT", "development"))

    if command == "get":
        if len(sys.argv) < 3:
            print("Error: Secret name required")
            sys.exit(1)

        secret_name = sys.argv[2]
        value = manager.get_secret(secret_name)

        if value:
            print(f"{secret_name}: {value[:10]}..." if len(value) > 10 else value)
        else:
            print(f"Secret '{secret_name}' not found")

    elif command == "set":
        if len(sys.argv) < 4:
            print("Error: Secret name and value required")
            sys.exit(1)

        secret_name = sys.argv[2]
        secret_value = sys.argv[3]

        if manager.set_secret(secret_name, secret_value):
            print(f"✅ Secret '{secret_name}' set successfully")
        else:
            print(f"❌ Failed to set secret '{secret_name}'")

    elif command == "list":
        secrets = manager.list_secrets()
        if secrets:
            print("Secrets in Key Vault:")
            for secret in secrets:
                print(f"  - {secret}")
        else:
            print("No secrets found or Key Vault not configured")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
