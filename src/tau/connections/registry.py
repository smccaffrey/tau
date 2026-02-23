"""Connection registry — manages named connections from tau.toml."""

from __future__ import annotations
import os
import re
from typing import Dict, Any
from tau.connectors.base import Connector


class ConnectionRegistry:
    """Registry for named connections configured in tau.toml."""

    def __init__(self, connections_config: Dict[str, Dict[str, Any]] | None = None):
        """Initialize the connection registry.

        Args:
            connections_config: Dict from [connections] section of tau.toml
        """
        self._connections_config = connections_config or {}
        self._connector_factories = {
            'postgres': self._get_postgres,
            'bigquery': self._get_bigquery,
            'snowflake': self._get_snowflake,
            'motherduck': self._get_motherduck,
            'duckdb_local': self._get_duckdb_local,
            'redshift': self._get_redshift,
            'clickhouse': self._get_clickhouse,
            'mysql': self._get_mysql,
            'http_api': self._get_http_api,
            's3': self._get_s3,
        }

    def list_connections(self) -> Dict[str, str]:
        """List all configured connections.

        Returns:
            Dict mapping connection name to connection type
        """
        return {
            name: config.get('type', 'unknown')
            for name, config in self._connections_config.items()
        }

    def get_connection(self, name: str) -> Connector:
        """Get a connector instance for the named connection.

        Args:
            name: Connection name from tau.toml

        Returns:
            Configured connector instance

        Raises:
            KeyError: If connection name not found
            ValueError: If connection type not supported or config invalid
        """
        if name not in self._connections_config:
            raise KeyError(f"Connection '{name}' not found. Available: {list(self._connections_config.keys())}")

        config = self._connections_config[name].copy()
        conn_type = config.pop('type', None)

        if not conn_type:
            raise ValueError(f"Connection '{name}' missing required 'type' field")

        if conn_type not in self._connector_factories:
            raise ValueError(
                f"Unsupported connection type '{conn_type}'. "
                f"Supported: {list(self._connector_factories.keys())}"
            )

        # Resolve environment variables in config values
        resolved_config = self._resolve_env_vars(config)

        # Get the factory function and create the connector
        factory = self._connector_factories[conn_type]
        return factory(**resolved_config)

    async def test_connection(self, name: str) -> bool:
        """Test if a connection can be established.

        Args:
            name: Connection name

        Returns:
            True if connection successful, False otherwise
        """
        try:
            connector = self.get_connection(name)
            async with connector:
                # Connection test passed if we can connect and disconnect
                return True
        except Exception:
            return False

    def _resolve_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve ${ENV_VAR} and ${ENV_VAR:-default} references in config values.

        Args:
            config: Raw config dict

        Returns:
            Config dict with environment variables resolved

        Raises:
            ValueError: If required environment variable is not set
        """
        resolved = {}
        env_var_pattern = re.compile(r'\$\{([^}]+)\}')

        for key, value in config.items():
            if isinstance(value, str):
                resolved[key] = self._resolve_env_var_string(value, env_var_pattern)
            elif isinstance(value, dict):
                # Recursively resolve nested dicts (e.g., headers)
                resolved[key] = self._resolve_env_vars(value)
            else:
                resolved[key] = value

        return resolved

    def _resolve_env_var_string(self, value: str, pattern: re.Pattern) -> str:
        """Resolve environment variables in a single string value."""
        def replace_env_var(match):
            var_expr = match.group(1)

            # Handle default value syntax: VAR:-default
            if ':-' in var_expr:
                var_name, default = var_expr.split(':-', 1)
                return os.environ.get(var_name.strip(), default)
            else:
                var_name = var_expr.strip()
                env_value = os.environ.get(var_name)
                if env_value is None:
                    raise ValueError(f"Environment variable '{var_name}' is not set")
                return env_value

        return pattern.sub(replace_env_var, value)

    # Connector factory methods - use lazy imports to avoid optional dependencies
    def _get_postgres(self, **kwargs):
        from tau.connectors import postgres
        return postgres(**kwargs)

    def _get_bigquery(self, **kwargs):
        from tau.connectors import bigquery
        return bigquery(**kwargs)

    def _get_snowflake(self, **kwargs):
        from tau.connectors import snowflake
        return snowflake(**kwargs)

    def _get_motherduck(self, **kwargs):
        from tau.connectors import motherduck
        return motherduck(**kwargs)

    def _get_duckdb_local(self, **kwargs):
        from tau.connectors import duckdb_local
        return duckdb_local(**kwargs)

    def _get_redshift(self, **kwargs):
        from tau.connectors import redshift
        return redshift(**kwargs)

    def _get_clickhouse(self, **kwargs):
        from tau.connectors import clickhouse
        return clickhouse(**kwargs)

    def _get_mysql(self, **kwargs):
        from tau.connectors import mysql
        return mysql(**kwargs)

    def _get_http_api(self, **kwargs):
        from tau.connectors import http_api
        return http_api(**kwargs)

    def _get_s3(self, **kwargs):
        from tau.connectors import s3
        return s3(**kwargs)