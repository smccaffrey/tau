"""Tests for connection registry functionality."""

import os
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from tau.connections.registry import ConnectionRegistry
from tau.pipeline.context import PipelineContext


class TestConnectionRegistry:
    """Test the connection registry functionality."""

    def test_empty_registry(self):
        """Test registry with no connections configured."""
        registry = ConnectionRegistry({})
        assert registry.list_connections() == {}

    def test_list_connections(self):
        """Test listing configured connections."""
        connections_config = {
            "warehouse": {"type": "postgres", "dsn": "postgresql://test"},
            "api": {"type": "http_api", "base_url": "https://api.test.com"},
            "lake": {"type": "snowflake", "account": "test123"},
        }
        registry = ConnectionRegistry(connections_config)

        connections = registry.list_connections()
        assert connections == {
            "warehouse": "postgres",
            "api": "http_api",
            "lake": "snowflake",
        }

    def test_get_connection_not_found(self):
        """Test error when connection name not found."""
        registry = ConnectionRegistry({})

        with pytest.raises(KeyError, match="Connection 'missing' not found"):
            registry.get_connection("missing")

    def test_get_connection_missing_type(self):
        """Test error when connection config missing type field."""
        connections_config = {
            "bad_conn": {"dsn": "test://example"}  # Missing 'type'
        }
        registry = ConnectionRegistry(connections_config)

        with pytest.raises(ValueError, match="missing required 'type' field"):
            registry.get_connection("bad_conn")

    def test_get_connection_unsupported_type(self):
        """Test error when connection type not supported."""
        connections_config = {
            "bad_conn": {"type": "unsupported_db", "dsn": "test://example"}
        }
        registry = ConnectionRegistry(connections_config)

        with pytest.raises(ValueError, match="Unsupported connection type"):
            registry.get_connection("bad_conn")

    @patch.dict(os.environ, {"TEST_DSN": "postgresql://env_value"})
    def test_env_var_resolution(self):
        """Test environment variable resolution in config values."""
        connections_config = {
            "test_conn": {
                "type": "postgres",
                "dsn": "${TEST_DSN}",
                "other": "static_value"
            }
        }
        registry = ConnectionRegistry(connections_config)

        # Mock the postgres connector factory
        with patch('tau.connectors.postgres') as mock_postgres:
            mock_connector = MagicMock()
            mock_postgres.return_value = mock_connector

            result = registry.get_connection("test_conn")

            # Verify the environment variable was resolved
            mock_postgres.assert_called_once_with(
                dsn="postgresql://env_value",
                other="static_value"
            )
            assert result == mock_connector

    @patch.dict(os.environ, {"TEST_VAR": "env_value"})
    def test_env_var_with_default(self):
        """Test environment variable with default value syntax."""
        connections_config = {
            "test_conn": {
                "type": "postgres",
                "existing": "${TEST_VAR:-default}",
                "missing": "${MISSING_VAR:-fallback}",
            }
        }
        registry = ConnectionRegistry(connections_config)

        with patch('tau.connectors.postgres') as mock_postgres:
            registry.get_connection("test_conn")

            mock_postgres.assert_called_once_with(
                existing="env_value",     # From environment
                missing="fallback"       # From default
            )

    def test_env_var_missing_no_default(self):
        """Test error when required environment variable is not set."""
        connections_config = {
            "test_conn": {
                "type": "postgres",
                "dsn": "${MISSING_VAR}",
            }
        }
        registry = ConnectionRegistry(connections_config)

        with pytest.raises(ValueError, match="Environment variable 'MISSING_VAR' is not set"):
            registry.get_connection("test_conn")

    def test_nested_dict_env_resolution(self):
        """Test environment variable resolution in nested dicts."""
        with patch.dict(os.environ, {"API_TOKEN": "secret123"}):
            connections_config = {
                "api_conn": {
                    "type": "http_api",
                    "base_url": "https://api.test.com",
                    "headers": {
                        "Authorization": "Bearer ${API_TOKEN}",
                        "Content-Type": "application/json"
                    }
                }
            }
            registry = ConnectionRegistry(connections_config)

            with patch('tau.connectors.http_api') as mock_http_api:
                registry.get_connection("api_conn")

                mock_http_api.assert_called_once_with(
                    base_url="https://api.test.com",
                    headers={
                        "Authorization": "Bearer secret123",
                        "Content-Type": "application/json"
                    }
                )

    @pytest.mark.asyncio
    async def test_test_connection_success(self):
        """Test successful connection test."""
        connections_config = {
            "test_conn": {"type": "postgres", "dsn": "test://example"}
        }
        registry = ConnectionRegistry(connections_config)

        # Mock connector that connects successfully
        mock_connector = AsyncMock()
        mock_connector.__aenter__ = AsyncMock(return_value=mock_connector)
        mock_connector.__aexit__ = AsyncMock(return_value=None)

        with patch('tau.connectors.postgres', return_value=mock_connector):
            result = await registry.test_connection("test_conn")
            assert result is True
            mock_connector.__aenter__.assert_called_once()
            mock_connector.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_test_connection_failure(self):
        """Test failed connection test."""
        connections_config = {
            "test_conn": {"type": "postgres", "dsn": "test://example"}
        }
        registry = ConnectionRegistry(connections_config)

        # Mock connector that fails to connect
        mock_connector = AsyncMock()
        mock_connector.__aenter__ = AsyncMock(side_effect=Exception("Connection failed"))

        with patch('tau.connectors.postgres', return_value=mock_connector):
            result = await registry.test_connection("test_conn")
            assert result is False

    def test_postgres_connector_factory(self):
        """Test postgres connector factory method."""
        connections_config = {
            "pg_conn": {
                "type": "postgres",
                "dsn": "postgresql://test",
                "pool_size": 10
            }
        }
        registry = ConnectionRegistry(connections_config)

        with patch('tau.connectors.postgres') as mock_postgres:
            mock_connector = MagicMock()
            mock_postgres.return_value = mock_connector

            result = registry.get_connection("pg_conn")

            mock_postgres.assert_called_once_with(
                dsn="postgresql://test",
                pool_size=10
            )
            assert result == mock_connector

    def test_http_api_connector_factory(self):
        """Test http_api connector factory method."""
        connections_config = {
            "api_conn": {
                "type": "http_api",
                "base_url": "https://api.test.com",
                "timeout": 30
            }
        }
        registry = ConnectionRegistry(connections_config)

        with patch('tau.connectors.http_api') as mock_http_api:
            mock_connector = MagicMock()
            mock_http_api.return_value = mock_connector

            result = registry.get_connection("api_conn")

            mock_http_api.assert_called_once_with(
                base_url="https://api.test.com",
                timeout=30
            )
            assert result == mock_connector


class TestPipelineContextConnections:
    """Test connection functionality in PipelineContext."""

    def test_connection_no_registry(self):
        """Test error when no connection registry available."""
        ctx = PipelineContext("test", "run1", connection_registry=None)

        with pytest.raises(ValueError, match="No connection registry available"):
            asyncio.run(ctx.connection("test"))

    @pytest.mark.asyncio
    async def test_connection_success(self):
        """Test successful connection retrieval."""
        connections_config = {
            "test_conn": {"type": "postgres", "dsn": "test://example"}
        }
        registry = ConnectionRegistry(connections_config)
        ctx = PipelineContext("test", "run1", connection_registry=registry)

        mock_connector = AsyncMock()
        with patch('tau.connectors.postgres', return_value=mock_connector):
            result = await ctx.connection("test_conn")

            assert result == mock_connector
            mock_connector.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_caching(self):
        """Test that connections are cached per pipeline run."""
        connections_config = {
            "test_conn": {"type": "postgres", "dsn": "test://example"}
        }
        registry = ConnectionRegistry(connections_config)
        ctx = PipelineContext("test", "run1", connection_registry=registry)

        mock_connector = AsyncMock()
        with patch('tau.connectors.postgres', return_value=mock_connector) as mock_postgres:
            # First call should create and connect
            result1 = await ctx.connection("test_conn")
            assert result1 == mock_connector
            mock_connector.connect.assert_called_once()

            # Second call should return cached connection
            result2 = await ctx.connection("test_conn")
            assert result2 == mock_connector
            assert result1 is result2

            # postgres() should only be called once (not cached)
            mock_postgres.assert_called_once()
            # connect() should only be called once (cached connection)
            assert mock_connector.connect.call_count == 1

    @pytest.mark.asyncio
    async def test_connection_not_found(self):
        """Test error when connection name not found in registry."""
        registry = ConnectionRegistry({})
        ctx = PipelineContext("test", "run1", connection_registry=registry)

        with pytest.raises(KeyError, match="Connection 'missing' not found"):
            await ctx.connection("missing")

    @pytest.mark.asyncio
    async def test_cleanup_connections(self):
        """Test cleanup of active connections."""
        connections_config = {
            "conn1": {"type": "postgres", "dsn": "test://1"},
            "conn2": {"type": "postgres", "dsn": "test://2"},
        }
        registry = ConnectionRegistry(connections_config)
        ctx = PipelineContext("test", "run1", connection_registry=registry)

        mock_conn1 = AsyncMock()
        mock_conn2 = AsyncMock()

        with patch('tau.connectors.postgres', side_effect=[mock_conn1, mock_conn2]):
            # Connect to both
            await ctx.connection("conn1")
            await ctx.connection("conn2")

            assert len(ctx._active_connections) == 2

            # Cleanup should disconnect all
            await ctx.cleanup_connections()

            mock_conn1.disconnect.assert_called_once()
            mock_conn2.disconnect.assert_called_once()
            assert len(ctx._active_connections) == 0

    @pytest.mark.asyncio
    async def test_cleanup_connections_error_handling(self):
        """Test cleanup handles disconnect errors gracefully."""
        connections_config = {
            "conn1": {"type": "postgres", "dsn": "test://1"},
        }
        registry = ConnectionRegistry(connections_config)
        ctx = PipelineContext("test", "run1", connection_registry=registry)

        mock_conn = AsyncMock()
        mock_conn.disconnect.side_effect = Exception("Disconnect failed")

        with patch('tau.connectors.postgres', return_value=mock_conn):
            await ctx.connection("conn1")

            # Cleanup should handle disconnect errors gracefully
            await ctx.cleanup_connections()

            mock_conn.disconnect.assert_called_once()
            # Should still clear the connections dict
            assert len(ctx._active_connections) == 0


# We need to make asyncio available for the tests that use it directly
import asyncio