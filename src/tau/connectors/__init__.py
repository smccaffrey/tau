"""Connectors — lazy imports to avoid requiring optional dependencies."""

from tau.connectors.base import Connector

# Lazy imports — only fail when you actually use the connector
def postgres(*args, **kwargs):
    from tau.connectors.postgres import postgres as _postgres
    return _postgres(*args, **kwargs)

def bigquery(*args, **kwargs):
    from tau.connectors.bigquery import bigquery as _bigquery
    return _bigquery(*args, **kwargs)

def snowflake(*args, **kwargs):
    from tau.connectors.snowflake import snowflake as _snowflake
    return _snowflake(*args, **kwargs)

def motherduck(*args, **kwargs):
    from tau.connectors.motherduck import motherduck as _motherduck
    return _motherduck(*args, **kwargs)

def duckdb_local(*args, **kwargs):
    from tau.connectors.motherduck import duckdb_local as _duckdb_local
    return _duckdb_local(*args, **kwargs)

def redshift(*args, **kwargs):
    from tau.connectors.redshift import redshift as _redshift
    return _redshift(*args, **kwargs)

def clickhouse(*args, **kwargs):
    from tau.connectors.clickhouse import clickhouse as _clickhouse
    return _clickhouse(*args, **kwargs)

def mysql(*args, **kwargs):
    from tau.connectors.mysql import mysql as _mysql
    return _mysql(*args, **kwargs)

def http_api(*args, **kwargs):
    from tau.connectors.http_api import http_api as _http_api
    return _http_api(*args, **kwargs)

def s3(*args, **kwargs):
    from tau.connectors.s3 import s3 as _s3
    return _s3(*args, **kwargs)

__all__ = [
    "Connector",
    "postgres", "bigquery", "snowflake", "motherduck", "duckdb_local",
    "redshift", "clickhouse", "mysql", "http_api", "s3",
]
