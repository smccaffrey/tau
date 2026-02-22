"""Materialization strategy definitions."""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class MaterializationType(str, Enum):
    FULL_REFRESH = "full_refresh"       # DROP + CREATE AS
    INCREMENTAL = "incremental"         # MERGE/INSERT new rows
    PARTITIONED = "partitioned"         # Partition-aware incremental
    SCD_TYPE_1 = "scd_type_1"           # Overwrite changed rows
    SCD_TYPE_2 = "scd_type_2"           # Track history with valid_from/valid_to
    SNAPSHOT = "snapshot"               # Point-in-time snapshot with timestamp
    APPEND_ONLY = "append_only"         # Insert only, no updates
    VIEW = "view"                       # CREATE OR REPLACE VIEW


@dataclass
class MaterializationConfig:
    """Base config for all materialization strategies."""
    target_table: str
    source_query: str
    strategy: MaterializationType = MaterializationType.FULL_REFRESH


@dataclass
class FullRefreshConfig(MaterializationConfig):
    """Drop and recreate the table on every run."""
    strategy: MaterializationType = MaterializationType.FULL_REFRESH
    pre_hook: str | None = None    # SQL to run before materialization
    post_hook: str | None = None   # SQL to run after materialization


@dataclass
class IncrementalConfig(MaterializationConfig):
    """Incrementally merge new/changed rows."""
    strategy: MaterializationType = MaterializationType.INCREMENTAL
    unique_key: str | list[str] = ""                 # Column(s) for MERGE match
    incremental_column: str = ""                     # Column to filter new rows (e.g. updated_at)
    incremental_strategy: str = "merge"              # merge | delete+insert | insert_overwrite
    on_schema_change: str = "append_new_columns"     # ignore | fail | append_new_columns | sync_all_columns


@dataclass
class PartitionedConfig(MaterializationConfig):
    """Partition-aware incremental materialization."""
    strategy: MaterializationType = MaterializationType.PARTITIONED
    unique_key: str | list[str] = ""
    partition_by: str = ""                           # Column to partition by
    partition_type: str = "date"                     # date | range | list
    partition_granularity: str = "day"               # hour | day | month | year (for date)
    partition_range: list | None = None              # For range/list partitioning
    cluster_by: list[str] = field(default_factory=list)  # Clustering columns (BigQuery, Snowflake)
    incremental_column: str = ""
    partition_expiration_days: int | None = None     # Auto-expire old partitions


@dataclass
class SCDType1Config(MaterializationConfig):
    """SCD Type 1: Overwrite old values with new ones."""
    strategy: MaterializationType = MaterializationType.SCD_TYPE_1
    unique_key: str | list[str] = ""                 # Business key for matching
    tracked_columns: list[str] = field(default_factory=list)  # Columns to check for changes (empty = all)
    updated_at_column: str = "updated_at"            # Auto-set on change


@dataclass
class SCDType2Config(MaterializationConfig):
    """SCD Type 2: Track full history with valid_from/valid_to."""
    strategy: MaterializationType = MaterializationType.SCD_TYPE_2
    unique_key: str | list[str] = ""                 # Business key
    tracked_columns: list[str] = field(default_factory=list)  # Columns that trigger new version
    valid_from_column: str = "valid_from"
    valid_to_column: str = "valid_to"
    is_current_column: str = "is_current"
    hash_column: str = "row_hash"                    # Hash of tracked columns for change detection
    invalidate_hard_deletes: bool = True             # Close records that disappear from source


@dataclass
class SnapshotConfig(MaterializationConfig):
    """Point-in-time snapshot: append a full copy with a snapshot timestamp."""
    strategy: MaterializationType = MaterializationType.SNAPSHOT
    snapshot_timestamp_column: str = "snapshot_at"
    snapshot_id_column: str = "snapshot_id"
    unique_key: str | list[str] = ""                 # Optional: dedupe within snapshot
    retain_snapshots: int | None = None              # Keep N most recent snapshots (prune older)
