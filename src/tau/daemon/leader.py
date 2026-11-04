"""Scheduler leader election.

Several coordinators can share one PostgreSQL database (behind a load balancer, for
availability). Each keeps the schedules in memory, but only the leader fires scheduled
runs, so a cron fires once. The leader holds a PostgreSQL session-level advisory lock on
a dedicated connection; if it dies, the lock is released and another coordinator takes
over on its next heartbeat. With SQLite there is only one daemon, which is always leader.
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from tau.core import database

logger = logging.getLogger("tau.leader")

# Arbitrary constant identifying Tau's scheduler lock ("tau" + "sched").
LOCK_KEY = 7_461_750_001

_conn = None
_is_leader = False


def is_leader() -> bool:
    return _is_leader or not _uses_postgres()


def _uses_postgres() -> bool:
    return database.engine is not None and database.engine.dialect.name == "postgresql"


async def refresh() -> bool:
    """Acquire, keep or lose leadership. Call it on every heartbeat."""
    global _conn, _is_leader

    if not _uses_postgres():
        _is_leader = True
        return True

    if _conn is not None:
        try:
            await _conn.execute(text("SELECT 1"))
            return _is_leader
        except Exception as e:  # the lock went with the connection
            logger.warning(f"Lost the scheduler lock connection: {e}")
            await _close()

    try:
        _conn = await database.engine.connect()
        got = (await _conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_KEY})).scalar()
        await _conn.commit()
    except Exception as e:
        logger.warning(f"Could not check the scheduler lock: {e}")
        await _close()
        return False

    if got:
        if not _is_leader:
            logger.info("This daemon is now the scheduler leader")
        _is_leader = True
    else:
        _is_leader = False
        await _close()  # try again next heartbeat
    return _is_leader


async def _close() -> None:
    global _conn, _is_leader
    _is_leader = False
    if _conn is not None:
        try:
            await _conn.close()
        except Exception:
            pass
        _conn = None


async def release() -> None:
    """Give up leadership (on shutdown)."""
    if _conn is not None and _is_leader:
        try:
            await _conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_KEY})
        except Exception:
            pass
    await _close()
