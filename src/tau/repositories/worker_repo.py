"""Registered remote worker repository."""

from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from tau.core.database import to_db_time
from tau.models.worker import RegisteredWorker


def _now():
    return to_db_time(datetime.now(tz=timezone.utc))


class WorkerRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert(self, worker_id: str, host: str, api_key: str, max_concurrent: int) -> None:
        row = await self.session.get(RegisteredWorker, worker_id)
        if row is None:
            row = RegisteredWorker(worker_id=worker_id, registered_at=_now())
            self.session.add(row)
        row.host, row.api_key, row.max_concurrent = host.rstrip("/"), api_key, max_concurrent
        row.last_seen_at = _now()
        await self.session.commit()

    async def list_all(self) -> list[RegisteredWorker]:
        return list((await self.session.execute(select(RegisteredWorker))).scalars().all())

    async def delete(self, worker_id: str) -> bool:
        result = await self.session.execute(
            delete(RegisteredWorker).where(RegisteredWorker.worker_id == worker_id)
        )
        await self.session.commit()
        return bool(result.rowcount)

    async def prune(self, older_than_seconds: int) -> int:
        """Forget workers that have not re-registered for ``older_than_seconds``."""
        cutoff = _now() - timedelta(seconds=older_than_seconds)
        result = await self.session.execute(
            delete(RegisteredWorker).where(RegisteredWorker.last_seen_at < cutoff)
        )
        await self.session.commit()
        return result.rowcount or 0
