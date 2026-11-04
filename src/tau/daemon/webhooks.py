"""Webhook notifications for pipeline events. Subscriptions are stored in the database."""

from __future__ import annotations

import logging

import httpx
from sqlalchemy import delete, select

from tau.core import database
from tau.models.webhook import Webhook

logger = logging.getLogger("tau.webhooks")


async def register_webhook(url: str, events: list[str] | None = None, secret: str | None = None):
    """Register (or update) a webhook endpoint."""
    events = events or ["run.failed"]
    async with database.async_session_factory() as session:
        existing = (await session.execute(select(Webhook).where(Webhook.url == url))).scalar_one_or_none()
        if existing:
            existing.events, existing.secret = events, secret
        else:
            session.add(Webhook(url=url, events=events, secret=secret))
        await session.commit()
    logger.info(f"Registered webhook: {url} for events: {events}")


async def _all() -> list[Webhook]:
    if database.async_session_factory is None:
        return []
    async with database.async_session_factory() as session:
        return list((await session.execute(select(Webhook).order_by(Webhook.created_at))).scalars().all())


async def list_webhooks() -> list[dict]:
    return [{"url": w.url, "events": w.events} for w in await _all()]


async def remove_webhook(url: str) -> bool:
    async with database.async_session_factory() as session:
        result = await session.execute(delete(Webhook).where(Webhook.url == url))
        await session.commit()
        return bool(result.rowcount)


async def fire_webhook(event: str, payload: dict):
    """Fire webhooks for a given event. Delivery failures are logged, never raised."""
    try:
        webhooks = await _all()
    except Exception as e:
        logger.error(f"Could not load webhooks: {e}")
        return
    for webhook in webhooks:
        if event in webhook.events or "*" in webhook.events:
            try:
                headers = {"Content-Type": "application/json"}
                if webhook.secret:
                    headers["X-Tau-Secret"] = webhook.secret

                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.post(
                        webhook.url,
                        json={"event": event, "payload": payload},
                        headers=headers,
                    )
                    logger.info(f"Webhook fired: {webhook.url} → {resp.status_code}")
            except Exception as e:
                logger.error(f"Webhook failed: {webhook.url} → {e}")


async def notify_run_complete(pipeline_name: str, run_id: str, status: str, error: str | None = None):
    """Convenience: fire webhook for run completion."""
    event = f"run.{status}"
    await fire_webhook(event, {
        "pipeline": pipeline_name,
        "run_id": run_id,
        "status": status,
        "error": error,
    })
