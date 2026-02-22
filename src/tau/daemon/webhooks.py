"""Webhook notifications for pipeline events."""

from __future__ import annotations
import logging
import httpx
from typing import Any

logger = logging.getLogger("tau.webhooks")

# In-memory webhook registry (could be DB-backed later)
_webhooks: list[dict] = []


def register_webhook(url: str, events: list[str] | None = None, secret: str | None = None):
    """Register a webhook endpoint."""
    _webhooks.append({
        "url": url,
        "events": events or ["run.failed"],
        "secret": secret,
    })
    logger.info(f"Registered webhook: {url} for events: {events}")


def list_webhooks() -> list[dict]:
    return [{"url": w["url"], "events": w["events"]} for w in _webhooks]


def remove_webhook(url: str):
    global _webhooks
    _webhooks = [w for w in _webhooks if w["url"] != url]


async def fire_webhook(event: str, payload: dict):
    """Fire webhooks for a given event."""
    for webhook in _webhooks:
        if event in webhook["events"] or "*" in webhook["events"]:
            try:
                headers = {"Content-Type": "application/json"}
                if webhook.get("secret"):
                    headers["X-Tau-Secret"] = webhook["secret"]

                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.post(
                        webhook["url"],
                        json={"event": event, "payload": payload},
                        headers=headers,
                    )
                    logger.info(f"Webhook fired: {webhook['url']} → {resp.status_code}")
            except Exception as e:
                logger.error(f"Webhook failed: {webhook['url']} → {e}")


async def notify_run_complete(pipeline_name: str, run_id: str, status: str, error: str | None = None):
    """Convenience: fire webhook for run completion."""
    event = f"run.{status}"
    await fire_webhook(event, {
        "pipeline": pipeline_name,
        "run_id": run_id,
        "status": status,
        "error": error,
    })
