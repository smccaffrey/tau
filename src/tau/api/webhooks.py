"""Webhook management API endpoints."""

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from tau.core.auth import verify_api_key
from tau.daemon.webhooks import register_webhook, list_webhooks, remove_webhook

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


class WebhookCreate(BaseModel):
    url: str
    events: list[str] = ["run.failed"]
    secret: str | None = None


@router.post("")
async def add_webhook(data: WebhookCreate, _: str = Depends(verify_api_key)):
    """Register a webhook endpoint."""
    register_webhook(url=data.url, events=data.events, secret=data.secret)
    return {"status": "registered", "url": data.url, "events": data.events}


@router.get("")
async def get_webhooks(_: str = Depends(verify_api_key)):
    """List registered webhooks."""
    return {"webhooks": list_webhooks()}


@router.delete("")
async def delete_webhook(url: str, _: str = Depends(verify_api_key)):
    """Remove a webhook."""
    remove_webhook(url)
    return {"status": "removed", "url": url}
