"""API endpoints for connection management."""

from typing import Dict
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from tau.core.config import get_settings
from tau.connections.registry import ConnectionRegistry

router = APIRouter(prefix="/connections", tags=["connections"])


class ConnectionTestResponse(BaseModel):
    success: bool
    message: str


def get_connection_registry() -> ConnectionRegistry:
    """Get the connection registry instance."""
    settings = get_settings()
    return ConnectionRegistry(settings.connections)


@router.get("/")
async def list_connections(
    registry: ConnectionRegistry = Depends(get_connection_registry),
) -> Dict[str, Dict[str, str]]:
    """List all configured connections.

    Returns connection names and their types, but not sensitive config.
    """
    connections = registry.list_connections()
    return {"connections": connections}


@router.post("/{name}/test")
async def test_connection(
    name: str,
    registry: ConnectionRegistry = Depends(get_connection_registry),
) -> ConnectionTestResponse:
    """Test if a connection can be established."""
    try:
        success = await registry.test_connection(name)
        if success:
            return ConnectionTestResponse(
                success=True,
                message="Connection successful"
            )
        else:
            return ConnectionTestResponse(
                success=False,
                message="Connection failed"
            )
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"Connection '{name}' not found"
        )
    except Exception as e:
        return ConnectionTestResponse(
            success=False,
            message=f"Connection error: {str(e)}"
        )