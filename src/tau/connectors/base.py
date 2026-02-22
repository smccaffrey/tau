"""Base connector interface."""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any


class Connector(ABC):
    """Base class for all Tau connectors."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection."""
        ...

    @abstractmethod
    async def extract(self, **kwargs) -> list[dict]:
        """Extract data from the source."""
        ...

    @abstractmethod
    async def load(self, data: list[dict], **kwargs) -> int:
        """Load data to the target. Returns rows loaded."""
        ...

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.disconnect()
