"""Generic HTTP API connector."""

from __future__ import annotations
from typing import Any
import httpx
from tau.connectors.base import Connector


class HttpApiConnector(Connector):
    """Connect to any HTTP API for data extraction."""

    def __init__(
        self,
        base_url: str,
        auth: dict | None = None,
        headers: dict | None = None,
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.auth = auth or {}
        self.headers = headers or {}
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        headers = {**self.headers}

        # Apply auth
        auth_type = self.auth.get("type", "").lower()
        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self.auth['token']}"
        elif auth_type == "api_key":
            header_name = self.auth.get("header", "X-API-Key")
            headers[header_name] = self.auth["key"]
        elif auth_type == "basic":
            import base64
            creds = base64.b64encode(
                f"{self.auth['username']}:{self.auth['password']}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {creds}"

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=self.timeout,
        )

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def extract(
        self,
        path: str = "",
        method: str = "GET",
        params: dict | None = None,
        json_body: dict | None = None,
        response_path: str | None = None,
        paginate: bool = False,
        page_param: str = "page",
        per_page_param: str = "per_page",
        per_page: int = 100,
        max_pages: int = 100,
        **kwargs,
    ) -> list[dict]:
        """Extract data from an HTTP API endpoint."""
        if not self._client:
            await self.connect()

        all_data = []

        if paginate:
            for page in range(1, max_pages + 1):
                page_params = {**(params or {}), page_param: page, per_page_param: per_page}
                resp = await self._client.request(method, path, params=page_params, json=json_body)
                resp.raise_for_status()
                data = resp.json()

                # Navigate to nested response path
                if response_path:
                    for key in response_path.split("."):
                        data = data[key]

                if not data:
                    break

                all_data.extend(data if isinstance(data, list) else [data])
        else:
            resp = await self._client.request(method, path, params=params, json=json_body)
            resp.raise_for_status()
            data = resp.json()

            if response_path:
                for key in response_path.split("."):
                    data = data[key]

            all_data = data if isinstance(data, list) else [data]

        return all_data

    async def load(self, data: list[dict], **kwargs) -> int:
        """POST data to an API endpoint."""
        if not self._client:
            await self.connect()

        path = kwargs.get("path", "")
        batch = kwargs.get("batch", False)

        if batch:
            resp = await self._client.post(path, json=data)
            resp.raise_for_status()
        else:
            for record in data:
                resp = await self._client.post(path, json=record)
                resp.raise_for_status()

        return len(data)


def http_api(
    url: str,
    auth: dict | None = None,
    headers: dict | None = None,
    timeout: int = 30,
) -> HttpApiConnector:
    """Create an HTTP API connector."""
    return HttpApiConnector(base_url=url, auth=auth, headers=headers, timeout=timeout)
