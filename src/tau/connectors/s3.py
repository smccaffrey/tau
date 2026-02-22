"""S3 connector — read/write data from S3-compatible storage."""

from __future__ import annotations
import json
import csv
import io
from typing import Any
from tau.connectors.base import Connector


class S3Connector(Connector):
    """Connect to S3 for reading and writing data files."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        aws_key: str | None = None,
        aws_secret: str | None = None,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
    ):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.region = region
        self.endpoint_url = endpoint_url
        self._client = None

    async def connect(self) -> None:
        try:
            import boto3
        except ImportError:
            raise ImportError("Install boto3: pip install boto3")

        kwargs = {"region_name": self.region}
        if self.aws_key and self.aws_secret:
            kwargs["aws_access_key_id"] = self.aws_key
            kwargs["aws_secret_access_key"] = self.aws_secret
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url

        self._client = boto3.client("s3", **kwargs)

    async def disconnect(self) -> None:
        self._client = None

    async def extract(
        self,
        key: str | None = None,
        format: str = "json",
        **kwargs,
    ) -> list[dict]:
        """Read a file from S3 and return as list of dicts."""
        if not self._client:
            await self.connect()

        full_key = f"{self.prefix}/{key}" if self.prefix and key else (key or self.prefix)
        response = self._client.get_object(Bucket=self.bucket, Key=full_key)
        body = response["Body"].read().decode("utf-8")

        if format == "json":
            data = json.loads(body)
            return data if isinstance(data, list) else [data]
        elif format == "jsonl":
            return [json.loads(line) for line in body.strip().split("\n") if line.strip()]
        elif format == "csv":
            reader = csv.DictReader(io.StringIO(body))
            return list(reader)
        else:
            raise ValueError(f"Unsupported format: {format}")

    async def load(
        self,
        data: list[dict],
        key: str | None = None,
        format: str = "json",
        **kwargs,
    ) -> int:
        """Write data to S3."""
        if not self._client or not data:
            return 0

        full_key = f"{self.prefix}/{key}" if self.prefix and key else (key or f"{self.prefix}/data.json")

        if format == "json":
            body = json.dumps(data, default=str)
        elif format == "jsonl":
            body = "\n".join(json.dumps(record, default=str) for record in data)
        elif format == "csv":
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            body = output.getvalue()
        else:
            raise ValueError(f"Unsupported format: {format}")

        self._client.put_object(Bucket=self.bucket, Key=full_key, Body=body.encode("utf-8"))
        return len(data)

    async def list_keys(self, prefix: str | None = None) -> list[str]:
        """List objects in the bucket."""
        if not self._client:
            await self.connect()

        search_prefix = prefix or self.prefix
        response = self._client.list_objects_v2(Bucket=self.bucket, Prefix=search_prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]


def s3(
    bucket: str,
    prefix: str = "",
    aws_key: str | None = None,
    aws_secret: str | None = None,
    region: str = "us-east-1",
    endpoint_url: str | None = None,
) -> S3Connector:
    """Create an S3 connector."""
    return S3Connector(
        bucket=bucket,
        prefix=prefix,
        aws_key=aws_key,
        aws_secret=aws_secret,
        region=region,
        endpoint_url=endpoint_url,
    )
