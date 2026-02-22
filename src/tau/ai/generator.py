"""AI pipeline generator — create pipelines from natural language intent."""

from __future__ import annotations
import json
import os
from typing import Optional

import httpx


SYSTEM_PROMPT = """You are Tau, an AI pipeline code generator. You write Python data pipelines using the Tau framework.

## Tau Pipeline API

```python
from tau import pipeline, PipelineContext

@pipeline(
    name="pipeline_name",           # Required: unique name
    description="What it does",     # Optional
    schedule="0 6 * * *",           # Optional: cron expression
    retry=3,                        # Optional: retry count
    timeout="30m",                  # Optional: timeout
    tags=["tag1", "tag2"],          # Optional
)
async def pipeline_name(ctx: PipelineContext):
    # Logging
    ctx.log("message")

    # Extract data
    data = await ctx.extract(source="source_name", resource="resource")

    # Transform
    transformed = await ctx.transform(data, steps=[...])

    # Load to target
    await ctx.load(target="schema.table", data=transformed, mode="incremental", merge_key="id")

    # Run SQL in warehouse
    await ctx.sql("CREATE TABLE ... AS SELECT ...", params={"key": "value"})

    # Assertions
    await ctx.check(len(data) > 0, ...)

    # Access secrets from environment
    api_key = ctx.secret("API_KEY")

    # Previous successful run timestamp
    since = ctx.last_successful_run

    # Step tracing
    async with ctx.step("step_name") as step:
        step.rows_out = 100
        # ... do work ...

    # Return result dict
    return {"records_processed": len(data)}
```

## Available Connectors (import from tau.connectors)
- `postgres(dsn=ctx.secret("DB_URL"))` — PostgreSQL connector
- `http_api(url="...", auth={"type": "bearer", "token": ctx.secret("TOKEN")})` — Generic HTTP API
- `s3(bucket="...", prefix="...", aws_key=ctx.secret("AWS_KEY"), aws_secret=ctx.secret("AWS_SECRET"))` — S3 connector

## Rules
1. Output ONLY valid Python code — no markdown, no explanation
2. Use async/await everywhere
3. Use ctx.log() for visibility
4. Use ctx.secret() for credentials (never hardcode)
5. Use ctx.step() for important sections (creates execution traces)
6. Always include error handling and assertions
7. Name the pipeline descriptively based on what it does
8. Include a schedule if the intent implies regular execution
9. Return a result dict summarizing what happened
"""


async def generate_pipeline(
    intent: str,
    model: str = "claude-sonnet-4-20250514",
    api_key: str | None = None,
) -> str:
    """Generate pipeline code from natural language intent."""

    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("TAU_AI_API_KEY")
    if not api_key:
        raise ValueError(
            "No AI API key found. Set ANTHROPIC_API_KEY or TAU_AI_API_KEY environment variable."
        )

    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 4096,
                "system": SYSTEM_PROMPT,
                "messages": [
                    {
                        "role": "user",
                        "content": f"Generate a Tau pipeline for: {intent}",
                    }
                ],
            },
        )

        if response.status_code != 200:
            raise RuntimeError(f"AI API error {response.status_code}: {response.text}")

        data = response.json()
        code = data["content"][0]["text"]

        # Strip markdown code fences if present
        if code.startswith("```"):
            lines = code.split("\n")
            # Remove first and last lines (```python and ```)
            lines = [l for l in lines if not l.strip().startswith("```")]
            code = "\n".join(lines)

        return code.strip()


async def diagnose_failure(
    pipeline_name: str,
    pipeline_code: str,
    error: str,
    trace: dict | None = None,
    logs: str | None = None,
    model: str = "claude-sonnet-4-20250514",
    api_key: str | None = None,
) -> dict:
    """Diagnose a pipeline failure and suggest a fix."""

    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("TAU_AI_API_KEY")
    if not api_key:
        raise ValueError("No AI API key found.")

    diagnosis_prompt = f"""A Tau pipeline failed. Diagnose the issue and provide a fix.

## Pipeline: {pipeline_name}

## Code:
```python
{pipeline_code}
```

## Error:
{error}

## Execution Trace:
{json.dumps(trace, indent=2, default=str) if trace else "No trace available"}

## Logs:
{logs or "No logs available"}

## Your Response (JSON):
Respond with ONLY a JSON object:
{{
    "diagnosis": "Brief explanation of what went wrong",
    "root_cause": "schema_drift|api_change|data_quality|infra|auth|code_bug|unknown",
    "severity": "low|medium|high|critical",
    "fix_description": "What the fix does",
    "fixed_code": "Complete fixed pipeline code (full file, not a diff)",
    "confidence": 0.0-1.0
}}
"""

    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 4096,
                "system": "You are an expert data pipeline debugger. Always respond with valid JSON only.",
                "messages": [{"role": "user", "content": diagnosis_prompt}],
            },
        )

        if response.status_code != 200:
            raise RuntimeError(f"AI API error {response.status_code}: {response.text}")

        data = response.json()
        text = data["content"][0]["text"]

        # Parse JSON from response (handle markdown fences)
        if "```" in text:
            lines = text.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            text = "\n".join(lines)

        try:
            return json.loads(text.strip())
        except json.JSONDecodeError:
            return {
                "diagnosis": text,
                "root_cause": "unknown",
                "severity": "medium",
                "fix_description": "Could not auto-generate fix",
                "fixed_code": None,
                "confidence": 0.0,
            }
