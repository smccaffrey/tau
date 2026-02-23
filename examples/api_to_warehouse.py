"""API to Warehouse — extract from an API, load to a database.

Demonstrates:
- Using named connections from tau.toml
- HTTP API connector for extraction
- PostgreSQL connector for loading
- Using connectors directly (without materializations)
- Step tracing with row counts

Configure connections in tau.toml:
[connections.api]
type = "http_api"
base_url = "https://api.example.com"
headers = { "Authorization" = "Bearer ${EXAMPLE_API_KEY}" }

[connections.warehouse]
type = "postgres"
dsn = "${WAREHOUSE_DSN}"
"""

from tau import pipeline, PipelineContext


@pipeline(
    name="api_to_warehouse",
    description="Pull user data from REST API and load to PostgreSQL",
    schedule="0 */6 * * *",  # Every 6 hours
    tags=["connector", "api", "postgres"],
)
async def api_to_warehouse(ctx: PipelineContext):
    # Get named connections from tau.toml
    source = await ctx.connection("api")
    target = await ctx.connection("warehouse")
    # Extract from API
    async with ctx.step("extract") as step:
        users = await source.extract(
            endpoint="/v1/users",
            params={"updated_since": "2026-01-01"},
        )
        step.rows_out = len(users)
        ctx.log(f"Extracted {len(users)} users from API")

    # Transform
    async with ctx.step("transform") as step:
        step.rows_in = len(users)
        cleaned = [
            {
                "user_id": u["id"],
                "email": u["email"].lower(),
                "name": u.get("name", "Unknown"),
                "plan": u.get("subscription", {}).get("plan", "free"),
                "created_at": u["created_at"],
            }
            for u in users
        ]
        step.rows_out = len(cleaned)

    # Load to warehouse
    async with ctx.step("load") as step:
        step.rows_in = len(cleaned)
        loaded = await target.load(
            data=cleaned,
            table="raw.users",
            mode="upsert",
            merge_key="user_id",
        )
        step.rows_out = loaded
        ctx.log(f"Loaded {loaded} rows to raw.users")

    return {"extracted": len(users), "loaded": loaded}
