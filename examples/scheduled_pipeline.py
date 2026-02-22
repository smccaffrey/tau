"""A pipeline with a built-in schedule."""

from tau import pipeline, PipelineContext
from datetime import datetime


@pipeline(
    name="hourly_check",
    description="Runs every hour to check data freshness",
    schedule="0 * * * *",
    tags=["monitoring", "scheduled"],
)
async def hourly_check(ctx: PipelineContext):
    ctx.log(f"Running hourly check at {datetime.utcnow().isoformat()}")

    if ctx.last_successful_run:
        ctx.log(f"Last successful run: {ctx.last_successful_run.isoformat()}")
    else:
        ctx.log("First run — no previous history")

    # Simulate checking data freshness
    tables = ["orders", "customers", "products"]
    results = {}
    for table in tables:
        # In a real pipeline, this would query the warehouse
        results[table] = {"status": "fresh", "last_updated": datetime.utcnow().isoformat()}
        ctx.log(f"  {table}: fresh ✓")

    return {"tables_checked": len(tables), "results": results}
