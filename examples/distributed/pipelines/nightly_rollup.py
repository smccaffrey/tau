"""A scheduled root with a dependent: the dependent runs after each successful root run."""

from tau import PipelineContext, pipeline


@pipeline(name="extract_events", schedule="*/5 * * * *", retry=2)
async def extract_events(ctx: PipelineContext):
    async with ctx.step("extract") as step:
        rows = [{"id": i} for i in range(100)]
        step.rows_out = len(rows)
    return {"rows": len(rows)}


@pipeline(name="rollup_events", depends_on=["extract_events"], retry=2)
async def rollup_events(ctx: PipelineContext):
    return {"since": ctx.last_successful_run.isoformat() if ctx.last_successful_run else None}
