"""A pipeline that fails — for testing error handling and self-healing."""

from tau import pipeline, PipelineContext


@pipeline(
    name="failing_example",
    description="Intentionally fails to demonstrate error traces",
    tags=["example", "testing"],
)
async def failing_example(ctx: PipelineContext):
    ctx.log("Starting pipeline...")
    ctx.log("Extracting data from source...")

    data = [{"id": 1, "value": 100}, {"id": 2, "value": None}]
    ctx.log(f"Got {len(data)} records")

    # This will fail — simulates a data quality issue
    for record in data:
        if record["value"] is None:
            raise ValueError(f"Null value found in record {record['id']} — possible schema drift upstream")

    return {"processed": len(data)}
