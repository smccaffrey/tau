"""Hello World — the simplest possible Tau pipeline."""

from tau import pipeline, PipelineContext


@pipeline(
    name="hello_world",
    description="A simple hello world pipeline that demonstrates core concepts",
    tags=["example", "getting-started"],
)
async def hello_world(ctx: PipelineContext):
    ctx.log("Hello from Tau! 🚀")

    # Extract some data
    data = [
        {"id": 1, "name": "Alice", "score": 95},
        {"id": 2, "name": "Bob", "score": 87},
        {"id": 3, "name": "Charlie", "score": 92},
    ]
    ctx.log(f"Extracted {len(data)} records")

    # Transform
    transformed = [
        {**r, "name": r["name"].upper(), "grade": "A" if r["score"] >= 90 else "B"}
        for r in data
    ]
    ctx.log(f"Transformed {len(transformed)} records")

    # Validate
    assert len(transformed) > 0, "No records to process"
    assert all(r["grade"] in ("A", "B") for r in transformed), "Invalid grades"
    ctx.log("All checks passed ✓")

    return {
        "records_processed": len(transformed),
        "grades": {r["name"]: r["grade"] for r in transformed},
    }
