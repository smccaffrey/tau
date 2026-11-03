"""Reports which machine ran it. Handy for checking that runs reach your workers."""

import asyncio
import socket

from tau import PipelineContext, pipeline


@pipeline(name="where_am_i", description="Return the hostname of the worker that ran it", retry=0)
async def where_am_i(ctx: PipelineContext):
    async with ctx.step("work") as step:
        await asyncio.sleep(float(ctx.params.get("seconds", 0)))
        step.rows_out = 1
    host = socket.gethostname()
    ctx.log(f"ran on {host}")
    return {"host": host}
