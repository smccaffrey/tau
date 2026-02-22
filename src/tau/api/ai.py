"""AI API endpoints — generate and heal pipelines."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from tau.core.auth import verify_api_key
from tau.core.database import get_session
from tau.repositories.pipeline_repo import PipelineRepository
from tau.repositories.run_repo import RunRepository

router = APIRouter(prefix="/ai", tags=["ai"])


class GenerateRequest(BaseModel):
    intent: str
    model: str = "claude-sonnet-4-20250514"


class GenerateResponse(BaseModel):
    code: str
    intent: str


class HealRequest(BaseModel):
    pipeline_name: str
    auto_apply: bool = False
    model: str = "claude-sonnet-4-20250514"


@router.post("/generate", response_model=GenerateResponse)
async def generate_pipeline(
    data: GenerateRequest,
    _: str = Depends(verify_api_key),
):
    """Generate pipeline code from natural language."""
    from tau.ai.generator import generate_pipeline as _generate

    try:
        code = await _generate(intent=data.intent, model=data.model)
        return GenerateResponse(code=code, intent=data.intent)
    except Exception as e:
        raise HTTPException(500, f"AI generation failed: {str(e)}")


@router.post("/heal")
async def heal_pipeline(
    data: HealRequest,
    session: AsyncSession = Depends(get_session),
    _: str = Depends(verify_api_key),
):
    """Diagnose a pipeline failure and optionally auto-fix."""
    from tau.ai.generator import diagnose_failure

    pipe_repo = PipelineRepository(session)
    run_repo = RunRepository(session)

    pipeline = await pipe_repo.get_by_name(data.pipeline_name)
    if not pipeline:
        raise HTTPException(404, f"Pipeline '{data.pipeline_name}' not found")

    last_run = await run_repo.get_last_run(data.pipeline_name)
    if not last_run or last_run.status != "failed":
        return {"status": "healthy", "message": "Last run was not a failure"}

    try:
        diagnosis = await diagnose_failure(
            pipeline_name=data.pipeline_name,
            pipeline_code=pipeline.code,
            error=last_run.error or "",
            trace=last_run.trace,
            logs=last_run.logs,
            model=data.model,
        )
    except Exception as e:
        raise HTTPException(500, f"Diagnosis failed: {str(e)}")

    if data.auto_apply and diagnosis.get("fixed_code"):
        await pipe_repo.update(pipeline, code=diagnosis["fixed_code"])
        diagnosis["applied"] = True

    return diagnosis
