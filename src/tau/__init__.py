"""Tau Pipelines — AI-native data pipeline orchestration."""

__version__ = "0.1.0"

from tau.pipeline.decorators import pipeline
from tau.pipeline.context import PipelineContext

__all__ = ["pipeline", "PipelineContext", "__version__"]
