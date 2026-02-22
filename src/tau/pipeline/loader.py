"""Load pipeline code from files and extract metadata."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from tau.pipeline.decorators import PipelineMetadata, clear_registry, get_registry


def load_pipeline_from_file(file_path: str | Path) -> list[PipelineMetadata]:
    """Load a Python file and return all @pipeline-decorated functions found."""
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Pipeline file not found: {file_path}")

    clear_registry()

    module_name = f"tau_pipeline_{file_path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    pipelines = list(get_registry().values())
    clear_registry()

    # Clean up
    del sys.modules[module_name]

    return pipelines


def extract_pipeline_metadata(file_path: str | Path) -> list[dict]:
    """Extract pipeline metadata without keeping the module loaded."""
    metas = load_pipeline_from_file(file_path)
    return [
        {
            "name": m.name,
            "description": m.description,
            "tags": m.tags,
        }
        for m in metas
    ]
