"""Load pipeline code from files and extract metadata."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from tau.pipeline.decorators import clear_registry, get_registry


def load_pipeline_from_file(file_path: str | Path) -> list[dict]:
    """Load a Python file and return the metadata of every @pipeline function in it."""
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
            "name": m["name"],
            "description": m["description"],
            "tags": m["tags"],
        }
        for m in metas
    ]


def parse_pipeline_metadata(code: str) -> list[dict]:
    """Read ``@pipeline(...)`` arguments from source code without running it.

    Only literal arguments are read (strings, numbers, lists). Returns one dict per
    decorated function with the keys name, description, schedule, depends_on, retry,
    timeout_seconds and tags; keys whose argument is missing or not a literal are absent.
    Raises SyntaxError if the code does not parse.
    """
    import ast

    from tau.pipeline.decorators import _parse_timeout

    tree = ast.parse(code)
    found: list[dict] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            continue
        for deco in node.decorator_list:
            target = deco.func if isinstance(deco, ast.Call) else deco
            deco_name = getattr(target, "id", None) or getattr(target, "attr", None)
            if deco_name != "pipeline":
                continue
            meta: dict = {"name": node.name}
            docstring = ast.get_docstring(node)
            if docstring:
                meta["description"] = docstring
            for kw in deco.keywords if isinstance(deco, ast.Call) else []:
                try:
                    value = ast.literal_eval(kw.value)
                except ValueError:
                    continue
                if kw.arg == "timeout":
                    try:
                        meta["timeout_seconds"] = _parse_timeout(str(value))
                    except ValueError:
                        pass
                elif kw.arg in ("name", "description", "schedule", "depends_on", "retry", "tags"):
                    meta[kw.arg] = value
            found.append(meta)
    return found


def find_pipeline_metadata(code: str, name: str | None = None) -> dict:
    """Metadata of the pipeline called ``name`` in ``code`` (or the first one). {} if none."""
    try:
        metas = parse_pipeline_metadata(code)
    except SyntaxError:
        return {}
    if name is not None:
        for meta in metas:
            if meta.get("name") == name:
                return meta
    return metas[0] if metas else {}
