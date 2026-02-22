"""DAG Pipeline — pipelines with dependencies.

Demonstrates:
- depends_on for declaring pipeline dependencies
- Tau runs upstream pipelines first, then downstream
- Pipelines in the same level run in parallel
"""

from tau import pipeline, PipelineContext
from tau.materializations import FullRefreshConfig, IncrementalConfig, SCDType2Config, MaterializationConfig, MaterializationType


# ─── Layer 1: Raw extraction (no dependencies, run in parallel) ───

@pipeline(
    name="extract_orders",
    description="Extract raw orders from source",
    schedule="0 4 * * *",
    tags=["raw", "extraction"],
)
async def extract_orders(ctx: PipelineContext):
    ctx.log("Extracting orders from source system")
    return {"rows": 10000}


@pipeline(
    name="extract_customers",
    description="Extract raw customers from source",
    schedule="0 4 * * *",
    tags=["raw", "extraction"],
)
async def extract_customers(ctx: PipelineContext):
    ctx.log("Extracting customers from source system")
    return {"rows": 5000}


@pipeline(
    name="extract_products",
    description="Extract raw products from source",
    schedule="0 4 * * *",
    tags=["raw", "extraction"],
)
async def extract_products(ctx: PipelineContext):
    ctx.log("Extracting products from source system")
    return {"rows": 500}


# ─── Layer 2: Staging (depends on raw) ───

@pipeline(
    name="stage_orders",
    description="Clean and stage orders",
    depends_on=["extract_orders"],
    tags=["staging"],
)
async def stage_orders(ctx: PipelineContext):
    ctx.log("Staging orders — dedup, clean, validate")
    return {"rows": 9500}


@pipeline(
    name="dim_customers",
    description="SCD2 customer dimension",
    depends_on=["extract_customers"],
    tags=["dimension", "scd2"],
)
async def dim_customers(ctx: PipelineContext):
    ctx.log("Building customer dimension with SCD2 tracking")
    return {"rows": 5000}


@pipeline(
    name="dim_products",
    description="Product dimension (full refresh)",
    depends_on=["extract_products"],
    tags=["dimension", "full-refresh"],
)
async def dim_products(ctx: PipelineContext):
    ctx.log("Rebuilding product dimension")
    return {"rows": 500}


# ─── Layer 3: Facts (depends on staging + dimensions) ───

@pipeline(
    name="fct_orders",
    description="Order fact table — joins orders with dimensions",
    depends_on=["stage_orders", "dim_customers", "dim_products"],
    tags=["fact", "incremental"],
)
async def fct_orders(ctx: PipelineContext):
    ctx.log("Building order fact table")
    return {"rows": 9500}


# ─── Layer 4: Reporting (depends on facts) ───

@pipeline(
    name="report_daily_revenue",
    description="Daily revenue report view",
    depends_on=["fct_orders"],
    tags=["reporting", "view"],
)
async def report_daily_revenue(ctx: PipelineContext):
    ctx.log("Creating daily revenue view")
    return {"view": "reporting.v_daily_revenue"}


# DAG visualization:
#
# extract_orders → stage_orders ──────→ fct_orders → report_daily_revenue
# extract_customers → dim_customers ──↗
# extract_products → dim_products ────↗
#
# Execution groups:
#   Group 1: [extract_orders, extract_customers, extract_products]  (parallel)
#   Group 2: [stage_orders, dim_customers, dim_products]            (parallel)
#   Group 3: [fct_orders]
#   Group 4: [report_daily_revenue]
