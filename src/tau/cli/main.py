"""Tau CLI — talks to the daemon over HTTP."""

import json
from pathlib import Path
from typing import Optional

import httpx
import typer
from rich.console import Console
from rich.table import Table
from rich.syntax import Syntax
from rich.panel import Panel
from rich import print as rprint

from tau import __version__
from tau.core.config import get_client_settings

app = typer.Typer(
    name="tau",
    help="AI-native data pipeline orchestration",
    no_args_is_help=True,
)
console = Console()
_state = {"json": False}


@app.callback()
def _main(
    json_output: bool = typer.Option(
        False, "--json", envvar="TAU_JSON",
        help="Print the raw JSON response of every command (for scripts and AI agents).",
    ),
):
    """AI-native data pipeline orchestration."""
    _state["json"] = json_output


def _emit(data) -> bool:
    """In --json mode print ``data`` as JSON and return True (the caller then stops)."""
    if _state["json"]:
        print(json.dumps(data, default=str, indent=2))
        return True
    return False


def _fail(message: str, status_code: int | None = None):
    if _state["json"]:
        print(json.dumps({"error": message, "status_code": status_code}))
    else:
        prefix = f"Error {status_code}" if status_code else "Error"
        console.print(f"[red]{prefix}:[/red] {message}")
    raise typer.Exit(1)


def _client(timeout: float | None = 120) -> httpx.Client:
    settings = get_client_settings()
    return httpx.Client(
        base_url=settings.host,
        headers={"Authorization": f"Bearer {settings.api_key}"},
        timeout=httpx.Timeout(timeout, connect=10.0),
        follow_redirects=True,
    )


def _api(method: str, path: str, timeout: float | None = 120, **kwargs) -> dict:
    """Make an API call to the daemon. ``timeout=None`` waits as long as it takes (runs)."""
    with _client(timeout) as client:
        try:
            resp = client.request(method, f"/api/v1{path}", **kwargs)
        except httpx.ConnectError:
            settings = get_client_settings()
            _fail(f"Cannot connect to Tau daemon at {settings.host}. Start it with: taud")

        if resp.status_code >= 400:
            detail = resp.json().get("detail", resp.text) if resp.headers.get("content-type", "").startswith("application/json") else resp.text
            _fail(str(detail), resp.status_code)

        return resp.json()


def _pipeline_name_from_code(code: str, fallback: str) -> str:
    from tau.pipeline.loader import find_pipeline_metadata

    return find_pipeline_metadata(code).get("name") or fallback


# ─── Pipeline Commands ───


@app.command()
def deploy(
    file: Path = typer.Argument(..., help="Pipeline Python file to deploy"),
    name: Optional[str] = typer.Option(None, "--name", "-n", help="Pipeline name (default: extracted from code)"),
    schedule: Optional[str] = typer.Option(None, "--schedule", "-s", help="Cron schedule (e.g. '0 6 * * *')"),
    description: Optional[str] = typer.Option(None, "--desc", "-d", help="Pipeline description"),
):
    """Deploy a pipeline to the daemon."""
    if not file.exists():
        _fail(f"File not found: {file}")

    code = file.read_text()

    # The daemon reads schedule, retry, timeout, tags and depends_on from the decorator.
    if name is None:
        name = _pipeline_name_from_code(code, file.stem)

    data = {
        "name": name,
        "code": code,
        "filename": file.name,
    }
    if schedule:
        data["schedule_cron"] = schedule
    if description:
        data["description"] = description

    result = _api("POST", "/pipelines", json=data)
    if _emit(result):
        return
    console.print(f"[green]✓[/green] Deployed pipeline: [bold]{result['name']}[/bold]")
    if result.get("schedule_cron"):
        console.print(f"  Schedule: {result['schedule_cron']}")
    if result.get("schedule_interval"):
        console.print(f"  Every: {result['schedule_interval']}s")
    if result.get("depends_on"):
        console.print(f"  Depends on: {', '.join(result['depends_on'])}")


@app.command()
def run(
    name: str = typer.Argument(..., help="Pipeline name to run"),
    params: Optional[str] = typer.Option(None, "--params", "-p", help="JSON params"),
    no_wait: bool = typer.Option(False, "--no-wait", help="Start the run and return its id"),
):
    """Run a pipeline and wait for the result (exit status 1 if it did not succeed)."""
    parsed_params = json.loads(params) if params else None
    if no_wait:
        result = _api("POST", f"/pipelines/{name}/run?wait=false", json=parsed_params)
        if _emit(result):
            return
        console.print(f"[yellow]●[/yellow] {name} — started run {result['id']}")
        console.print(f"  Follow it: tau runs {name}   Stop it: tau cancel {result['id']}")
        return
    result = _api("POST", f"/pipelines/{name}/run", json=parsed_params, timeout=None)
    status = result["status"]
    if _emit(result):
        raise typer.Exit(0 if status == "success" else 1)

    color = "green" if status == "success" else "red" if status == "failed" else "yellow"

    console.print(f"\n[{color}]●[/{color}] {result['pipeline_name']} — {status}")
    if result.get("duration_ms") is not None:
        console.print(f"  Duration: {result['duration_ms']}ms")
    trace = result.get("trace") or {}
    if trace.get("worker_id"):
        console.print(f"  Worker: {trace['worker_id']}")
    if (trace.get("attempts") or 1) > 1:
        console.print(f"  Attempts: {trace['attempts']}")
    if result.get("error"):
        console.print(f"  [red]Error:[/red] {result['error'][:200]}")
    if result.get("result"):
        console.print(f"  Result: {json.dumps(result['result'], indent=2)}")
    if status != "success":
        raise typer.Exit(1)


@app.command()
def cancel(run_id: str = typer.Argument(..., help="Run id (from `tau runs` or `tau run --no-wait`)")):
    """Cancel a queued or running pipeline run."""
    result = _api("POST", f"/runs/{run_id}/cancel")
    if _emit(result):
        return
    console.print(f"[yellow]●[/yellow] Run {run_id} — {result['status']}")


@app.command(name="list")
def list_pipelines():
    """List all registered pipelines."""
    result = _api("GET", "/pipelines")
    if _emit(result):
        return
    pipelines = result["pipelines"]

    if not pipelines:
        console.print("[dim]No pipelines deployed[/dim]")
        return

    table = Table(title="Pipelines", show_lines=False)
    table.add_column("Name", style="bold")
    table.add_column("Status")
    table.add_column("Schedule")
    table.add_column("Last Run")
    table.add_column("Last Status")

    for p in pipelines:
        status_color = "green" if p["status"] == "active" else "red"
        last_status = p.get("last_run_status") or "—"
        ls_color = "green" if last_status == "success" else "red" if last_status == "failed" else "dim"

        table.add_row(
            p["name"],
            f"[{status_color}]{p['status']}[/{status_color}]",
            p.get("schedule_cron") or (f"every {p['schedule_interval']}s" if p.get("schedule_interval") else "—"),
            p.get("last_run_at", "—") or "—",
            f"[{ls_color}]{last_status}[/{ls_color}]",
        )

    console.print(table)


@app.command()
def show(name: str = typer.Argument(..., help="Pipeline name")):
    """Show pipeline details."""
    result = _api("GET", f"/pipelines/{name}")
    if _emit(result):
        return
    rprint(Panel(json.dumps(result, indent=2, default=str), title=f"Pipeline: {name}"))


@app.command()
def code(name: str = typer.Argument(..., help="Pipeline name")):
    """Show pipeline source code."""
    result = _api("GET", f"/pipelines/{name}/code")
    if _emit(result):
        return
    console.print(Syntax(result["code"], "python", theme="monokai", line_numbers=True))


@app.command()
def undeploy(name: str = typer.Argument(..., help="Pipeline name to remove")):
    """Remove a pipeline from the daemon."""
    result = _api("DELETE", f"/pipelines/{name}")
    if _emit(result):
        return
    console.print(f"[green]✓[/green] Removed pipeline: [bold]{name}[/bold]")


@app.command()
def inspect(
    name: str = typer.Argument(..., help="Pipeline name"),
    last_run: bool = typer.Option(True, "--last-run", help="Show last run trace"),
):
    """Inspect pipeline execution trace (JSON — AI-optimized)."""
    result = _api("GET", f"/runs/{name}/last")

    # Output structured JSON for AI consumption
    output = {
        "run_id": result["id"],
        "pipeline": result["pipeline_name"],
        "status": result["status"],
        "trigger": result["trigger"],
        "started_at": result.get("started_at"),
        "finished_at": result.get("finished_at"),
        "duration_ms": result.get("duration_ms"),
        "result": result.get("result"),
        "error": result.get("error"),
        "trace": result.get("trace"),
    }
    if _emit(output):
        return
    console.print_json(json.dumps(output, default=str))


@app.command()
def logs(
    name: str = typer.Argument(..., help="Pipeline name"),
    run_id: Optional[str] = typer.Option(None, "--run", help="Specific run ID (default: latest)"),
):
    """Show logs for a pipeline run."""
    if run_id:
        result = _api("GET", f"/runs/{name}/{run_id}/logs")
    else:
        # Get latest run first
        run = _api("GET", f"/runs/{name}/last")
        result = _api("GET", f"/runs/{name}/{run['id']}/logs")

    if _emit(result):
        return
    if result.get("logs"):
        console.print(result["logs"])
    else:
        console.print("[dim]No logs available[/dim]")


@app.command()
def errors(limit: int = typer.Option(20, "--limit", "-l", help="Number of errors to show")):
    """Show recent failures across all pipelines."""
    result = _api("GET", f"/runs/errors?limit={limit}")
    if _emit(result):
        return
    runs = result["runs"]

    if not runs:
        console.print("[green]No recent errors[/green] 🎉")
        return

    for r in runs:
        console.print(f"\n[red]●[/red] {r['pipeline_name']} — {r['status']} — {r.get('created_at', '')}")
        if r.get("error"):
            console.print(f"  {r['error'][:200]}")


@app.command()
def schedule(
    name: str = typer.Argument(..., help="Pipeline name"),
    cron: Optional[str] = typer.Argument(None, help="Cron expression (e.g. '0 6 * * *')"),
    every: Optional[int] = typer.Option(None, "--every", help="Interval in seconds"),
    disable: bool = typer.Option(False, "--disable", help="Disable schedule"),
):
    """Set or update pipeline schedule."""
    if disable:
        result = _api("POST", f"/pipelines/{name}/schedule")
        if _emit(result):
            return
        console.print(f"[yellow]Schedule disabled for {name}[/yellow]")
        return

    if not cron and not every:
        _fail("Provide a cron expression or --every interval")
    if cron and every:
        _fail("Provide a cron expression or --every interval, not both")

    params = {}
    if cron:
        params["cron"] = cron
    if every:
        params["interval_seconds"] = every

    result = _api("POST", f"/pipelines/{name}/schedule", params=params)
    if _emit(result):
        return
    console.print(f"[green]✓[/green] Schedule updated for [bold]{name}[/bold]")
    if cron:
        console.print(f"  Cron: {cron}")
    if every:
        console.print(f"  Every: {every}s")


@app.command()
def runs(
    name: str = typer.Argument(..., help="Pipeline name"),
    last: int = typer.Option(10, "--last", "-l", help="Number of runs to show"),
):
    """Show recent runs for a pipeline."""
    result = _api("GET", f"/runs/{name}?limit={last}")
    if _emit(result):
        return

    table = Table(title=f"Runs: {name}")
    table.add_column("ID", style="dim", no_wrap=True)
    table.add_column("Status")
    table.add_column("Trigger")
    table.add_column("Worker")
    table.add_column("Duration")
    table.add_column("Time")

    for r in result["runs"]:
        status = r["status"]
        color = "green" if status == "success" else "red" if status == "failed" else "yellow"
        duration = f"{r['duration_ms']}ms" if r.get("duration_ms") is not None else "—"

        table.add_row(
            r["id"],
            f"[{color}]{status}[/{color}]",
            r["trigger"],
            r.get("worker_id") or "—",
            duration,
            r.get("created_at", "—") or "—",
        )

    console.print(table)


@app.command()
def create(
    intent: str = typer.Argument(..., help="Natural language description of the pipeline"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file (default: stdout)"),
    deploy_now: bool = typer.Option(False, "--deploy", help="Deploy immediately after generation"),
    schedule: Optional[str] = typer.Option(None, "--schedule", "-s", help="Schedule if deploying"),
):
    """Generate a pipeline from natural language using AI."""
    import asyncio

    async def _create():
        from tau.ai.generator import generate_pipeline

        with console.status("[bold]Generating pipeline..."):
            code = await generate_pipeline(intent)

        if output:
            Path(output).write_text(code)
            console.print(f"[green]✓[/green] Pipeline saved to [bold]{output}[/bold]")
        else:
            console.print(Syntax(code, "python", theme="monokai", line_numbers=True))

        if deploy_now:
            # Extract name from generated code
            name = _pipeline_name_from_code(code, "generated_pipeline")

            data = {"name": name, "code": code}
            if schedule:
                data["schedule_cron"] = schedule

            result = _api("POST", "/pipelines", json=data)
            console.print(f"[green]✓[/green] Deployed: [bold]{result['name']}[/bold]")

    asyncio.run(_create())


@app.command()
def heal(
    name: str = typer.Argument(..., help="Pipeline name to diagnose/heal"),
    auto: bool = typer.Option(False, "--auto", help="Auto-apply fix and redeploy"),
):
    """Diagnose a pipeline failure and suggest/apply a fix."""
    import asyncio

    async def _heal():
        from tau.ai.generator import diagnose_failure

        # Get pipeline code
        pipe = _api("GET", f"/pipelines/{name}/code")
        code = pipe["code"]

        # Get last failed run
        try:
            run = _api("GET", f"/runs/{name}/last")
        except typer.Exit:
            console.print("[red]No runs found to diagnose[/red]")
            return

        if run["status"] not in ("failed", "timeout"):
            console.print(f"[green]Last run was {run['status']}[/green] — nothing to heal")
            return

        with console.status("[bold]Diagnosing failure..."):
            diagnosis = await diagnose_failure(
                pipeline_name=name,
                pipeline_code=code,
                error=run.get("error", ""),
                trace=run.get("trace"),
                logs=_api("GET", f"/runs/{name}/{run['id']}/logs").get("logs"),
            )

        console.print(f"\n[bold]Diagnosis:[/bold] {diagnosis.get('diagnosis', 'Unknown')}")
        console.print(f"[bold]Root Cause:[/bold] {diagnosis.get('root_cause', 'unknown')}")
        console.print(f"[bold]Severity:[/bold] {diagnosis.get('severity', 'unknown')}")
        console.print(f"[bold]Confidence:[/bold] {diagnosis.get('confidence', 0):.0%}")

        if diagnosis.get("fix_description"):
            console.print(f"\n[bold]Fix:[/bold] {diagnosis['fix_description']}")

        if diagnosis.get("fixed_code"):
            console.print("\n[bold]Fixed Code:[/bold]")
            console.print(Syntax(diagnosis["fixed_code"], "python", theme="monokai"))

            if auto:
                _api("POST", "/pipelines", json={
                    "name": name,
                    "code": diagnosis["fixed_code"],
                })
                console.print(f"\n[green]✓[/green] Fix deployed for [bold]{name}[/bold]")

                # Run it to verify
                console.print("Running healed pipeline...")
                run_result = _api("POST", f"/pipelines/{name}/run", timeout=None)
                status = run_result["status"]
                color = "green" if status == "success" else "red"
                console.print(f"[{color}]●[/{color}] Result: {status}")
            else:
                console.print("\n[dim]Run with --auto to apply fix automatically[/dim]")
        else:
            console.print("\n[yellow]Could not generate an automatic fix[/yellow]")

    asyncio.run(_heal())


# ─── DAG Commands ───

@app.command()
def dag(
    run: bool = typer.Option(False, "--run", help="Run the DAG in dependency order"),
    root: Optional[str] = typer.Option(None, "--from", help="With --run: only this pipeline and its downstream"),
):
    """Show the pipeline dependency DAG, or run it with --run."""
    if run:
        params = {"root": root} if root else {}
        result = _api("POST", "/dag/run", params=params, timeout=None)
        ok = result["status"] == "success"
        if _emit(result):
            raise typer.Exit(0 if ok else 1)
        color = "green" if ok else "red"
        console.print(f"[{color}]●[/{color}] DAG — {result['status']} ({result.get('duration_ms')}ms)")
        for i, group in enumerate(result.get("execution_order", [])):
            cells = []
            for n in group:
                r = result["results"].get(n)
                if r is None:
                    cells.append(f"[dim]{n} (skipped)[/dim]")
                else:
                    c = "green" if r.get("status") == "success" else "red"
                    cells.append(f"[{c}]{n} ({r.get('status')})[/{c}]")
            console.print(f"  Level {i}: " + "  ".join(cells))
        raise typer.Exit(0 if ok else 1)

    result = _api("GET", "/dag")
    if _emit(result):
        return

    if not result.get("nodes"):
        console.print("[dim]No dependencies defined[/dim]")
        return

    console.print("[bold]Pipeline DAG[/bold]\n")

    groups = result.get("groups", [])
    for i, group in enumerate(groups):
        level = f"Level {i}"
        nodes = "  ".join(f"[cyan]{n}[/cyan]" for n in group)
        console.print(f"  {level}: {nodes}")

    if result.get("edges"):
        console.print(f"\n  [dim]{len(result['edges'])} dependencies, {len(result['nodes'])} pipelines[/dim]")


@app.command()
def depends(
    name: str = typer.Argument(..., help="Pipeline name"),
    on: list[str] = typer.Option([], "--on", help="Upstream dependencies (repeatable)"),
):
    """Set pipeline dependencies (e.g. tau depends fct_orders --on dim_customers --on stg_orders)."""
    if not on:
        # Show current dependencies
        result = _api("GET", f"/dag/{name}/upstream")
        upstream = result.get("upstream", [])
        down = _api("GET", f"/dag/{name}/downstream")
        if _emit({"pipeline": name, "upstream": upstream, "downstream": down.get("downstream", [])}):
            return
        if upstream:
            console.print(f"[bold]{name}[/bold] depends on: {', '.join(upstream)}")
        else:
            console.print(f"[bold]{name}[/bold] has no dependencies")

        result = _api("GET", f"/dag/{name}/downstream")
        downstream = result.get("downstream", [])
        if downstream:
            console.print(f"  Downstream: {', '.join(downstream)}")
        return

    result = _api("POST", "/dag/dependencies", json={
        "pipeline": name,
        "depends_on": on,
    })
    if _emit(result):
        return
    console.print(f"[green]✓[/green] {name} depends on: {', '.join(on)}")


# ─── Connection Commands ───

@app.command()
def connections(
    test: Optional[str] = typer.Option(None, "--test", help="Test a specific connection"),
):
    """List configured connections or test a specific connection."""
    if test:
        # Test specific connection
        result = _api("POST", f"/connections/{test}/test")
        if _emit(result):
            raise typer.Exit(0 if result.get("success") else 1)
        status = "✓" if result.get("success") else "✗"
        color = "green" if result.get("success") else "red"
        message = result.get("message", "")
        console.print(f"[{color}]{status}[/{color}] {test} — {message}")
        raise typer.Exit(0 if result.get("success") else 1)

    # List all connections
    result = _api("GET", "/connections")
    if _emit(result):
        return
    connections_list = result.get("connections", {})

    if not connections_list:
        console.print("[dim]No connections configured[/dim]")
        console.print("Configure connections in tau.toml:")
        console.print()
        console.print("[dim]  \\[connections.warehouse][/dim]")
        console.print("[dim]  type = \"postgres\"[/dim]")
        console.print("[dim]  dsn = \"postgresql://user:pass@localhost/db\"[/dim]")
        return

    table = Table(title="Configured Connections")
    table.add_column("Name", style="bold cyan")
    table.add_column("Type", style="yellow")

    for name, conn_type in connections_list.items():
        table.add_row(name, conn_type)

    console.print(table)
    console.print(f"\n  {len(connections_list)} connections configured")
    console.print("  Use [bold]tau connections --test <name>[/bold] to test connectivity")


# ─── Worker Commands ───

@app.command()
def workers():
    """Show worker pool status."""
    result = _api("GET", "/workers")
    if _emit(result):
        return
    workers_list = result.get("workers", [])

    if not workers_list:
        console.print("[dim]No workers registered[/dim]")
        return

    table = Table(title="Workers")
    table.add_column("ID", style="bold")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Load")
    table.add_column("Host")

    for w in workers_list:
        status = w["status"]
        color = "green" if status == "idle" else "yellow" if status == "busy" else "red"
        table.add_row(
            w["worker_id"],
            w["type"],
            f"[{color}]{status}[/{color}]",
            f"{w['current_load'] - w.get('queued', 0)}/{w['max_concurrent']}"
            + (f" (+{w['queued']} queued)" if w.get("queued") else ""),
            w.get("host", "local"),
        )

    console.print(table)
    console.print(f"\n  Total capacity: {result.get('total_capacity', 0)}")
    console.print(f"  Current load: {result.get('total_load', 0)}")


@app.command()
def dashboard():
    """Open the web dashboard URL."""
    settings = get_client_settings()
    url = f"{settings.host}/"
    console.print(f"Dashboard: [cyan]{url}[/cyan]")
    try:
        import webbrowser
        webbrowser.open(url)
    except Exception:
        pass


@app.command()
def version():
    """Show Tau version."""
    if _emit({"version": __version__}):
        return
    console.print(f"tau-pipelines v{__version__}")


@app.command()
def status():
    """Show daemon status."""
    with _client() as client:
        try:
            resp = client.get("/health")
            data = resp.json()
            if _emit(data):
                return
            console.print(f"[green]●[/green] Tau daemon v{data['version']} — running")
            jobs = data.get("scheduler_jobs", [])
            if jobs:
                console.print(f"  Scheduled jobs: {len(jobs)}")
                for j in jobs:
                    console.print(f"    {j['id']} → next: {j.get('next_run', '—')}")
            else:
                console.print("  No scheduled jobs")
        except httpx.ConnectError:
            settings = get_client_settings()
            if _state["json"]:
                print(json.dumps({"status": "unreachable", "host": settings.host}))
            else:
                console.print(f"[red]●[/red] Daemon not running at {settings.host}")
            raise typer.Exit(1)


if __name__ == "__main__":
    app()
