"""Tau CLI — talks to the daemon over HTTP."""

import json
import sys
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


def _client() -> httpx.Client:
    settings = get_client_settings()
    return httpx.Client(
        base_url=settings.host,
        headers={"Authorization": f"Bearer {settings.api_key}"},
        timeout=120,
    )


def _api(method: str, path: str, **kwargs) -> dict:
    """Make an API call to the daemon."""
    with _client() as client:
        try:
            resp = client.request(method, f"/api/v1{path}", **kwargs)
        except httpx.ConnectError:
            settings = get_client_settings()
            console.print(f"[red]Error:[/red] Cannot connect to Tau daemon at {settings.host}")
            console.print("Start the daemon with: [bold]taud[/bold]")
            raise typer.Exit(1)

        if resp.status_code >= 400:
            detail = resp.json().get("detail", resp.text) if resp.headers.get("content-type", "").startswith("application/json") else resp.text
            console.print(f"[red]Error {resp.status_code}:[/red] {detail}")
            raise typer.Exit(1)

        return resp.json()


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
        console.print(f"[red]Error:[/red] File not found: {file}")
        raise typer.Exit(1)

    code = file.read_text()

    # Try to extract pipeline name from code
    if name is None:
        # Look for @pipeline(name="...") in the code
        import re
        match = re.search(r'@pipeline\([^)]*name\s*=\s*["\']([^"\']+)["\']', code)
        if match:
            name = match.group(1)
        else:
            name = file.stem

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
    console.print(f"[green]✓[/green] Deployed pipeline: [bold]{result['name']}[/bold]")
    if result.get("schedule_cron"):
        console.print(f"  Schedule: {result['schedule_cron']}")


@app.command()
def run(
    name: str = typer.Argument(..., help="Pipeline name to run"),
    params: Optional[str] = typer.Option(None, "--params", "-p", help="JSON params"),
):
    """Trigger a pipeline run."""
    parsed_params = json.loads(params) if params else None
    result = _api("POST", f"/pipelines/{name}/run", params={"params": params} if params else None)

    status = result["status"]
    color = "green" if status == "success" else "red" if status == "failed" else "yellow"

    console.print(f"\n[{color}]●[/{color}] {result['pipeline_name']} — {status}")
    if result.get("duration_ms"):
        console.print(f"  Duration: {result['duration_ms']}ms")
    if result.get("error"):
        console.print(f"  [red]Error:[/red] {result['error'][:200]}")
    if result.get("result"):
        console.print(f"  Result: {json.dumps(result['result'], indent=2)}")


@app.command(name="list")
def list_pipelines():
    """List all registered pipelines."""
    result = _api("GET", "/pipelines")
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
            p.get("schedule_cron") or "—",
            p.get("last_run_at", "—") or "—",
            f"[{ls_color}]{last_status}[/{ls_color}]",
        )

    console.print(table)


@app.command()
def show(name: str = typer.Argument(..., help="Pipeline name")):
    """Show pipeline details."""
    result = _api("GET", f"/pipelines/{name}")
    rprint(Panel(json.dumps(result, indent=2, default=str), title=f"Pipeline: {name}"))


@app.command()
def code(name: str = typer.Argument(..., help="Pipeline name")):
    """Show pipeline source code."""
    result = _api("GET", f"/pipelines/{name}/code")
    console.print(Syntax(result["code"], "python", theme="monokai", line_numbers=True))


@app.command()
def undeploy(name: str = typer.Argument(..., help="Pipeline name to remove")):
    """Remove a pipeline from the daemon."""
    result = _api("DELETE", f"/pipelines/{name}")
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

    if result.get("logs"):
        console.print(result["logs"])
    else:
        console.print("[dim]No logs available[/dim]")


@app.command()
def errors(limit: int = typer.Option(20, "--limit", "-l", help="Number of errors to show")):
    """Show recent failures across all pipelines."""
    result = _api("GET", f"/runs/errors?limit={limit}")
    runs = result["runs"]

    if not runs:
        console.print("[green]No recent errors[/green] 🎉")
        return

    for r in runs:
        console.print(f"\n[red]●[/red] {r['pipeline_name']} — {r.get('created_at', '')}")
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
        _api("POST", f"/pipelines/{name}/schedule")
        console.print(f"[yellow]Schedule disabled for {name}[/yellow]")
        return

    if not cron and not every:
        console.print("[red]Provide a cron expression or --every interval[/red]")
        raise typer.Exit(1)

    params = {}
    if cron:
        params["cron"] = cron
    if every:
        params["interval_seconds"] = every

    result = _api("POST", f"/pipelines/{name}/schedule", params=params)
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

    table = Table(title=f"Runs: {name}")
    table.add_column("ID", style="dim", max_width=8)
    table.add_column("Status")
    table.add_column("Trigger")
    table.add_column("Duration")
    table.add_column("Time")

    for r in result["runs"]:
        status = r["status"]
        color = "green" if status == "success" else "red" if status == "failed" else "yellow"
        duration = f"{r['duration_ms']}ms" if r.get("duration_ms") else "—"

        table.add_row(
            r["id"][:8],
            f"[{color}]{status}[/{color}]",
            r["trigger"],
            duration,
            r.get("created_at", "—") or "—",
        )

    console.print(table)


@app.command()
def version():
    """Show Tau version."""
    console.print(f"tau-pipelines v{__version__}")


@app.command()
def status():
    """Show daemon status."""
    with _client() as client:
        try:
            resp = client.get("/health")
            data = resp.json()
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
            console.print(f"[red]●[/red] Daemon not running at {settings.host}")


if __name__ == "__main__":
    app()
