"""Dashboard — single-page web UI served by the daemon."""

from __future__ import annotations
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["dashboard"])

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tau Pipelines</title>
<style>
:root {
    --bg: #0d1117;
    --surface: #161b22;
    --border: #30363d;
    --text: #c9d1d9;
    --text-dim: #8b949e;
    --green: #3fb950;
    --red: #f85149;
    --amber: #d29922;
    --blue: #58a6ff;
    --cyan: #39d353;
    --font: 'SF Mono', 'Cascadia Code', 'JetBrains Mono', monospace;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { background:var(--bg); color:var(--text); font-family:var(--font); font-size:13px; }
.container { max-width:1200px; margin:0 auto; padding:24px; }
header { display:flex; justify-content:space-between; align-items:center; margin-bottom:24px; padding-bottom:16px; border-bottom:1px solid var(--border); }
header h1 { font-size:18px; color:var(--green); font-weight:600; }
header .version { color:var(--text-dim); font-size:12px; }
.stats { display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:12px; margin-bottom:24px; }
.stat { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:16px; }
.stat .label { color:var(--text-dim); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; }
.stat .value { font-size:24px; font-weight:700; margin-top:4px; }
.stat .value.green { color:var(--green); }
.stat .value.red { color:var(--red); }
.stat .value.amber { color:var(--amber); }
.stat .value.blue { color:var(--blue); }
.section { background:var(--surface); border:1px solid var(--border); border-radius:8px; margin-bottom:16px; }
.section-header { padding:12px 16px; border-bottom:1px solid var(--border); display:flex; justify-content:space-between; align-items:center; }
.section-header h2 { font-size:14px; font-weight:600; }
table { width:100%; border-collapse:collapse; }
th { text-align:left; padding:8px 16px; color:var(--text-dim); font-size:11px; text-transform:uppercase; letter-spacing:0.5px; border-bottom:1px solid var(--border); }
td { padding:10px 16px; border-bottom:1px solid var(--border); }
tr:last-child td { border-bottom:none; }
.badge { display:inline-block; padding:2px 8px; border-radius:10px; font-size:11px; font-weight:600; }
.badge.success { background:rgba(63,185,80,0.15); color:var(--green); }
.badge.failed { background:rgba(248,81,73,0.15); color:var(--red); }
.badge.running { background:rgba(88,166,255,0.15); color:var(--blue); }
.badge.pending { background:rgba(139,148,158,0.15); color:var(--text-dim); }
.badge.idle { background:rgba(63,185,80,0.15); color:var(--green); }
.badge.busy { background:rgba(210,153,34,0.15); color:var(--amber); }
.badge.offline { background:rgba(248,81,73,0.15); color:var(--red); }
.tag { display:inline-block; padding:1px 6px; border-radius:4px; font-size:10px; background:rgba(88,166,255,0.1); color:var(--blue); margin-right:4px; }
.empty { padding:24px; text-align:center; color:var(--text-dim); }
.refresh-btn { background:transparent; border:1px solid var(--border); color:var(--text-dim); padding:6px 12px; border-radius:6px; cursor:pointer; font-family:var(--font); font-size:12px; }
.refresh-btn:hover { border-color:var(--green); color:var(--green); }
.dag-viz { padding:16px; font-size:12px; }
.dag-group { display:flex; gap:8px; align-items:center; margin-bottom:8px; }
.dag-group .group-label { color:var(--text-dim); min-width:60px; }
.dag-node { background:rgba(88,166,255,0.1); border:1px solid rgba(88,166,255,0.3); padding:4px 10px; border-radius:4px; color:var(--blue); }
.dag-arrow { color:var(--text-dim); }
#error { color:var(--red); padding:16px; display:none; }
</style>
</head>
<body>
<div class="container">
    <header>
        <h1>τ Tau Pipelines</h1>
        <div>
            <span class="version" id="version"></span>
            <button class="refresh-btn" onclick="refresh()">↻ Refresh</button>
        </div>
    </header>
    <div id="error"></div>
    <div class="stats" id="stats"></div>
    <div class="section" id="pipelines-section">
        <div class="section-header"><h2>Pipelines</h2></div>
        <div id="pipelines"></div>
    </div>
    <div class="section" id="runs-section">
        <div class="section-header"><h2>Recent Runs</h2></div>
        <div id="runs"></div>
    </div>
    <div class="section" id="workers-section">
        <div class="section-header"><h2>Workers</h2></div>
        <div id="workers"></div>
    </div>
    <div class="section" id="dag-section" style="display:none">
        <div class="section-header"><h2>DAG</h2></div>
        <div class="dag-viz" id="dag"></div>
    </div>
</div>
<script>
const API_KEY = new URLSearchParams(window.location.search).get('key') || '';
const headers = API_KEY ? {'Authorization': `Bearer ${API_KEY}`} : {};

async function api(path) {
    const resp = await fetch(`/api/v1${path}`, {headers});
    if (!resp.ok) throw new Error(`${resp.status}`);
    return resp.json();
}

function badge(status) {
    const cls = {success:'success', failed:'failed', running:'running', error:'failed',
                 idle:'idle', busy:'busy', offline:'offline', active:'success'}[status] || 'pending';
    return `<span class="badge ${cls}">${status}</span>`;
}

function ago(ts) {
    if (!ts) return '—';
    const d = new Date(ts);
    const s = Math.floor((Date.now() - d) / 1000);
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.floor(s/60)}m ago`;
    if (s < 86400) return `${Math.floor(s/3600)}h ago`;
    return `${Math.floor(s/86400)}d ago`;
}

function dur(ms) {
    if (!ms) return '—';
    if (ms < 1000) return `${ms}ms`;
    return `${(ms/1000).toFixed(1)}s`;
}

async function refresh() {
    try {
        document.getElementById('error').style.display = 'none';
        const [health, pipelines] = await Promise.all([
            api('/../../health').catch(() => ({})),
            api('/pipelines'),
        ]);

        // Version
        document.getElementById('version').textContent = `v${health.version || '?'}`;

        // Stats
        const total = pipelines.total || 0;
        const active = (pipelines.pipelines || []).filter(p => p.status === 'active').length;
        const scheduled = (pipelines.pipelines || []).filter(p => p.schedule_cron || p.schedule_interval).length;
        const failing = (pipelines.pipelines || []).filter(p => p.last_run_status === 'failed').length;

        document.getElementById('stats').innerHTML = `
            <div class="stat"><div class="label">Pipelines</div><div class="value blue">${total}</div></div>
            <div class="stat"><div class="label">Active</div><div class="value green">${active}</div></div>
            <div class="stat"><div class="label">Scheduled</div><div class="value">${scheduled}</div></div>
            <div class="stat"><div class="label">Failing</div><div class="value ${failing?'red':''}">${failing}</div></div>
        `;

        // Pipelines table
        const pList = pipelines.pipelines || [];
        if (!pList.length) {
            document.getElementById('pipelines').innerHTML = '<div class="empty">No pipelines deployed</div>';
        } else {
            document.getElementById('pipelines').innerHTML = `<table>
                <tr><th>Name</th><th>Status</th><th>Schedule</th><th>Last Run</th><th>Tags</th></tr>
                ${pList.map(p => `<tr>
                    <td>${p.name}</td>
                    <td>${badge(p.status)}${p.last_run_status ? ' '+badge(p.last_run_status) : ''}</td>
                    <td>${p.schedule_cron || (p.schedule_interval ? `every ${p.schedule_interval}s` : '—')}</td>
                    <td>${ago(p.last_run_at)}</td>
                    <td>${(p.tags||[]).map(t => `<span class="tag">${t}</span>`).join('')}</td>
                </tr>`).join('')}
            </table>`;
        }

        // Recent runs
        try {
            const runsResp = await api('/runs?limit=20');
            const runs = runsResp.runs || runsResp || [];
            if (!runs.length) {
                document.getElementById('runs').innerHTML = '<div class="empty">No runs yet</div>';
            } else {
                document.getElementById('runs').innerHTML = `<table>
                    <tr><th>Pipeline</th><th>Status</th><th>Duration</th><th>Trigger</th><th>Finished</th></tr>
                    ${runs.map(r => `<tr>
                        <td>${r.pipeline_name}</td>
                        <td>${badge(r.status)}</td>
                        <td>${dur(r.duration_ms)}</td>
                        <td>${r.trigger || '—'}</td>
                        <td>${ago(r.finished_at)}</td>
                    </tr>`).join('')}
                </table>`;
            }
        } catch(e) {
            document.getElementById('runs').innerHTML = '<div class="empty">—</div>';
        }

        // Workers
        try {
            const workersResp = await api('/workers');
            const workers = workersResp.workers || [];
            if (!workers.length) {
                document.getElementById('workers').innerHTML = '<div class="empty">No workers registered</div>';
            } else {
                document.getElementById('workers').innerHTML = `<table>
                    <tr><th>ID</th><th>Type</th><th>Status</th><th>Load</th><th>Host</th></tr>
                    ${workers.map(w => `<tr>
                        <td>${w.worker_id}</td>
                        <td>${w.type}</td>
                        <td>${badge(w.status)}</td>
                        <td>${w.current_load}/${w.max_concurrent}</td>
                        <td>${w.host || 'local'}</td>
                    </tr>`).join('')}
                </table>`;
            }
        } catch(e) {
            document.getElementById('workers').innerHTML = '<div class="empty">Local mode (no worker API)</div>';
        }

    } catch(e) {
        document.getElementById('error').textContent = `Error: ${e.message}`;
        document.getElementById('error').style.display = 'block';
    }
}

refresh();
setInterval(refresh, 15000);
</script>
</body>
</html>"""


@router.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the dashboard."""
    return DASHBOARD_HTML


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_alt():
    """Alternate dashboard path."""
    return DASHBOARD_HTML
