"""dashboard.py — single-page live dashboard for Polymarket maker_system Phase 1A.

Serves on http://localhost:8000; auto-refreshes every 5s.
Shows: top-N candidates, recent placements & fills, latency p50/p95/p99,
rotation events, raw event rate, run summary, and log milestones.

Run:
    python3 dashboard.py  (or nohup python3 -u dashboard.py > dashboard.log 2>&1 &)
Reading sources:
    state/candidates_ranked.parquet   — top-N candidates by activity_score (Loop A scan output)
    state/ledger.parquet              — per-fill rows (Per-fill ledger per Contract 4)
    state/raw_events.jsonl            — count-of-lines indicates WS throughput rate
    state/latency_summary.json        — p50/p95/p99 ws_detect + ws_apply
    state/phase1a_run_summary.json    — final run recap; if running, this file may not exist yet
    state/phase1a_*rotating.log       — last log file; milestone lines (rotation / placed / router tick) shown
"""
from __future__ import annotations
import http.server
import json
import os
import re
import socketserver
import time
from pathlib import Path

import pyarrow.parquet as pq

PORT = int(os.environ.get("DASH_PORT", "8000"))
STATE_DIR = Path(os.environ.get(
    "DASH_STATE_DIR",
    "/home/ubuntu/polymarket_research/maker_system/state",
))
LOG_FILE_GLOB = "phase1a_*rotating.log"


def gather_state() -> dict:
    """Read all state files and produce a single JSON snapshot."""
    out: dict = {}
    if not STATE_DIR.exists():
        out["error"] = f"state dir not found: {STATE_DIR}"
        return out

    out["dir"] = str(STATE_DIR)
    out["ts_utc_ms"] = int(time.time() * 1000)

    # candidates_ranked.parquet — top 20 by activity_score
    cand_path = STATE_DIR / "candidates_ranked.parquet"
    try:
        if cand_path.exists():
            table = pq.read_table(cand_path).to_pylist()
            out["candidates_top20"] = [
                {
                    "rank": i + 1,
                    "asset_id": (r.get("asset_id") or "")[:14] + "...",
                    "question": (r.get("market_question") or "")[:60],
                    "event_title": (r.get("event_title") or "")[:40],
                    "event_vol24h": float(r.get("event_vol24h") or 0.0),
                    "spread_c": float(r.get("spread_c") or 0.0),
                    "inside_depth_usd": float(r.get("inside_depth_usd") or 0.0),
                    "enriched_score": float(r.get("enriched_score") or 0.0),
                    "ws_activity_count": int(r.get("ws_activity_count") or 0),
                    "activity_score": float(r.get("activity_score") or 0.0),
                    "topic_key": r.get("topic_key") or "",
                }
                for i, r in enumerate(table[:20])
            ]
            out["candidates_total_rows"] = len(table)
        else:
            out["candidates_top20"] = []
    except Exception as e:
        out["candidates_top20_error"] = str(e)

    # ledger.parquet — last 10 fills
    ledger_path = STATE_DIR / "ledger.parquet"
    try:
        if ledger_path.exists():
            table = pq.read_table(ledger_path).to_pylist()
            out["fills_last_10"] = [
                {
                    "ts_utc": r.get("ts_utc"),
                    "asset_id": (r.get("asset_id") or "")[:14] + "...",
                    "side_taker": r.get("side_taker"),
                    "exec_price": r.get("exec_price"),
                    "exec_qty": round(float(r.get("exec_qty") or 0.0), 3),
                    "qpc": r.get("queue_position_best_case"),
                    "qpe": r.get("queue_position_expected"),
                    "qpw": r.get("queue_position_worst_case"),
                    "gross_edge": round(float(r.get("gross_edge_at_fill") or 0.0), 5),
                    "expected_pnl": round(float(r.get("expected_pnl_per_fill") or 0.0), 5),
                    "pnl_best": round(float(r.get("pnl_best_case") or 0.0), 5),
                    "pnl_worst": round(float(r.get("pnl_worst_case") or 0.0), 5),
                    "markout_60s": r.get("markout_60s"),
                }
                for r in table[-10:]
            ]
            out["fills_total"] = len(table)
        else:
            out["fills_last_10"] = []
            out["fills_total"] = 0
    except Exception as e:
        out["ledger_error"] = str(e)

    # raw_events.jsonl line count
    raw_path = STATE_DIR / "raw_events.jsonl"
    try:
        if raw_path.exists():
            with open(raw_path, "r") as f:
                line_count = sum(1 for _ in f)
            out["raw_events_total"] = line_count
            out["raw_events_size_bytes"] = raw_path.stat().st_size
            out["raw_events_mtime"] = raw_path.stat().st_mtime
        else:
            out["raw_events_total"] = 0
    except Exception as e:
        out["raw_error"] = str(e)

    # latency_summary.json
    lat_path = STATE_DIR / "latency_summary.json"
    try:
        if lat_path.exists():
            out["latency"] = json.loads(lat_path.read_text())
        else:
            out["latency"] = None
    except Exception as e:
        out["latency_error"] = str(e)

    # phase1a_run_summary.json (may not exist if capture still running)
    run_path = STATE_DIR / "phase1a_run_summary.json"
    try:
        if run_path.exists():
            out["run_summary"] = json.loads(run_path.read_text())
        else:
            out["run_summary"] = None
    except Exception:
        out["run_summary"] = None

    # Milestones from the LIVE log file (latest phase1a_*rotating.log)
    milestone_re = re.compile(
        r"(rotation [0-9]\d*:|scan cycle [0-9]+:|paper_executor: placed|"
        r"capture period complete|backfilled|flushed|capture live|^.*WS connected|"
        r"router tick.*emitted=|Loop A discovery)"
    )
    milestone_lines: list[str] = []
    try:
        log_files = sorted(STATE_DIR.glob(LOG_FILE_GLOB))
        if log_files:
            log_path = log_files[-1]
            content = log_path.read_text()[-100_000:]
            for line in content.splitlines():
                if milestone_re.search(line):
                    milestone_lines.append(line.strip())
            milestone_lines = milestone_lines[-30:]
            out["log_file"] = str(log_path)
        out["milestones"] = milestone_lines
    except Exception as e:
        out["milestone_error"] = str(e)

    return out


PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>Polymarket Namespace</title>
<style>
  body { font-family: -apple-system, SF Mono, monospace; background: #0c0e16; color: #e0e0e0; padding: 16px; margin: 0; }
  h1 { font-size: 18px; margin: 0 0 8px; color: #61dafb; }
  h2 { font-size: 14px; margin: 12px 0 4px; color: #adbac7; border-bottom: 1px solid #2a3240; padding-bottom: 2px; }
  .cards { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 8px; }
  .card { background: #1a1f2b; padding: 8px 12px; border: 1px solid #2a3240; border-radius: 4px; min-width: 80px; }
  .card .label { font-size: 11px; color: #777e88; }
  .card .value { font-size: 18px; font-weight: bold; color: #61dafb; }
  table { width: 100%; border-collapse: collapse; font-size: 11px; }
  th, td { padding: 4px 6px; text-align: left; border-bottom: 1px solid #2a3240; }
  th { color: #777e88; font-weight: normal; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .panel { background: #13171f; padding: 8px 10px; border-radius: 4px; }
  .log { font-family: SF Mono, ui-monospace, monospace; font-size: 10px; line-height: 14px; max-height: 380px; overflow-y: auto; background: #0c0e16; padding: 8px; }
  .log-line { white-space: pre; color: #b3cdd9; }
  .timestamp { color: #4ade80; }
  .small { font-size: 10px; color: #6e7681; }
  .ok { color: #62e2a3; } .warn { color: #f6c177; } .err { color: #e96a5e; }
</style>
</head>
<body>
<h1>Polymarket Phase 1A live dashboard — periodic re-discovery + WS rotation</h1>
<div class="small" id="dir">...</div>

<div class="cards" id="cards"></div>

<div class="grid">
  <div class="panel">
    <h2>top-30 candidates by activity_score (candidates_ranked.parquet)</h2>
    <div id="candidates"></div>
  </div>
  <div class="panel">
    <h2>fills ledger (last 10 rows of ledger.parquet — Contract 4 schema)</h2>
    <div id="fills"></div>
  </div>
</div>

<div class="panel" style="margin: 12px 0;">
  <h2>log milestones (rotation events | placements | router ticks | scan cycles)</h2>
  <div class="log" id="milestones"></div>
</div>

<script>
function fmt(v, p=2) { return (v === null || v === undefined) ? 'n/a' : (typeof v === 'number' ? v.toFixed(p) : v); }
function shortAsset(s) { return s && s.length > 16 ? s.slice(0,14) + '...' : s; }

function renderCards(s) {
  const cards = [];
  const rs = s.run_summary || {};
  const stats_rec = (rs.stats_rec !== undefined) ? rs.stats_rec : {};
  cards.push({label: 'mode', value: rs.mode || 'n/a'});
  cards.push({label: 'raw events', value: s.raw_events_total || 0});
  cards.push({label: 'candidates rows', value: s.candidates_total_rows ?? '-'});
  cards.push({label: 'scans/count', value: stats_rec.scan_count ?? '0'});
  cards.push({label: 'rotations', value: stats_rec.rotation_count ?? '0'});
  cards.push({label: 'submits', value: stats_rec.quote_submits_total ?? '-'});
  cards.push({label: 'fills', value: s.fills_total ?? '-'});
  const wsDetect = (s.latency && s.latency.ws_detect_ms) || {};
  const wsApply = (s.latency && s.latency.ws_apply_ms) || {};
  cards.push({label: 'ws_detect p50', value: wsDetect.p50 ?? '-'});
  cards.push({label: 'ws_detect p99', value: wsDetect.p99 ?? '-'});
  cards.push({label: 'ws_apply p50', value: wsApply.p50 ?? '-'});
  return cards.map(c => `<div class="card"><div class="label">${c.label}</div><div class="value">${c.value}</div></div>`).join('');
}

function renderCandidates(rows) {
  if (!rows || rows.length === 0) return '<div class="small">No candidates yet (scanner may still be starting up)</div>';
  const header = '<tr><th>rank</th><th>asset_id</th><th>market</th><th>event_vol</th><th>spread_c</th><th>depth</th><th>ws_msgs/15s</th><th>activity_score</th><th>topic</th></tr>';
  const body = rows.map(r => `<tr>
    <td>${r.rank || '-'}</td>
    <td>${r.asset_id}</td>
    <td>${(r.question || '').slice(0,38)}</td>
    <td>$${Math.round(r.event_vol24h || 0)}</td>
    <td>${(r.spread_c || 0).toFixed(2)}c</td>
    <td>$${(r.inside_depth_usd || 0).toFixed(1)}</td>
    <td>${r.ws_activity_count}</td>
    <td>${Math.round(r.activity_score).toLocaleString()}</td>
    <td>${r.topic_key}</td>
  </tr>`).join('');
  return `<table>${header}${body}</table>`;
}

function renderFills(rows) {
  if (!rows || rows.length === 0) return '<div class="small">No fills yet (router may have placed quotes but no price-changes shrunk at our quote level)</div>';
  const header = '<tr><th>ts_utc</th><th>asset_id</th><th>side</th><th>exec_price</th><th>exec_qty</th><th>qpb</th><th>qpe</th><th>qpw</th><th>gross_edge</th><th>expected_pnl</th><th>pnl_best</th><th>pnl_worst</th><th>markout_60s</th></tr>';
  const body = rows.map(r => `<tr>
    <td>${r.ts_utc || '-'}</td>
    <td>${r.asset_id}</td>
    <td>${r.side_taker}</td>
    <td>${(r.exec_price ?? 0).toFixed(3)}</td>
    <td>${r.exec_qty}</td>
    <td>${r.qpc}</td>
    <td>${r.qpe}</td>
    <td>${r.qpw}</td>
    <td>${fmt(r.gross_edge)}</td>
    <td>${fmt(r.expected_pnl)}</td>
    <td>${fmt(r.pnl_best)}</td>
    <td>${fmt(r.pnl_worst)}</td>
    <td>${r.markout_60s === null ? 'n/a' : fmt(r.markout_60s)}</td>
  </tr>`).join('');
  return `<table>${header}${body}</table>`;
}

function renderMilestones(lines) {
  if (!lines || lines.length === 0) return '<div class="small">No milestones captured yet</div>';
  return lines.map(l => {
    const colorClass = l.includes('rotation') ? 'ok' :
      l.includes('paper_executor: placed') ? 'warn' :
      l.includes('capture period complete') ? 'err' : '';
    return `<div class="log-line ${colorClass}">${l}</div>`;
  }).join('');
}

async function poll() {
  try {
    const res = await fetch('/api/state').then(r => r.json());
    document.getElementById('dir').textContent = 'state dir: ' + res.dir + '  |  server time: ' + new Date().toISOString();
    document.getElementById('cards').innerHTML = renderCards(res);
    document.getElementById('candidates').innerHTML = renderCandidates(res.candidates_top20 || []);
    document.getElementById('fills').innerHTML = renderFills(res.fills_last_10 || []);
    document.getElementById('milestones').innerHTML = renderMilestones(res.milestones || []);
  } catch (e) {
    console.error(e);
  }
}
poll();
setInterval(poll, 5000);
</script>
</body>
</html>
"""


class DashboardHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            body = PAGE_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/api/state":
            snapshot = gather_state()
            body = json.dumps(snapshot, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


def main():
    with ReusableTCPServer(("0.0.0.0", PORT), DashboardHandler) as httpd:
        print(f"polymarket maker dashboard up: http://0.0.0.0:{PORT}/")
        httpd.serve_forever()


if __name__ == "__main__":
    main()
