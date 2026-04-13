from __future__ import annotations

import base64
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from redis import Redis
from rq import Queue
from rq.job import Job
from rq.registry import (
    DeferredJobRegistry,
    FailedJobRegistry,
    FinishedJobRegistry,
    ScheduledJobRegistry,
    StartedJobRegistry,
)

from tasks import zip_full_logs, zip_job_logs


REPO_ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = REPO_ROOT / "outputs"
LOG_DIR = OUTPUTS_DIR / "log"
JOBS_LOG_ROOT = LOG_DIR / "jobs"
ARCHIVE_FILE = LOG_DIR / "clicks_archive.csv"
ENV_FILE = REPO_ROOT / ".env"
SECRETS_DIR = REPO_ROOT / "secrets"
IST = ZoneInfo("Asia/Kolkata")

DEFAULT_COMMAND_TIMEOUTS = {
    "campaign": 60 * 30,
    "fetch_clicks": 60 * 10,
}

ADMIN_API_KEY = (os.getenv("ADMIN_API_KEY") or "").strip()
ENABLE_LIVE_RUNS = (os.getenv("ENABLE_LIVE_RUNS") or "false").strip().lower() == "true"
APP_TITLE = os.getenv("APP_TITLE", "Clinic PN Automation")
REDIS_URL = (os.getenv("REDIS_URL") or "redis://localhost:6379/0").strip()
RQ_QUEUE_NAME = (os.getenv("RQ_QUEUE_NAME") or "clinic-jobs").strip()


HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>__APP_TITLE__</title>
  <style>
    :root {
      --bg: #0f172a;
      --panel: #111827;
      --panel-soft: #1f2937;
      --text: #f8fafc;
      --muted: #cbd5e1;
      --accent: #38bdf8;
      --accent-2: #22c55e;
      --danger: #fb7185;
      --border: rgba(255,255,255,0.10);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(56,189,248,0.24), transparent 28%),
        radial-gradient(circle at top right, rgba(34,197,94,0.14), transparent 24%),
        var(--bg);
      color: var(--text);
    }
    .wrap { max-width: 1180px; margin: 0 auto; padding: 32px 20px 48px; }
    .hero { display: grid; gap: 16px; grid-template-columns: 1fr; margin-bottom: 24px; }
    .title { font-size: clamp(2rem, 3vw, 3rem); margin: 0; letter-spacing: -0.03em; }
    .subtitle { margin: 0; color: var(--muted); max-width: 72ch; line-height: 1.55; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
    .card {
      background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 24px 60px rgba(0,0,0,0.24);
      backdrop-filter: blur(8px);
    }
    .card h2 { margin: 0 0 14px; font-size: 1.1rem; }
    label { display: block; margin: 12px 0 6px; color: var(--muted); font-size: 0.92rem; }
    input, select {
      width: 100%;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: rgba(17,24,39,0.75);
      color: var(--text);
    }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .actions { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 14px; }
    button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      padding: 12px 16px;
      border-radius: 12px;
      border: 0;
      font-weight: 700;
      text-decoration: none;
      cursor: pointer;
    }
    button.primary { background: var(--accent); color: #0f172a; }
    button.secondary { background: #334155; color: var(--text); }
    button.success { background: var(--accent-2); color: #052e16; }
    button.ghost { background: transparent; border: 1px solid var(--border); color: var(--text); }
    .status {
      margin-top: 16px;
      padding: 14px;
      border-radius: 14px;
      background: rgba(15,23,42,0.72);
      border: 1px solid var(--border);
      white-space: pre-wrap;
      font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
      min-height: 64px;
      overflow-x: auto;
    }
    .table { width: 100%; border-collapse: collapse; }
    .table th, .table td {
      border-bottom: 1px solid var(--border);
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
      font-size: 0.92rem;
    }
    .pill {
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      background: rgba(56,189,248,0.14);
      color: #bae6fd;
      font-size: 0.82rem;
    }
    .pill.ok { background: rgba(34,197,94,0.16); color: #bbf7d0; }
    .pill.warn { background: rgba(251,113,133,0.16); color: #fecdd3; }
    code { color: #93c5fd; }
    .small { font-size: 0.85rem; color: var(--muted); }
    .section { margin-top: 22px; }
    .columns { display: grid; grid-template-columns: 1.2fr 0.8fr; gap: 16px; }
    @media (max-width: 900px) {
      .columns { grid-template-columns: 1fr; }
      .row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1 class="title">__APP_TITLE__</h1>
      <p class="subtitle">Redis-backed queueing for campaign triggers and clicks fetch with operator previews and full log downloads.</p>
      <div class="small">Default mode is dry-run. Live mode requires ENABLE_LIVE_RUNS=true.</div>
    </div>

    <div class="grid">
      <div class="card">
        <h2>Run Campaign</h2>
        <label for="apiKey">Admin API Key</label>
        <input id="apiKey" type="password" placeholder="Paste ADMIN_API_KEY" />
        <div class="row">
          <div>
            <label for="runDate">Date (DDMMYYYY)</label>
            <input id="runDate" value="__CURRENT_DATE__" />
          </div>
          <div>
            <label for="runSlot">Slot</label>
            <select id="runSlot">
              <option value="both">both</option>
              <option value="morning">morning</option>
              <option value="evening">evening</option>
            </select>
          </div>
        </div>
        <div class="row">
          <div>
            <label for="runMode">Run Mode</label>
            <select id="runMode">
              <option value="dry">dry</option>
              <option value="live">live</option>
            </select>
          </div>
          <div>
            <label for="maxWorkers">Max Workers</label>
            <input id="maxWorkers" type="number" min="1" max="200" value="10" />
          </div>
        </div>
        <label for="cohorts">Cohorts (optional)</label>
        <input id="cohorts" placeholder="N2B_All_Bangalore, Clinic_KN_Mar26" />
        <div class="actions">
          <button class="primary" onclick="runCampaign()">Queue Campaign</button>
          <button class="ghost" onclick="reviewPayloads()">Review ExternalTrigger Payloads</button>
        </div>
      </div>

      <div class="card">
        <h2>Clicks & Logs</h2>
        <label for="clicksApiKey">Admin API Key</label>
        <input id="clicksApiKey" type="password" placeholder="Paste ADMIN_API_KEY" />
        <div class="actions">
          <button class="secondary" onclick="fetchClicks()">Queue Fetch Clicks</button>
          <button class="success" onclick="downloadArchive()">Download Clicks Archive</button>
          <button class="ghost" onclick="downloadFullLogs()">Download Full Logs Zip</button>
        </div>
        <div class="section">
          <div class="pill" id="archiveState">Archive status loading...</div>
        </div>
      </div>
    </div>

    <div class="columns section">
      <div class="card">
        <h2>Job Status</h2>
        <div id="jobTable"></div>
      </div>
      <div class="card">
        <h2>Recent Outputs</h2>
        <div id="outputList"></div>
      </div>
    </div>

    <div class="card section">
      <h2>ExternalTrigger Review</h2>
      <div class="status" id="review">No review loaded yet.</div>
    </div>

    <div class="card section">
      <h2>Action Log</h2>
      <div class="status" id="log">Ready.</div>
    </div>
  </div>

  <script>
    const logEl = document.getElementById('log');
    const reviewEl = document.getElementById('review');
    const jobTableEl = document.getElementById('jobTable');
    const outputListEl = document.getElementById('outputList');
    const archiveStateEl = document.getElementById('archiveState');

    function log(message) {
      const timestamp = new Date().toLocaleTimeString();
      logEl.textContent = `[${timestamp}] ${message}\n\n` + logEl.textContent;
    }

    function getKey() {
      return document.getElementById('apiKey').value.trim() || document.getElementById('clicksApiKey').value.trim();
    }

    function headers(isJson = true) {
      const base = { 'X-Admin-Key': getKey() };
      return isJson ? { ...base, 'Content-Type': 'application/json' } : base;
    }

    async function api(path, method = 'GET', body = null) {
      const options = { method, headers: headers(method !== 'GET') };
      if (body !== null) {
        options.body = JSON.stringify(body);
      }
      const response = await fetch(path, options);
      const payload = await response.json().catch(() => ({ detail: 'Invalid response' }));
      if (!response.ok) {
        throw new Error(payload.detail || payload.message || `HTTP ${response.status}`);
      }
      return payload;
    }

    async function runCampaign() {
      try {
        const cohortsRaw = document.getElementById('cohorts').value.trim();
        const payload = {
          date: document.getElementById('runDate').value.trim() || null,
          slot: document.getElementById('runSlot').value,
          run_mode: document.getElementById('runMode').value,
          max_workers: Number(document.getElementById('maxWorkers').value || 10),
          cohorts: cohortsRaw ? cohortsRaw.split(/[\s,]+/).filter(Boolean) : null,
        };
        const result = await api('/api/campaign/run', 'POST', payload);
        log(`Queued campaign job ${result.job.id} (${result.job.status})`);
        refreshJobs();
      } catch (error) {
        log(`Campaign request failed: ${error.message}`);
      }
    }

    async function fetchClicks() {
      try {
        const result = await api('/api/clicks/fetch', 'POST', {});
        log(`Queued clicks job ${result.job.id} (${result.job.status})`);
        refreshJobs();
      } catch (error) {
        log(`Fetch clicks failed: ${error.message}`);
      }
    }

    async function reviewPayloads() {
      try {
        const date = document.getElementById('runDate').value.trim();
        const slot = document.getElementById('runSlot').value;
        const result = await api(`/api/review/external-trigger?date=${encodeURIComponent(date)}&slot=${encodeURIComponent(slot)}&limit=3`);
        reviewEl.textContent = JSON.stringify(result, null, 2);
        log(`Loaded review for ${result.output_folder}`);
      } catch (error) {
        reviewEl.textContent = `Review failed: ${error.message}`;
        log(`Review failed: ${error.message}`);
      }
    }

    async function downloadBlob(path, filename) {
      const response = await fetch(path, { headers: headers(false) });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({ detail: 'Download failed' }));
        throw new Error(payload.detail || `HTTP ${response.status}`);
      }
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = filename;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
    }

    async function downloadArchive() {
      try {
        await downloadBlob('/api/clicks/archive', 'clicks_archive.csv');
        log('Started clicks archive download.');
      } catch (error) {
        log(`Archive download failed: ${error.message}`);
      }
    }

    async function downloadFullLogs() {
      try {
        await downloadBlob('/api/logs/archive', 'full_logs.zip');
        log('Started full log archive download.');
      } catch (error) {
        log(`Full log archive download failed: ${error.message}`);
      }
    }

    function renderJobs(jobs) {
      if (!jobs.length) {
        jobTableEl.innerHTML = '<div class="small">No jobs yet.</div>';
        return;
      }
      const rows = jobs.map(job => {
        const isBad = ['failed', 'timeout', 'error'].includes(job.status);
        const isGood = ['finished', 'succeeded'].includes(job.status);
        const logAction = job.id
          ? `<button class="ghost" style="padding:6px 10px;" onclick="downloadJobLogs('${job.id}')">Logs</button>`
          : '';
        return `
          <tr>
            <td><code>${job.id}</code></td>
            <td>${job.name || ''}</td>
            <td><span class="pill ${isGood ? 'ok' : isBad ? 'warn' : ''}">${job.status}</span></td>
            <td>${job.created_at || ''}</td>
            <td>${job.finished_at || ''}</td>
            <td>${logAction}</td>
          </tr>
        `;
      }).join('');
      jobTableEl.innerHTML = `
        <table class="table">
          <thead><tr><th>ID</th><th>Name</th><th>Status</th><th>Created</th><th>Finished</th><th>Logs</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      `;
    }

    async function downloadJobLogs(jobId) {
      try {
        await downloadBlob(`/api/jobs/${jobId}/logs`, `job_${jobId}_logs.zip`);
        log(`Started log download for job ${jobId}.`);
      } catch (error) {
        log(`Job log download failed for ${jobId}: ${error.message}`);
      }
    }

    function renderOutputs(outputs) {
      if (!outputs.length) {
        outputListEl.innerHTML = '<div class="small">No output folders found yet.</div>';
        return;
      }
      outputListEl.innerHTML = outputs.map(folder => `
        <div style="margin-bottom: 14px;">
          <div><strong>${folder.name}</strong></div>
          <div class="small">Updated: ${folder.updated_at}</div>
          <div class="small">Files: ${folder.files.join(', ')}</div>
        </div>
      `).join('');
    }

    async function refreshArchiveState() {
      try {
        const payload = await api('/api/clicks/archive/status');
        archiveStateEl.textContent = payload.exists ? `Archive ready: ${payload.updated_at}` : 'Archive not found yet.';
        archiveStateEl.className = payload.exists ? 'pill ok' : 'pill warn';
      } catch (error) {
        archiveStateEl.textContent = `Archive status unavailable: ${error.message}`;
        archiveStateEl.className = 'pill warn';
      }
    }

    async function refreshJobs() {
      try {
        const payload = await api('/api/jobs');
        renderJobs(payload.jobs || []);
      } catch (error) {
        jobTableEl.innerHTML = `<div class="small">Job status unavailable: ${error.message}</div>`;
      }
    }

    async function refreshOutputs() {
      try {
        const payload = await api('/api/outputs');
        renderOutputs(payload.outputs || []);
      } catch (error) {
        outputListEl.innerHTML = `<div class="small">Output listing unavailable: ${error.message}</div>`;
      }
    }

    async function refreshAll() {
      await Promise.all([refreshJobs(), refreshOutputs(), refreshArchiveState()]);
    }

    refreshAll();
    setInterval(refreshAll, 5000);
  </script>
</body>
</html>
"""


def now_iso() -> str:
    return datetime.now(tz=IST).isoformat(timespec="seconds")


def current_ist_date() -> str:
    return datetime.now(tz=IST).strftime("%d%m%Y")


def materialize_secret_file(env_key: str, target_path: Path, encoded_env_key: str | None = None) -> None:
    value = (os.getenv(env_key) or "").strip()
    if encoded_env_key:
        encoded_value = (os.getenv(encoded_env_key) or "").strip()
        if encoded_value:
            try:
                value = base64.b64decode(encoded_value.encode("utf-8")).decode("utf-8")
            except Exception:
                value = ""

    if not value:
        return

    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and target_path.read_text(encoding="utf-8").strip():
        return

    target_path.write_text(value, encoding="utf-8")


def build_runtime_env_file() -> None:
    if ENV_FILE.exists():
        return

    materialize_secret_file("GOOGLE_CREDENTIALS_JSON", SECRETS_DIR / "credentials.json", "GOOGLE_CREDENTIALS_B64")
    materialize_secret_file("GOOGLE_TOKEN_JSON", SECRETS_DIR / "token.json", "GOOGLE_TOKEN_B64")

    defaults = {
        "SPREADSHEET_ID": os.getenv("SPREADSHEET_ID", ""),
        "GOOGLE_CREDENTIALS_FILE": os.getenv("GOOGLE_CREDENTIALS_FILE", "secrets/credentials.json"),
        "GOOGLE_TOKEN_FILE": os.getenv("GOOGLE_TOKEN_FILE", "secrets/token.json"),
        "CLEVERTAP_ACCOUNT_ID": os.getenv("CLEVERTAP_ACCOUNT_ID", ""),
        "CLEVERTAP_PASSCODE": os.getenv("CLEVERTAP_PASSCODE", ""),
        "CLEVERTAP_REGION": os.getenv("CLEVERTAP_REGION", "in1"),
        "CLEVERTAP_CAMPAIGN_ID": os.getenv("CLEVERTAP_CAMPAIGN_ID", ""),
        "MYSQL_HOST": os.getenv("MYSQL_HOST", ""),
        "MYSQL_PORT": os.getenv("MYSQL_PORT", "3306"),
        "MYSQL_USER": os.getenv("MYSQL_USER", ""),
        "MYSQL_PASSWORD": os.getenv("MYSQL_PASSWORD", ""),
        "START_DATE": os.getenv("START_DATE", ""),
        "CAMPAIGN_ID": os.getenv("CAMPAIGN_ID", ""),
    }

    lines = [f"{key}={value}" for key, value in defaults.items() if value]
    ENV_FILE.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


build_runtime_env_file()


class CampaignRunRequest(BaseModel):
    date: str | None = Field(default=None, description="DDMMYYYY format")
    slot: str = Field(default="both")
    run_mode: str = Field(default="dry", description="dry or live")
    live: bool | None = Field(default=None, description="Backward compatible boolean")
    max_workers: int = Field(default=10, ge=1, le=200)
    cohorts: list[str] | None = None


class ClicksRunRequest(BaseModel):
    pass


def redis_connection() -> Redis:
    return Redis.from_url(REDIS_URL)


def queue() -> Queue:
    return Queue(name=RQ_QUEUE_NAME, connection=redis_connection(), default_timeout=DEFAULT_COMMAND_TIMEOUTS["campaign"])


def require_admin_key(x_admin_key: str = Header(default="", alias="X-Admin-Key")) -> None:
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY is not configured")
    if x_admin_key.strip() != ADMIN_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid admin key")


def normalize_slot(slot: str) -> str:
    valid = {"morning", "evening", "both"}
    if slot not in valid:
        raise HTTPException(status_code=422, detail=f"Invalid slot: {slot}. Must be one of {sorted(valid)}")
    return slot


def normalize_live_flag(payload: CampaignRunRequest) -> bool:
    if payload.live is not None:
        requested_live = bool(payload.live)
    else:
        requested_live = payload.run_mode.strip().lower() == "live"

    if requested_live and not ENABLE_LIVE_RUNS:
        raise HTTPException(status_code=403, detail="Live runs are disabled. Set ENABLE_LIVE_RUNS=true to allow them.")
    return requested_live


def load_job_meta_file(job_id: str) -> dict[str, Any]:
    meta_path = JOBS_LOG_ROOT / job_id / "meta.json"
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def map_rq_status(raw_status: str | None) -> str:
    if raw_status is None:
        return "unknown"
    mapping = {
        "queued": "queued",
        "started": "running",
        "finished": "finished",
        "failed": "failed",
        "deferred": "deferred",
        "scheduled": "scheduled",
        "stopped": "stopped",
        "canceled": "canceled",
    }
    return mapping.get(raw_status, raw_status)


def read_tail(path: Path, max_chars: int = 6000) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return ""
    return text[-max_chars:]


def serialize_job(job: Job) -> dict[str, Any]:
    meta_file = load_job_meta_file(job.id)
    status = map_rq_status(job.get_status(refresh=True))
    log_dir_rel = job.meta.get("log_dir")
    log_dir = REPO_ROOT / log_dir_rel if log_dir_rel else JOBS_LOG_ROOT / job.id
    stdout_tail = read_tail(log_dir / "stdout.log")
    stderr_tail = read_tail(log_dir / "stderr.log")
    return {
        "id": job.id,
        "name": str(job.meta.get("task") or job.description or "job"),
        "status": status,
        "created_at": datetime.fromtimestamp(job.created_at.timestamp(), tz=IST).isoformat(timespec="seconds") if job.created_at else None,
        "started_at": job.meta.get("started_at") or meta_file.get("started_at"),
        "finished_at": job.meta.get("finished_at") or meta_file.get("finished_at"),
        "enqueued_at": datetime.fromtimestamp(job.enqueued_at.timestamp(), tz=IST).isoformat(timespec="seconds") if job.enqueued_at else None,
        "ended_at": datetime.fromtimestamp(job.ended_at.timestamp(), tz=IST).isoformat(timespec="seconds") if job.ended_at else None,
        "log_dir": job.meta.get("log_dir"),
        "meta_path": job.meta.get("meta_path"),
        "metadata": job.meta,
        "result": job.result,
        "exc_info": job.exc_info[-3000:] if job.exc_info else "",
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }


def list_recent_jobs(limit: int = 25) -> list[dict[str, Any]]:
    q = queue()
    registries = [
        StartedJobRegistry(queue=q),
        ScheduledJobRegistry(queue=q),
        DeferredJobRegistry(queue=q),
        FailedJobRegistry(queue=q),
        FinishedJobRegistry(queue=q),
    ]

    job_ids: list[str] = []
    for job_id in q.job_ids:
        if job_id not in job_ids:
            job_ids.append(job_id)
    for registry in registries:
        for job_id in registry.get_job_ids()[:limit]:
            if job_id not in job_ids:
                job_ids.append(job_id)

    discovered_ids = sorted(
        [p.name for p in JOBS_LOG_ROOT.iterdir() if p.is_dir()],
        reverse=True,
    ) if JOBS_LOG_ROOT.exists() else []
    for job_id in discovered_ids:
        if job_id not in job_ids:
            job_ids.append(job_id)

    jobs: list[dict[str, Any]] = []
    for job_id in job_ids[: limit * 2]:
        job = q.fetch_job(job_id)
        if job is not None:
            jobs.append(serialize_job(job))
            continue

        meta = load_job_meta_file(job_id)
        if meta:
            jobs.append(
                {
                    "id": job_id,
                    "name": meta.get("name", "job"),
                    "status": meta.get("status", "unknown"),
                    "created_at": meta.get("started_at"),
                    "started_at": meta.get("started_at"),
                    "finished_at": meta.get("finished_at"),
                    "enqueued_at": None,
                    "ended_at": None,
                    "log_dir": str((JOBS_LOG_ROOT / job_id).relative_to(REPO_ROOT)),
                    "meta_path": str((JOBS_LOG_ROOT / job_id / "meta.json").relative_to(REPO_ROOT)),
                    "metadata": meta.get("metadata", {}),
                    "result": meta,
                    "exc_info": meta.get("error", ""),
                    "stdout_tail": read_tail(JOBS_LOG_ROOT / job_id / "stdout.log"),
                    "stderr_tail": read_tail(JOBS_LOG_ROOT / job_id / "stderr.log"),
                }
            )

    jobs.sort(key=lambda item: item.get("created_at") or "", reverse=True)
    return jobs[:limit]


def latest_output_folders(limit: int = 8) -> list[dict[str, Any]]:
    if not OUTPUTS_DIR.exists():
        return []

    folders: list[dict[str, Any]] = []
    for path in sorted(
        (item for item in OUTPUTS_DIR.iterdir() if item.is_dir() and item.name != "log"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    ):
        folders.append(
            {
                "name": path.name,
                "updated_at": datetime.fromtimestamp(path.stat().st_mtime, tz=IST).isoformat(timespec="seconds"),
                "files": sorted(child.name for child in path.iterdir() if child.is_file())[:20],
            }
        )
        if len(folders) >= limit:
            break
    return folders


def archive_status() -> dict[str, Any]:
    if not ARCHIVE_FILE.exists():
        return {"exists": False, "path": str(ARCHIVE_FILE)}
    stat = ARCHIVE_FILE.stat()
    return {
        "exists": True,
        "path": str(ARCHIVE_FILE),
        "size_bytes": stat.st_size,
        "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=IST).isoformat(timespec="seconds"),
    }


def output_folder(date: str, slot: str) -> Path:
    return OUTPUTS_DIR / f"{date}_{slot}"


def build_external_trigger_preview(date: str, slot: str, limit: int) -> dict[str, Any]:
    slot_name = normalize_slot(slot)
    if slot_name == "both":
        raise HTTPException(status_code=422, detail="Review endpoint requires slot=morning or slot=evening")

    folder = output_folder(date, slot_name)
    if not folder.exists():
        raise HTTPException(status_code=404, detail=f"Output folder not found: {folder.name}")

    summary_path = folder / "summary.csv"
    if not summary_path.exists():
        raise HTTPException(status_code=404, detail=f"summary.csv not found in {folder.name}")

    summary_df = pd.read_csv(summary_path, dtype=str, keep_default_na=False)
    required_summary = {"priority", "cohort_name", "output_file"}
    missing_summary = required_summary - set(summary_df.columns)
    if missing_summary:
        raise HTTPException(status_code=422, detail=f"summary.csv missing columns: {sorted(missing_summary)}")

    rows: list[dict[str, Any]] = []
    required_fields = ["Email", "title", "body", "android_deeplink", "ios_deeplink"]

    for _, row in summary_df.head(limit).iterrows():
        file_name = str(row.get("output_file", "")).strip()
        if not file_name:
            continue

        csv_path = folder / file_name
        if not csv_path.exists():
            rows.append(
                {
                    "cohort_name": str(row.get("cohort_name", "")),
                    "priority": str(row.get("priority", "")),
                    "file": file_name,
                    "exists": False,
                    "row_count": 0,
                    "has_required_fields": False,
                    "missing_fields": required_fields,
                    "payload_sample": {},
                }
            )
            continue

        cohort_df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        fields_present = set(cohort_df.columns)
        missing = [field for field in required_fields if field not in fields_present]

        payload_sample: dict[str, Any] = {}
        sample_row: dict[str, Any] = {}
        if not cohort_df.empty and not missing:
            first = cohort_df.iloc[0].to_dict()
            sample_row = {key: str(first.get(key, "")) for key in required_fields}
            payload_sample = {
                "to": {"email": [sample_row["Email"]]},
                "campaign_id_list": ["<CLEVERTAP_CAMPAIGN_ID>"],
                "ExternalTrigger": {
                    "title": sample_row["title"],
                    "body": sample_row["body"],
                    "android_deeplink": sample_row["android_deeplink"],
                    "ios_deeplink": sample_row["ios_deeplink"],
                },
            }

        rows.append(
            {
                "cohort_name": str(row.get("cohort_name", "")),
                "priority": str(row.get("priority", "")),
                "file": file_name,
                "exists": True,
                "row_count": int(len(cohort_df.index)),
                "has_required_fields": len(missing) == 0,
                "missing_fields": missing,
                "sample_row": sample_row,
                "payload_sample": payload_sample,
            }
        )

    return {
        "date": date,
        "slot": slot_name,
        "output_folder": folder.name,
        "reviewed_at": now_iso(),
        "rows": rows,
    }


def enqueue_campaign(payload: CampaignRunRequest, live_flag: bool) -> Job:
    q = queue()
    task_payload = {
        "date": payload.date or current_ist_date(),
        "slot": normalize_slot(payload.slot),
        "live": live_flag,
        "max_workers": payload.max_workers,
        "cohorts": payload.cohorts or [],
        "timeout_seconds": DEFAULT_COMMAND_TIMEOUTS["campaign"],
    }
    return q.enqueue(
        "tasks.run_campaign_task",
        task_payload,
        job_timeout=DEFAULT_COMMAND_TIMEOUTS["campaign"],
        meta={
            "task": "campaign",
            "requested_at": now_iso(),
            "payload": task_payload,
        },
    )


def enqueue_fetch_clicks() -> Job:
    q = queue()
    task_payload = {
        "timeout_seconds": DEFAULT_COMMAND_TIMEOUTS["fetch_clicks"],
    }
    return q.enqueue(
        "tasks.run_fetch_clicks_task",
        task_payload,
        job_timeout=DEFAULT_COMMAND_TIMEOUTS["fetch_clicks"],
        meta={
            "task": "fetch_clicks",
            "requested_at": now_iso(),
            "payload": task_payload,
        },
    )


app = FastAPI(title=APP_TITLE)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/health")
def api_health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
def index() -> HTMLResponse:
    return HTMLResponse(
        HTML_TEMPLATE.replace("__APP_TITLE__", APP_TITLE).replace("__CURRENT_DATE__", current_ist_date())
    )


@app.post("/api/campaign/run")
def run_campaign(payload: CampaignRunRequest, _: None = Depends(require_admin_key)) -> JSONResponse:
    live_flag = normalize_live_flag(payload)
    job = enqueue_campaign(payload, live_flag)
    return JSONResponse(status_code=202, content={"message": "Campaign queued", "job": serialize_job(job)})


@app.post("/api/clicks/fetch")
def fetch_clicks(_: None = Depends(require_admin_key)) -> JSONResponse:
    job = enqueue_fetch_clicks()
    return JSONResponse(status_code=202, content={"message": "Clicks fetch queued", "job": serialize_job(job)})


@app.get("/api/jobs")
def list_jobs(_: None = Depends(require_admin_key)) -> dict[str, Any]:
    return {"jobs": list_recent_jobs()}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, _: None = Depends(require_admin_key)) -> dict[str, Any]:
    q = queue()
    job = q.fetch_job(job_id)
    if job is None:
        meta = load_job_meta_file(job_id)
        if not meta:
            raise HTTPException(status_code=404, detail="Job not found")
        meta["stdout_tail"] = read_tail(JOBS_LOG_ROOT / job_id / "stdout.log")
        meta["stderr_tail"] = read_tail(JOBS_LOG_ROOT / job_id / "stderr.log")
        return {"job": meta}
    return {"job": serialize_job(job)}



@app.get("/api/jobs/{job_id}/tail")
def get_job_tail(
    job_id: str,
    max_chars: int = Query(default=6000, ge=200, le=20000),
    _: None = Depends(require_admin_key),
) -> dict[str, Any]:
    log_dir = JOBS_LOG_ROOT / job_id
    if not log_dir.exists():
        raise HTTPException(status_code=404, detail=f"Log folder not found for job {job_id}")
    return {
        "job_id": job_id,
        "stdout_tail": read_tail(log_dir / "stdout.log", max_chars=max_chars),
        "stderr_tail": read_tail(log_dir / "stderr.log", max_chars=max_chars),
    }


@app.get("/api/jobs/{job_id}/logs")
def download_job_logs(job_id: str, _: None = Depends(require_admin_key)) -> FileResponse:
    if not (JOBS_LOG_ROOT / job_id).exists():
        raise HTTPException(status_code=404, detail=f"Log folder not found for job {job_id}")
    zip_path = zip_job_logs(job_id)
    return FileResponse(zip_path, media_type="application/zip", filename=f"job_{job_id}_logs.zip")


@app.get("/api/outputs")
def list_outputs(_: None = Depends(require_admin_key)) -> dict[str, Any]:
    return {"outputs": latest_output_folders()}


@app.get("/api/review/external-trigger")
def review_external_trigger(
    date: str = Query(..., min_length=8, max_length=8),
    slot: str = Query(...),
    limit: int = Query(default=5, ge=1, le=25),
    _: None = Depends(require_admin_key),
) -> dict[str, Any]:
    return build_external_trigger_preview(date=date, slot=slot, limit=limit)


@app.get("/api/clicks/archive/status")
def clicks_archive_status(_: None = Depends(require_admin_key)) -> dict[str, Any]:
    return archive_status()


@app.get("/api/clicks/archive")
def download_clicks_archive(_: None = Depends(require_admin_key)) -> FileResponse:
    if not ARCHIVE_FILE.exists():
        raise HTTPException(status_code=404, detail=f"Archive not found: {ARCHIVE_FILE}")
    return FileResponse(ARCHIVE_FILE, media_type="text/csv", filename="clicks_archive.csv")


@app.get("/api/logs/archive")
def download_logs_archive(_: None = Depends(require_admin_key)) -> FileResponse:
    zip_path = zip_full_logs()
    return FileResponse(zip_path, media_type="application/zip", filename=zip_path.name)


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
