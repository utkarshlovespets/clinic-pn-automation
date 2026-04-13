from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from rq import get_current_job


REPO_ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = REPO_ROOT / "outputs"
JOBS_LOG_ROOT = OUTPUTS_DIR / "log" / "jobs"
IST = ZoneInfo("Asia/Kolkata")


def now_iso() -> str:
    return datetime.now(tz=IST).isoformat(timespec="seconds")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _build_command(script_name: str, args: list[str]) -> list[str]:
    return [sys.executable, str(REPO_ROOT / script_name), *args]


def _job_log_dir(job_id: str) -> Path:
    job_dir = JOBS_LOG_ROOT / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


def _run_subprocess(name: str, command: list[str], timeout_seconds: int, metadata: dict[str, Any]) -> dict[str, Any]:
    job = get_current_job()
    job_id = job.id if job else "manual"
    job_dir = _job_log_dir(job_id)
    stdout_path = job_dir / "stdout.log"
    stderr_path = job_dir / "stderr.log"
    meta_path = job_dir / "meta.json"

    run_meta = {
        "job_id": job_id,
        "name": name,
        "command": command,
        "timeout_seconds": timeout_seconds,
        "started_at": now_iso(),
        "metadata": metadata,
        "status": "running",
    }
    _write_json(meta_path, run_meta)

    with stdout_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open("w", encoding="utf-8") as stderr_file:
        try:
            completed = subprocess.run(
                command,
                cwd=REPO_ROOT,
                stdout=stdout_file,
                stderr=stderr_file,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
            run_meta["exit_code"] = completed.returncode
            run_meta["status"] = "succeeded" if completed.returncode == 0 else "failed"
        except subprocess.TimeoutExpired:
            run_meta["status"] = "timeout"
            run_meta["error"] = f"Command exceeded timeout of {timeout_seconds} seconds"
            run_meta["exit_code"] = None
        except Exception as exc:  # pragma: no cover
            run_meta["status"] = "error"
            run_meta["error"] = str(exc)
            run_meta["exit_code"] = None

    run_meta["finished_at"] = now_iso()
    run_meta["stdout_path"] = str(stdout_path.relative_to(REPO_ROOT))
    run_meta["stderr_path"] = str(stderr_path.relative_to(REPO_ROOT))
    _write_json(meta_path, run_meta)

    if job:
        job.meta["log_dir"] = str(job_dir.relative_to(REPO_ROOT))
        job.meta["meta_path"] = str(meta_path.relative_to(REPO_ROOT))
        job.meta["task"] = name
        job.meta["started_at"] = run_meta["started_at"]
        job.meta["finished_at"] = run_meta["finished_at"]
        job.meta["status"] = run_meta["status"]
        job.save_meta()

    if run_meta["status"] in {"failed", "timeout", "error"}:
        raise RuntimeError(f"{name} job failed with status={run_meta['status']}")

    return run_meta


def run_campaign_task(payload: dict[str, Any]) -> dict[str, Any]:
    args: list[str] = []
    if payload.get("date"):
        args.extend(["--date", str(payload["date"])])
    if payload.get("slot"):
        args.extend(["--slot", str(payload["slot"])])
    if payload.get("max_workers"):
        args.extend(["--max-workers", str(payload["max_workers"])])

    cohorts = payload.get("cohorts") or []
    if cohorts:
        args.extend(["--cohorts", *[str(item) for item in cohorts]])

    if payload.get("live"):
        args.append("--live")

    timeout_seconds = int(payload.get("timeout_seconds", 60 * 30))
    command = _build_command("run_campaign.py", args)
    return _run_subprocess("campaign", command, timeout_seconds, payload)


def run_fetch_clicks_task(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    task_payload = payload or {}
    timeout_seconds = int(task_payload.get("timeout_seconds", 60 * 10))
    command = _build_command("fetch_clicks.py", [])
    return _run_subprocess("fetch_clicks", command, timeout_seconds, task_payload)


def zip_job_logs(job_id: str) -> Path:
    job_dir = _job_log_dir(job_id)
    zip_path = job_dir / "logs.zip"

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(job_dir.rglob("*")):
            if file_path.is_file() and file_path.name != "logs.zip":
                archive.write(file_path, arcname=file_path.relative_to(job_dir))

    return zip_path


def zip_full_logs() -> Path:
    log_root = OUTPUTS_DIR / "log"
    log_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=IST).strftime("%d%m%Y_%H%M%S")
    zip_path = log_root / f"full_logs_{stamp}.zip"

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(log_root.rglob("*")):
            if file_path.is_file() and file_path != zip_path:
                archive.write(file_path, arcname=file_path.relative_to(log_root))

    return zip_path
