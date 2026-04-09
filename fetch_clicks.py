from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def run_script(script_path: Path) -> None:
    print(f"Running {script_path.name}...")
    subprocess.run([sys.executable, str(script_path)], check=True)


def main() -> None:
    repo_root = Path(__file__).resolve().parent
    scripts_dir = repo_root / "scripts"
    script_paths = [
        scripts_dir / "01_build_utm_campaign_csv.py",
        scripts_dir / "02_build_clicks_from_query_results.py",
        scripts_dir / "03_build_clicks_archive.py",
    ]

    for script_path in script_paths:
        if not script_path.exists():
            raise FileNotFoundError(f"Missing script: {script_path}")
        run_script(script_path)


if __name__ == "__main__":
    main()