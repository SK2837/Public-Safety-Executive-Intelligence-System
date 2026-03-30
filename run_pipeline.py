"""End-to-end pipeline orchestrator for PeregrineOps."""

import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent
DBT_DIR = BASE_DIR / "dbt_project"


def run(name: str, cmd: list[str], cwd: Path = BASE_DIR) -> None:
    print(f"\n{'=' * 50}")
    print(f"  {name}")
    print(f"{'=' * 50}")
    result = subprocess.run(cmd, cwd=str(cwd))
    if result.returncode != 0:
        print(f"\n[ERROR] '{name}' failed with exit code {result.returncode}")
        sys.exit(result.returncode)
    print(f"[OK] {name} completed")


if __name__ == "__main__":
    run("Step 1: Ingest SF Open Data", [sys.executable, "scripts/ingest_sf_data.py"])
    run("Step 2: dbt Run", ["dbt", "run", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)])
    run("Step 3: dbt Test", ["dbt", "test", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)])

    print("\n" + "=" * 50)
    print("  Pipeline complete!")
    print("=" * 50)
    print("\nLaunch the dashboard with:")
    print("  streamlit run dashboard/app.py")
