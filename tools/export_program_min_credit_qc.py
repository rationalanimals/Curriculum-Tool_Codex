from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = ROOT / "docs" / "program_min_credit_qc_report.csv"
BACKEND_PATH = ROOT / "backend"
if str(BACKEND_PATH) not in sys.path:
    sys.path.insert(0, str(BACKEND_PATH))

from sqlalchemy import select

from app.main import CurriculumVersion, SessionLocal, design_feasibility


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise SystemExit("No ACTIVE curriculum version found")
        result = design_feasibility(active.id, db, None)
    rows = result.get("rows") or []
    export_rows = []
    for row in rows:
        export_rows.append(
            {
                "kind": row.get("kind"),
                "label": row.get("label"),
                "min_required_credits": row.get("min_required_credits"),
                "status": row.get("status"),
                "consistency_status": row.get("consistency_status"),
                "validation_fail_count": row.get("validation_fail_count"),
                "consistency_fail_count": row.get("consistency_fail_count"),
                "mandatory_course_count": row.get("mandatory_course_count"),
            }
        )
    export_rows.sort(key=lambda x: (str(x.get("kind") or ""), -float(x.get("min_required_credits") or 0.0), str(x.get("label") or "")))
    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(export_rows[0].keys()) if export_rows else [])
        writer.writeheader()
        writer.writerows(export_rows)
    print(
        {
            "rows": len(export_rows),
            "path": str(OUT_PATH),
        }
    )


if __name__ == "__main__":
    main()
