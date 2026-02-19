from __future__ import annotations

import csv
from pathlib import Path
import sqlite3


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
OUT_PATH = ROOT / "docs" / "course_metadata_deep_dive_queue.csv"


def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    active = cur.execute(
        "select id, name from curriculum_versions where status='ACTIVE' order by created_at desc limit 1"
    ).fetchone()
    if not active:
        raise SystemExit("No ACTIVE curriculum version found")
    version_id = active["id"]

    rows = cur.execute(
        """
        select id, course_number, title, credit_hours, designated_semester, offered_periods_json,
               standing_requirement, additional_requirements_text, min_section_size
        from courses
        where version_id=?
        order by course_number asc, id asc
        """,
        (version_id,),
    ).fetchall()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "action",
                "course_id",
                "course_number",
                "current_title",
                "current_credit_hours",
                "current_designated_semester",
                "current_offered_periods_json",
                "current_standing_requirement",
                "current_additional_requirements_text",
                "current_min_section_size",
                "new_title",
                "new_credit_hours",
                "new_designated_semester",
                "new_offered_periods_json",
                "new_standing_requirement",
                "new_additional_requirements_text",
                "prereq_numbers_semicolon",
                "coreq_numbers_semicolon",
                "notes",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    "",
                    r["id"],
                    r["course_number"],
                    r["title"],
                    r["credit_hours"],
                    r["designated_semester"],
                    r["offered_periods_json"] or "",
                    r["standing_requirement"] or "",
                    r["additional_requirements_text"] or "",
                    r["min_section_size"],
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                ]
            )

    print(
        {
            "version_id": version_id,
            "version_name": active["name"],
            "rows": len(rows),
            "output_csv": str(OUT_PATH),
        }
    )


if __name__ == "__main__":
    main()

