from __future__ import annotations

import csv
import sqlite3
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
INPUT_CSV = ROOT / "docs" / "course_curation_updates_template.csv"


def norm_num(raw: str) -> str:
    return " ".join(str(raw or "").upper().split())


def parse_numbers(raw: str) -> list[str]:
    return [norm_num(x) for x in str(raw or "").split(";") if str(x).strip()]


def main() -> None:
    if not INPUT_CSV.exists():
        raise SystemExit(f"Missing input CSV: {INPUT_CSV}")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    active = cur.execute(
        "select id, name from curriculum_versions where status='ACTIVE' order by created_at desc limit 1"
    ).fetchone()
    if not active:
        raise SystemExit("No ACTIVE curriculum version found")
    version_id = active["id"]

    course_rows = cur.execute("select id, course_number from courses where version_id=?", (version_id,)).fetchall()
    course_id_by_num = {norm_num(r["course_number"]): r["id"] for r in course_rows}

    updated = 0
    deleted = 0
    prereq_added = 0
    prereq_deleted = 0
    skipped = 0

    with INPUT_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            action = str(row.get("action") or "").strip().upper()
            if not action:
                continue

            course_id = str(row.get("course_id") or "").strip()
            if not course_id:
                num = norm_num(row.get("course_number") or "")
                course_id = course_id_by_num.get(num, "")
            if not course_id:
                skipped += 1
                continue

            if action == "DELETE":
                cur.execute("delete from plan_items where course_id=?", (course_id,))
                cur.execute("delete from requirement_fulfillment where course_id=?", (course_id,))
                cur.execute(
                    "delete from requirement_substitutions where primary_course_id=? or substitute_course_id=?",
                    (course_id, course_id),
                )
                cur.execute(
                    "delete from course_prerequisites where course_id=? or required_course_id=?",
                    (course_id, course_id),
                )
                cur.execute(
                    "delete from course_substitutions where original_course_id=? or substitute_course_id=?",
                    (course_id, course_id),
                )
                cur.execute("delete from course_bucket_tags where course_id=?", (course_id,))
                cur.execute("delete from course_basket_items where course_id=?", (course_id,))
                cur.execute("delete from courses where id=?", (course_id,))
                deleted += 1
                continue

            if action not in {"UPDATE", "UPSERT"}:
                skipped += 1
                continue

            new_title = str(row.get("new_title") or "").strip()
            new_credit_raw = str(row.get("new_credit_hours") or "").strip()
            if new_title:
                if new_credit_raw:
                    try:
                        ch = float(new_credit_raw)
                    except Exception:
                        ch = None
                else:
                    ch = None
                if ch is None:
                    cur.execute("update courses set title=? where id=?", (new_title, course_id))
                else:
                    cur.execute("update courses set title=?, credit_hours=? where id=?", (new_title, ch, course_id))
                updated += 1

            # Rebuild prerequisites when prereq/coreq columns are provided.
            prereqs_raw = row.get("prereq_numbers_semicolon")
            coreqs_raw = row.get("coreq_numbers_semicolon")
            if (prereqs_raw is not None and str(prereqs_raw).strip()) or (coreqs_raw is not None and str(coreqs_raw).strip()):
                removed = cur.execute("delete from course_prerequisites where course_id=?", (course_id,)).rowcount
                prereq_deleted += int(removed or 0)

                for num in parse_numbers(prereqs_raw):
                    req_id = course_id_by_num.get(num)
                    if not req_id:
                        continue
                    cur.execute(
                        "insert into course_prerequisites(id, course_id, required_course_id, relationship_type, enforcement) values(?,?,?,?,?)",
                        (uuid.uuid4().hex, course_id, req_id, "PREREQUISITE", "HARD"),
                    )
                    prereq_added += 1
                for num in parse_numbers(coreqs_raw):
                    req_id = course_id_by_num.get(num)
                    if not req_id:
                        continue
                    cur.execute(
                        "insert into course_prerequisites(id, course_id, required_course_id, relationship_type, enforcement) values(?,?,?,?,?)",
                        (uuid.uuid4().hex, course_id, req_id, "COREQUISITE", "HARD"),
                    )
                    prereq_added += 1

    con.commit()
    print(
        {
            "updated_courses": updated,
            "deleted_courses": deleted,
            "prerequisites_added": prereq_added,
            "prerequisites_deleted": prereq_deleted,
            "skipped_rows": skipped,
            "version_id": version_id,
            "source_csv": str(INPUT_CSV),
        }
    )


if __name__ == "__main__":
    main()
