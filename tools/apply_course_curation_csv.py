from __future__ import annotations

import csv
import sqlite3
import uuid
import sys
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
DEFAULT_INPUT_CSV = ROOT / "docs" / "course_curation_updates_template.csv"


def norm_num(raw: str) -> str:
    s = re.sub(r"\s+", "", str(raw or "").upper())
    m = re.match(r"^([A-Z]{2,20})(\d{3}[A-Z]?)$", s)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return " ".join(str(raw or "").strip().split())


def parse_numbers(raw: str) -> list[str]:
    return [norm_num(x) for x in str(raw or "").split(";") if str(x).strip()]


def main() -> None:
    input_csv = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_INPUT_CSV
    if not input_csv.is_absolute():
        input_csv = ROOT / input_csv
    if not input_csv.exists():
        raise SystemExit(f"Missing input CSV: {input_csv}")

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

    with input_csv.open("r", encoding="utf-8-sig", newline="") as f:
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
                if action == "UPSERT":
                    num = norm_num(row.get("course_number") or "")
                    if not num:
                        skipped += 1
                        continue
                    new_title = str(row.get("new_title") or "").strip() or num
                    new_credit_raw = str(row.get("new_credit_hours") or "").strip()
                    try:
                        new_credit = float(new_credit_raw) if new_credit_raw else 3.0
                    except Exception:
                        new_credit = 3.0
                    new_designated_raw = str(row.get("new_designated_semester") or "").strip()
                    try:
                        designated = int(new_designated_raw) if new_designated_raw else None
                    except Exception:
                        designated = None
                    offered = str(row.get("new_offered_periods_json") or "").strip() or None
                    standing = str(row.get("new_standing_requirement") or "").strip() or None
                    additional = str(row.get("new_additional_requirements_text") or "").strip() or None
                    course_id = uuid.uuid4().hex
                    cur.execute(
                        """
                        insert into courses(
                            id, version_id, course_number, title, credit_hours, designated_semester,
                            offered_periods_json, standing_requirement, additional_requirements_text, min_section_size
                        ) values(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (course_id, version_id, num, new_title, new_credit, designated, offered, standing, additional, 6),
                    )
                    course_id_by_num[num] = course_id
                    updated += 1
                else:
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
            new_designated_raw = str(row.get("new_designated_semester") or "").strip()
            new_offered_periods_json = str(row.get("new_offered_periods_json") or "").strip()
            new_standing_req = str(row.get("new_standing_requirement") or "").strip()
            new_additional_req = str(row.get("new_additional_requirements_text") or "").strip()
            has_course_field_update = any(
                [
                    bool(new_title),
                    bool(new_credit_raw),
                    (row.get("new_designated_semester") is not None and str(row.get("new_designated_semester")).strip() != ""),
                    (row.get("new_offered_periods_json") is not None and str(row.get("new_offered_periods_json")).strip() != ""),
                    (row.get("new_standing_requirement") is not None and str(row.get("new_standing_requirement")).strip() != ""),
                    (row.get("new_additional_requirements_text") is not None and str(row.get("new_additional_requirements_text")).strip() != ""),
                ]
            )
            if has_course_field_update:
                existing = cur.execute(
                    "select title, credit_hours, designated_semester, offered_periods_json, standing_requirement, additional_requirements_text from courses where id=?",
                    (course_id,),
                ).fetchone()
                if not existing:
                    skipped += 1
                    continue
                next_title = new_title if new_title else existing["title"]
                if new_credit_raw:
                    try:
                        next_credit = float(new_credit_raw)
                    except Exception:
                        next_credit = existing["credit_hours"]
                else:
                    next_credit = existing["credit_hours"]

                if row.get("new_designated_semester") is not None:
                    if not new_designated_raw:
                        next_designated = None
                    else:
                        try:
                            next_designated = int(new_designated_raw)
                        except Exception:
                            next_designated = existing["designated_semester"]
                else:
                    next_designated = existing["designated_semester"]

                next_offered = (
                    (new_offered_periods_json or None)
                    if row.get("new_offered_periods_json") is not None
                    else existing["offered_periods_json"]
                )
                next_standing = (
                    (new_standing_req or None)
                    if row.get("new_standing_requirement") is not None
                    else existing["standing_requirement"]
                )
                next_additional = (
                    (new_additional_req or None)
                    if row.get("new_additional_requirements_text") is not None
                    else existing["additional_requirements_text"]
                )

                cur.execute(
                    """
                    update courses
                    set title=?, credit_hours=?, designated_semester=?, offered_periods_json=?, standing_requirement=?, additional_requirements_text=?
                    where id=?
                    """,
                    (next_title, next_credit, next_designated, next_offered, next_standing, next_additional, course_id),
                )
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
            "source_csv": str(input_csv),
        }
    )


if __name__ == "__main__":
    main()
