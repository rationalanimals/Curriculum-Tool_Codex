from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
OUT_JSON = ROOT / "docs" / "course_catalog_qc_report_pass2.json"
OUT_CSV = ROOT / "docs" / "course_curation_queue.csv"
OUT_TEMPLATE_CSV = ROOT / "docs" / "course_curation_updates_template.csv"


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
    version_name = active["name"]

    courses = cur.execute(
        "select id, course_number, title, credit_hours from courses where version_id=? order by course_number asc, id asc",
        (version_id,),
    ).fetchall()
    course_ids = [r["id"] for r in courses]
    req_ids = [r["id"] for r in cur.execute("select id from requirements where version_id=?", (version_id,)).fetchall()]

    prereqs = (
        cur.execute(
            f"select id, course_id, required_course_id from course_prerequisites where course_id in ({','.join(['?'] * len(course_ids))})",
            course_ids,
        ).fetchall()
        if course_ids
        else []
    )
    fulfillments = (
        cur.execute(
            f"select id, requirement_id, course_id from requirement_fulfillment where requirement_id in ({','.join(['?'] * len(req_ids))})",
            req_ids,
        ).fetchall()
        if req_ids
        else []
    )

    course_by_id = {c["id"]: c for c in courses}
    prereq_by_course: dict[str, list[sqlite3.Row]] = {}
    for row in prereqs:
        prereq_by_course.setdefault(row["course_id"], []).append(row)

    malformed_number_re = re.compile(r"^[A-Z][A-Z\s&.\-]{0,24}\s\d{3}[A-Z]?$", re.IGNORECASE)
    title_credit_like_re = re.compile(r"^\s*(\d+(\.\d+)?)\s*(credits?|hrs?|hours?)?\s*$", re.IGNORECASE)
    title_id_like_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-", re.IGNORECASE)
    polluted_title_re = re.compile(
        r"^\s*(sem\s*hrs?|prereq|coreq|co-?req|hours?|credit|course,\s*summer|required\.)\b",
        re.IGNORECASE,
    )
    prereq_text_re = re.compile(r"\b(prereq|coreq|co-?req)\b", re.IGNORECASE)

    anomalies: list[dict] = []
    prefixes: dict[str, int] = {}

    for c in courses:
        cid = c["id"]
        num = str(c["course_number"] or "").strip()
        title = str(c["title"] or "").strip()
        prefix = num.split(" ")[0] if " " in num else re.sub(r"[^A-Za-z]", "", num)[:10]
        if prefix:
            prefixes[prefix] = prefixes.get(prefix, 0) + 1

        if not malformed_number_re.match(num):
            anomalies.append({"type": "malformed_course_number", "course_id": cid, "course_number": num, "title": title})
        if not title:
            anomalies.append({"type": "missing_title", "course_id": cid, "course_number": num, "title": title})
        elif title_credit_like_re.match(title):
            anomalies.append({"type": "title_looks_like_credit_hours", "course_id": cid, "course_number": num, "title": title})
        elif title_id_like_re.match(title):
            anomalies.append({"type": "title_looks_like_id", "course_id": cid, "course_number": num, "title": title})
        elif polluted_title_re.search(title):
            anomalies.append(
                {
                    "type": "title_contains_metadata_not_short_title",
                    "course_id": cid,
                    "course_number": num,
                    "title": title,
                }
            )
        if len(title) > 140:
            anomalies.append({"type": "title_too_long_for_short_title", "course_id": cid, "course_number": num, "title": title})
        if num.upper().startswith("GENR "):
            anomalies.append({"type": "placeholder_course", "course_id": cid, "course_number": num, "title": title})
        if prereq_text_re.search(title) and not prereq_by_course.get(cid):
            anomalies.append(
                {
                    "type": "prereq_or_coreq_text_in_title_but_no_structured_rows",
                    "course_id": cid,
                    "course_number": num,
                    "title": title,
                }
            )

    for row in fulfillments:
        if row["course_id"] not in course_by_id:
            anomalies.append(
                {
                    "type": "orphan_requirement_link",
                    "fulfillment_id": row["id"],
                    "requirement_id": row["requirement_id"],
                    "course_id": row["course_id"],
                }
            )

    counts = dict(Counter(a["type"] for a in anomalies))

    report = {
        "version_id": version_id,
        "version_name": version_name,
        "total_courses": len(courses),
        "total_prerequisites": len(prereqs),
        "courses_with_prereqs": len(prereq_by_course),
        "prefix_counts": dict(sorted(prefixes.items(), key=lambda x: x[0].lower())),
        "anomaly_count": len(anomalies),
        "anomaly_counts": counts,
        "anomalies_sample": anomalies[:300],
    }
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")

    queue_rows = [
        a
        for a in anomalies
        if a["type"]
        in {
            "missing_title",
            "title_looks_like_credit_hours",
            "title_looks_like_id",
            "title_contains_metadata_not_short_title",
            "title_too_long_for_short_title",
            "placeholder_course",
            "prereq_or_coreq_text_in_title_but_no_structured_rows",
            "orphan_requirement_link",
        }
    ]
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "type",
                "course_number",
                "title",
                "course_id",
                "fulfillment_id",
                "requirement_id",
            ],
        )
        writer.writeheader()
        for row in queue_rows:
            writer.writerow(
                {
                    "type": row.get("type", ""),
                    "course_number": row.get("course_number", ""),
                    "title": row.get("title", ""),
                    "course_id": row.get("course_id", ""),
                    "fulfillment_id": row.get("fulfillment_id", ""),
                    "requirement_id": row.get("requirement_id", ""),
                }
            )

    with OUT_TEMPLATE_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "action",
                "course_id",
                "course_number",
                "current_title",
                "current_credit_hours",
                "new_title",
                "new_credit_hours",
                "prereq_numbers_semicolon",
                "coreq_numbers_semicolon",
                "notes",
            ],
        )
        writer.writeheader()
        written = set()
        for row in queue_rows:
            cid = row.get("course_id") or ""
            if not cid or cid in written:
                continue
            course = course_by_id.get(cid)
            if not course:
                continue
            writer.writerow(
                {
                    "action": "",
                    "course_id": cid,
                    "course_number": course["course_number"],
                    "current_title": course["title"],
                    "current_credit_hours": course["credit_hours"],
                    "new_title": "",
                    "new_credit_hours": "",
                    "prereq_numbers_semicolon": "",
                    "coreq_numbers_semicolon": "",
                    "notes": "",
                }
            )
            written.add(cid)

    print(f"Wrote {OUT_JSON}")
    print(f"Wrote {OUT_CSV}")
    print(f"Wrote {OUT_TEMPLATE_CSV}")
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
