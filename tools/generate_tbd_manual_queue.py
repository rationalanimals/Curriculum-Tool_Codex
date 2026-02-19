from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
COI_TEXT = ROOT / "coi_extracted.txt"
OUT_CSV = ROOT / "docs" / "course_curation_tbd_manual_queue.csv"


def make_pattern(course_number: str) -> re.Pattern[str]:
    toks = [re.escape(x) for x in str(course_number).split() if x]
    if not toks:
        return re.compile(r"$^")
    return re.compile(r"\b" + r"\s+".join(toks) + r"\b", re.IGNORECASE)


def load_snippets(lines: list[str], course_number: str, limit: int = 3) -> str:
    pat = make_pattern(course_number)
    hits = []
    for line in lines:
        if pat.search(line):
            s = re.sub(r"\s+", " ", line).strip()
            if s:
                hits.append(s[:240])
        if len(hits) >= limit:
            break
    return " || ".join(hits)


def main() -> None:
    coi_lines = []
    if COI_TEXT.exists():
        coi_lines = [x for x in COI_TEXT.read_text(encoding="utf-8", errors="ignore").splitlines() if x.strip()]

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    active = cur.execute("select id from curriculum_versions where status='ACTIVE' order by created_at desc limit 1").fetchone()
    if not active:
        raise SystemExit("No ACTIVE curriculum version found")
    version_id = active["id"]

    rows = cur.execute(
        """
        select c.id,c.course_number,c.title,c.credit_hours,
          (select count(*) from requirement_fulfillment rf where rf.course_id=c.id) as req_links,
          (select count(*) from course_basket_items bi where bi.course_id=c.id) as basket_links,
          (select count(*) from plan_items pi where pi.course_id=c.id) as plan_links
        from courses c
        where c.version_id=? and c.title like '%(TBD Curate)%'
        order by req_links desc, basket_links desc, c.course_number asc
        """,
        (version_id,),
    ).fetchall()
    con.close()

    out_rows = []
    for r in rows:
        out_rows.append(
            {
                "course_id": r["id"],
                "course_number": r["course_number"],
                "current_title": r["title"],
                "current_credit_hours": str(r["credit_hours"]),
                "requirement_links": str(r["req_links"]),
                "basket_links": str(r["basket_links"]),
                "plan_links": str(r["plan_links"]),
                "coi_snippets": load_snippets(coi_lines, r["course_number"]),
                "new_title": "",
                "new_credit_hours": "",
                "new_designated_semester": "",
                "new_offered_periods_json": "",
                "new_standing_requirement": "",
                "new_additional_requirements_text": "",
                "prereq_numbers_semicolon": "",
                "coreq_numbers_semicolon": "",
                "notes": "",
            }
        )

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "course_id",
                "course_number",
                "current_title",
                "current_credit_hours",
                "requirement_links",
                "basket_links",
                "plan_links",
                "coi_snippets",
                "new_title",
                "new_credit_hours",
                "new_designated_semester",
                "new_offered_periods_json",
                "new_standing_requirement",
                "new_additional_requirements_text",
                "prereq_numbers_semicolon",
                "coreq_numbers_semicolon",
                "notes",
            ],
        )
        w.writeheader()
        w.writerows(out_rows)
    print(f"Wrote {len(out_rows)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()

