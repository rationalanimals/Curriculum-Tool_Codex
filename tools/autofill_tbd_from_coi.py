from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
COI_TEXT = ROOT / "coi_extracted.txt"
OUT_CSV = ROOT / "docs" / "course_curation_tbd_autofill_from_coi.csv"


def norm_num(raw: str) -> str:
    s = re.sub(r"\s+", "", str(raw or "").upper())
    m = re.match(r"^([A-Z]{2,20})(\d{3}[A-Z]?)$", s)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return " ".join(str(raw or "").strip().split())


def clean_title(raw: str) -> str:
    t = re.sub(r"\s+", " ", str(raw or "")).strip(" .\t")
    return t


def parse_coi_catalog(text: str) -> dict[str, tuple[str, float]]:
    # Pattern examples:
    # Econ 332. Microeconomic Theory. 3(3).
    # Chem 243. Organic Chemistry Laboratory. 1(2).
    pat = re.compile(
        r"(?P<num>[A-Za-z][A-Za-z\s&.\-]{0,28}\s\d{3}[A-Za-z]?)\.\s+"
        r"(?P<title>[^.\n]{2,140})\.\s+"
        r"(?P<cr>\d+(?:\.\d+)?)\(\d+\)",
        re.IGNORECASE,
    )
    out: dict[str, tuple[str, float]] = {}
    for m in pat.finditer(text):
        num = norm_num(m.group("num"))
        title = clean_title(m.group("title"))
        if not num or not title:
            continue
        if re.match(r"^(prereq|coreq|co-?req|sem\s*hrs?)\b", title, re.IGNORECASE):
            continue
        try:
            cr = float(m.group("cr"))
        except Exception:
            cr = 3.0
        # Keep the first seen entry to avoid noisy duplicates.
        out.setdefault(num, (title, cr))
    return out


def main() -> None:
    if not COI_TEXT.exists():
        raise SystemExit(f"Missing {COI_TEXT}")
    coi_map = parse_coi_catalog(COI_TEXT.read_text(encoding="utf-8", errors="ignore"))

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    active = cur.execute("select id from curriculum_versions where status='ACTIVE' order by created_at desc limit 1").fetchone()
    if not active:
        raise SystemExit("No ACTIVE curriculum version found")
    version_id = active["id"]

    tbd_rows = cur.execute(
        "select id, course_number, title, credit_hours from courses where version_id=? and title like '%(TBD Curate)%'",
        (version_id,),
    ).fetchall()
    con.close()

    out_rows: list[dict[str, str]] = []
    matched = 0
    for r in tbd_rows:
        num = norm_num(r["course_number"])
        found = coi_map.get(num)
        if not found:
            continue
        title, cr = found
        matched += 1
        out_rows.append(
            {
                "action": "UPDATE",
                "course_id": r["id"],
                "course_number": r["course_number"],
                "current_title": r["title"],
                "current_credit_hours": str(r["credit_hours"]),
                "new_title": title,
                "new_credit_hours": str(cr),
                "new_designated_semester": "",
                "new_offered_periods_json": "",
                "new_standing_requirement": "",
                "new_additional_requirements_text": "",
                "prereq_numbers_semicolon": "",
                "coreq_numbers_semicolon": "",
                "notes": "autofill_tbd_from_coi",
            }
        )

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "action",
                "course_id",
                "course_number",
                "current_title",
                "current_credit_hours",
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

    print(f"TBD courses: {len(tbd_rows)}")
    print(f"COI matches: {matched}")
    print(f"Wrote: {OUT_CSV}")


if __name__ == "__main__":
    main()

