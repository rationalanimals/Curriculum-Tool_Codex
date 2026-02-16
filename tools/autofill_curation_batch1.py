from __future__ import annotations

import csv
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IN_CSV = ROOT / "docs" / "course_curation_updates_template.csv"
OUT_CSV = ROOT / "docs" / "course_curation_updates_batch1.csv"


def norm_num(raw: str) -> str:
    return " ".join(str(raw or "").upper().split())


COURSE_NUM_RE = re.compile(r"\b([A-Za-z][A-Za-z\s&.\-]{0,24}\s\d{3}[A-Za-z]?)\b")


def extract_course_numbers(text: str) -> list[str]:
    vals = []
    for m in COURSE_NUM_RE.finditer(text or ""):
        num = norm_num(m.group(1))
        if num and num not in vals:
            vals.append(num)
    return vals


def main() -> None:
    if not IN_CSV.exists():
        raise SystemExit(f"Missing template: {IN_CSV}")
    with IN_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    out_rows = []
    changed = 0
    for r in rows:
        row = dict(r)
        title = str(row.get("current_title") or "")
        lo = title.lower()
        prereq_nums: list[str] = []
        coreq_nums: list[str] = []
        if "prereq" in lo:
            prereq_nums = extract_course_numbers(title)
        if "coreq" in lo or "co-req" in lo or "co req" in lo:
            coreq_nums = extract_course_numbers(title)

        if prereq_nums or coreq_nums:
            row["action"] = "UPDATE"
            row["prereq_numbers_semicolon"] = "; ".join(prereq_nums)
            row["coreq_numbers_semicolon"] = "; ".join(coreq_nums)
            note = str(row.get("notes") or "")
            row["notes"] = (note + " | auto-batch1: extracted prereq/coreq from title text").strip(" |")
            changed += 1
        out_rows.append(row)

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else [])
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Wrote {OUT_CSV} with {changed} auto-updated rows")


if __name__ == "__main__":
    main()

