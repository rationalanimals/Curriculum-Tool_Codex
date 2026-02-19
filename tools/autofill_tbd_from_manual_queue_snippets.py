from __future__ import annotations

import csv
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IN_FILE = ROOT / "docs" / "course_curation_tbd_manual_queue.csv"
OUT_FILE = ROOT / "docs" / "course_curation_tbd_autofill_from_snippets.csv"


def clean(s: str) -> str:
    t = str(s or "").replace("\x07", " ").replace("\x0c", " ")
    return re.sub(r"\s+", " ", t).strip()


def extract_title(course_number: str, snippets: str) -> str:
    cn = re.escape(course_number)
    candidates: list[str] = []
    parts = [clean(x) for x in str(snippets or "").split("||") if clean(x)]
    for p in parts:
        # "Course 123 Title Words"
        m = re.search(rf"\b{cn}\b\.?\s+([A-Za-z][A-Za-z0-9&'(),/\- ]{{3,90}})", p, re.IGNORECASE)
        if m:
            t = m.group(1)
            t = re.split(r"\s{2,}|\. \d+\(|Prereq:|Coreq:", t, maxsplit=1)[0]
            t = clean(t).strip(" .")
            if t and not re.search(r"\b(Spring|Fall|Semester|course|cadets?)\b", t, re.IGNORECASE):
                candidates.append(t)
        # "Course 123. Title. 3(1)."
        m2 = re.search(rf"\b{cn}\b\.\s+([^.]+)\.\s+\d+(?:\.\d+)?\(", p, re.IGNORECASE)
        if m2:
            t2 = clean(m2.group(1)).strip(" .")
            if t2:
                candidates.append(t2)
    # pick shortest plausible candidate for short title style
    candidates = [c for c in candidates if 3 <= len(c) <= 90 and not re.search(r"\d{3}", c)]
    if not candidates:
        return ""
    return sorted(set(candidates), key=lambda x: (len(x), x.lower()))[0]


def extract_credit(course_number: str, snippets: str) -> str:
    cn = re.escape(course_number)
    parts = [clean(x) for x in str(snippets or "").split("||") if clean(x)]
    for p in parts:
        m = re.search(rf"\b{cn}\b\.\s+[^.]+\.\s+(\d+(?:\.\d+)?)\(", p, re.IGNORECASE)
        if m:
            return m.group(1)
    return ""


def main() -> None:
    if not IN_FILE.exists():
        raise SystemExit(f"Missing input queue: {IN_FILE}")
    rows = list(csv.DictReader(IN_FILE.open("r", encoding="utf-8-sig", newline="")))
    out_rows: list[dict[str, str]] = []
    for r in rows:
        course_number = str(r.get("course_number") or "").strip()
        snippets = str(r.get("coi_snippets") or "")
        title = extract_title(course_number, snippets)
        if not title:
            continue
        credit = extract_credit(course_number, snippets) or str(r.get("current_credit_hours") or "3.0")
        out_rows.append(
            {
                "action": "UPDATE",
                "course_id": str(r.get("course_id") or ""),
                "course_number": course_number,
                "current_title": str(r.get("current_title") or ""),
                "current_credit_hours": str(r.get("current_credit_hours") or ""),
                "new_title": title,
                "new_credit_hours": credit,
                "new_designated_semester": "",
                "new_offered_periods_json": "",
                "new_standing_requirement": "",
                "new_additional_requirements_text": "",
                "prereq_numbers_semicolon": "",
                "coreq_numbers_semicolon": "",
                "notes": "autofill_tbd_from_manual_queue_snippets",
            }
        )

    with OUT_FILE.open("w", encoding="utf-8", newline="") as f:
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
    print(f"Wrote {len(out_rows)} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()

