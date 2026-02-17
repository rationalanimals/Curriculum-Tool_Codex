from __future__ import annotations

import csv
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COI_TEXT = ROOT / "coi_extracted.txt"
QUEUE_CSV = ROOT / "docs" / "course_curation_queue.csv"
TEMPLATE_CSV = ROOT / "docs" / "course_curation_updates_template.csv"
OUT_CSV = ROOT / "docs" / "course_curation_updates_batch4_titles.csv"


def clean_title(raw: str) -> str:
    return re.sub(r"\s+", " ", str(raw or "")).strip(" .")


def main() -> None:
    if not (COI_TEXT.exists() and QUEUE_CSV.exists() and TEMPLATE_CSV.exists()):
        raise SystemExit("Missing required files for strict title autofill")

    text = COI_TEXT.read_text(encoding="utf-8", errors="ignore")
    queue = list(csv.DictReader(QUEUE_CSV.open("r", encoding="utf-8-sig", newline="")))
    rows = list(csv.DictReader(TEMPLATE_CSV.open("r", encoding="utf-8-sig", newline="")))

    target_numbers = {
        str(r.get("course_number") or "").strip()
        for r in queue
        if str(r.get("type") or "") in {"title_contains_metadata_not_short_title", "title_too_long_for_short_title"}
    }

    suggestions = {}
    for num in sorted(target_numbers):
        if not num:
            continue
        pat = re.compile(rf"{re.escape(num)}\.\s+([^\n]{{3,160}}?)\.\s+\d+\(\d+", re.IGNORECASE)
        m = pat.search(text)
        if not m:
            continue
        title = clean_title(m.group(1))
        if not title:
            continue
        if re.match(r"^(sem\s*hrs?|prereq|coreq|co-?req)\b", title, re.IGNORECASE):
            continue
        suggestions[num] = title

    changed = 0
    for row in rows:
        num = str(row.get("course_number") or "").strip()
        if num not in suggestions:
            continue
        cur = clean_title(row.get("current_title") or "")
        new = suggestions[num]
        if cur == new:
            continue
        if str(row.get("new_title") or "").strip():
            continue
        action = str(row.get("action") or "").strip().upper()
        if action and action not in {"UPDATE", "UPSERT"}:
            continue
        row["action"] = "UPDATE"
        row["new_title"] = new
        note = str(row.get("notes") or "")
        row["notes"] = (note + " | auto-batch4: strict COI title match").strip(" |")
        changed += 1

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {OUT_CSV} with {changed} title updates")
    print(f"Suggestions available: {len(suggestions)}")


if __name__ == "__main__":
    main()

