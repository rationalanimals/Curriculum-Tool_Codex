from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COI_TEXT = ROOT / "coi_extracted.txt"
QUEUE_CSV = ROOT / "docs" / "course_curation_queue.csv"
TEMPLATE_CSV = ROOT / "docs" / "course_curation_updates_template.csv"
OUT_CSV = ROOT / "docs" / "course_curation_updates_batch2_titles.csv"


def norm_num(raw: str) -> str:
    return " ".join(str(raw or "").upper().split())


def clean_title(raw: str) -> str:
    t = str(raw or "").strip()
    t = re.sub(r"\s+", " ", t)
    return t.strip(" .")


def parse_coi_titles(text: str) -> dict[str, str]:
    # Captures lines like:
    # Aero Engr 241. Aero-Thermodynamics. 3(1).
    # Chem 243. Organic Chemistry Laboratory. 1(2).
    pattern = re.compile(
        r"(?P<num>[A-Za-z][A-Za-z\s&.\-]{0,24}\s\d{3}[A-Za-z]?)\.\s+(?P<title>[^.\n]{2,140})\.\s+\d+\(\d+",
        re.IGNORECASE,
    )
    titles_by_num: dict[str, Counter] = defaultdict(Counter)
    for m in pattern.finditer(text):
        num = norm_num(m.group("num"))
        title = clean_title(m.group("title"))
        if not num or not title:
            continue
        # Ignore obvious metadata captures.
        if re.match(r"^(sem\s*hrs?|prereq|coreq|co-?req)\b", title, re.IGNORECASE):
            continue
        titles_by_num[num][title] += 1
    out = {}
    for num, c in titles_by_num.items():
        out[num] = c.most_common(1)[0][0]
    return out


def main() -> None:
    if not COI_TEXT.exists():
        raise SystemExit(f"Missing COI text: {COI_TEXT}")
    if not QUEUE_CSV.exists() or not TEMPLATE_CSV.exists():
        raise SystemExit("Missing queue/template CSV files in docs/")

    coi = COI_TEXT.read_text(encoding="utf-8", errors="ignore")
    title_map = parse_coi_titles(coi)

    queue_rows = list(csv.DictReader(QUEUE_CSV.open("r", encoding="utf-8-sig", newline="")))
    queue_type_by_course_id = {str(r.get("course_id") or ""): str(r.get("type") or "") for r in queue_rows}

    rows = list(csv.DictReader(TEMPLATE_CSV.open("r", encoding="utf-8-sig", newline="")))
    changed = 0
    for row in rows:
        cid = str(row.get("course_id") or "")
        qtype = queue_type_by_course_id.get(cid, "")
        if qtype not in {"title_contains_metadata_not_short_title", "title_too_long_for_short_title"}:
            continue
        num = norm_num(row.get("course_number") or "")
        suggested = title_map.get(num)
        if not suggested:
            continue
        current_title = clean_title(row.get("current_title") or "")
        if current_title == suggested:
            continue
        # Preserve explicit manual actions if already present.
        action = str(row.get("action") or "").strip().upper()
        if action and action not in {"UPDATE", "UPSERT"}:
            continue
        row["action"] = "UPDATE"
        row["new_title"] = suggested
        note = str(row.get("notes") or "")
        row["notes"] = (note + " | auto-batch2: short title from COI extracted text").strip(" |")
        changed += 1

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {OUT_CSV} with {changed} title updates")


if __name__ == "__main__":
    main()

