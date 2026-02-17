from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COI_TEXT = ROOT / "coi_extracted.txt"
QUEUE_CSV = ROOT / "docs" / "course_curation_queue.csv"
TEMPLATE_CSV = ROOT / "docs" / "course_curation_updates_template.csv"
OUT_CSV = ROOT / "docs" / "course_curation_updates_batch3_titles.csv"


def norm_num(raw: str) -> str:
    return " ".join(str(raw or "").upper().split())


def clean_line(raw: str) -> str:
    s = raw.replace("\x07", " ").replace("\x0c", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def clean_title(raw: str) -> str:
    t = re.sub(r"\s+", " ", str(raw or "")).strip(" .\t")
    return t


BAD_TITLE_RE = re.compile(
    r"\b(sem\s*hrs?|prereq|coreq|co-?req|final exam|department approval|pass/fail|spring|fall)\b",
    re.IGNORECASE,
)


def extract_candidates_for_number(lines: list[str], course_number: str) -> list[str]:
    num = re.escape(course_number)
    # Patterns from tabular/list styles in extracted text.
    patterns = [
        re.compile(rf"\b{num}\b\s*[.\t ]+\s*([A-Za-z][A-Za-z0-9&'’\-/,() ]{{3,120}})"),
        re.compile(rf"\b{num}\b\s*-\s*([A-Za-z][A-Za-z0-9&'’\-/,() ]{{3,120}})"),
    ]
    out = []
    for line in lines:
        for pat in patterns:
            m = pat.search(line)
            if not m:
                continue
            cand = clean_title(m.group(1))
            # Truncate at obvious metadata boundaries.
            cand = re.split(r"\s{2,}| \d\(\d| Prereq:| Coreq:| Sem hrs:", cand, maxsplit=1)[0].strip(" .")
            if len(cand) < 3:
                continue
            if BAD_TITLE_RE.search(cand):
                continue
            if re.search(r"\d{3}", cand):
                continue
            out.append(cand)
    return out


def main() -> None:
    if not (COI_TEXT.exists() and QUEUE_CSV.exists() and TEMPLATE_CSV.exists()):
        raise SystemExit("Missing required files for batch3 title autofill")

    lines = [clean_line(x) for x in COI_TEXT.read_text(encoding="utf-8", errors="ignore").splitlines() if clean_line(x)]
    queue = list(csv.DictReader(QUEUE_CSV.open("r", encoding="utf-8-sig", newline="")))
    template = list(csv.DictReader(TEMPLATE_CSV.open("r", encoding="utf-8-sig", newline="")))

    queue_type_by_cid = {str(r.get("course_id") or ""): str(r.get("type") or "") for r in queue}
    target_nums = {
        norm_num(r.get("course_number") or "")
        for r in queue
        if str(r.get("type") or "") in {"title_contains_metadata_not_short_title", "title_too_long_for_short_title"}
    }

    title_suggestions: dict[str, str] = {}
    for num in sorted(target_nums):
        cand = extract_candidates_for_number(lines, num)
        if not cand:
            continue
        c = Counter(cand)
        # Prefer most frequent, then shortest.
        best = sorted(c.items(), key=lambda kv: (-kv[1], len(kv[0]), kv[0].lower()))[0][0]
        title_suggestions[num] = best

    changed = 0
    for row in template:
        cid = str(row.get("course_id") or "")
        qtype = queue_type_by_cid.get(cid, "")
        if qtype not in {"title_contains_metadata_not_short_title", "title_too_long_for_short_title"}:
            continue
        num = norm_num(row.get("course_number") or "")
        suggested = title_suggestions.get(num)
        if not suggested:
            continue
        current = clean_title(row.get("current_title") or "")
        if current == suggested:
            continue
        # Only fill rows without manual title already entered.
        if str(row.get("new_title") or "").strip():
            continue
        action = str(row.get("action") or "").strip().upper()
        if action and action not in {"UPDATE", "UPSERT"}:
            continue
        row["action"] = "UPDATE"
        row["new_title"] = suggested
        note = str(row.get("notes") or "")
        row["notes"] = (note + " | auto-batch3: title suggestion from COI line patterns").strip(" |")
        changed += 1

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=template[0].keys() if template else [])
        writer.writeheader()
        writer.writerows(template)

    print(f"Wrote {OUT_CSV} with {changed} additional title updates")
    print(f"Found suggestions for {len(title_suggestions)} course numbers")


if __name__ == "__main__":
    main()

