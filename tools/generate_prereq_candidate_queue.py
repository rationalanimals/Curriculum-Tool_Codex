from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
COI_TEXT_PATH = ROOT / "coi_extracted.txt"
OUT_CSV = ROOT / "docs" / "course_curation_prereq_candidates_batch2.csv"
OUT_REPORT = ROOT / "docs" / "course_prereq_candidate_report_batch2.json"


def norm(s: str) -> str:
    return " ".join((s or "").upper().split())


def title_case_course_number(n: str) -> str:
    return " ".join(x.capitalize() if not x.isupper() else x.title() for x in n.split())


COURSE_TOKEN_RE = re.compile(r"\b([A-Za-z][A-Za-z&]*(?: [A-Za-z&]+){0,5} \d{3}[A-Z]?)\b")
COURSE_BLOCK_RE = re.compile(
    r"([A-Za-z][A-Za-z& ]{1,30}\s\d{3}[A-Z]?)\.\s(.{0,1800}?)\bPrereq:\s(.{0,650}?)\.\sSem\s*hrs:",
    re.IGNORECASE,
)
LEADING_CONNECTOR_RE = re.compile(r"^(OR|AND|EITHER|BOTH|WITH|PLUS)\s+", re.IGNORECASE)


def extract_course_tokens(prereq_text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in COURSE_TOKEN_RE.findall(prereq_text or ""):
        cleaned = LEADING_CONNECTOR_RE.sub("", token).strip()
        # Some lines include merged phrases like "AERO ENGR 241 OR AERO ENGR 315".
        parts = re.split(r"\b(?:OR|AND|EITHER|BOTH|WITH|PLUS)\b", cleaned, flags=re.IGNORECASE)
        for part in parts:
            n = norm(part)
            if not n or n in seen:
                continue
            seen.add(n)
            out.append(n)
    return out


def main() -> None:
    if not COI_TEXT_PATH.exists():
        raise SystemExit(f"Missing extracted text file: {COI_TEXT_PATH}")
    if not DB_PATH.exists():
        raise SystemExit(f"Missing DB: {DB_PATH}")

    text = COI_TEXT_PATH.read_text(encoding="utf-8", errors="ignore")
    flat = re.sub(r"\s+", " ", text)

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    active = cur.execute(
        "select id, name from curriculum_versions where status='ACTIVE' order by created_at desc limit 1"
    ).fetchone()
    if not active:
        raise SystemExit("No ACTIVE curriculum version found")
    version_id = active["id"]

    course_rows = cur.execute(
        "select id, course_number from courses where version_id=?",
        (version_id,),
    ).fetchall()
    course_id_by_num = {norm(r["course_number"]): r["id"] for r in course_rows}

    existing_pairs = set(
        (row[0], row[1])
        for row in cur.execute("select course_id, required_course_id from course_prerequisites").fetchall()
    )

    rows_out: list[dict[str, str]] = []
    stats = {
        "version_id": version_id,
        "version_name": active["name"],
        "blocks_with_prereq": 0,
        "candidate_rows": 0,
        "total_candidate_pairs": 0,
        "unmapped_pairs": 0,
    }

    for m in COURSE_BLOCK_RE.finditer(flat):
        src = norm(m.group(1))
        prereq_text_full = m.group(3).strip()
        src_id = course_id_by_num.get(src)
        if not src_id:
            continue

        stats["blocks_with_prereq"] += 1
        prereq_part = prereq_text_full
        coreq_part = ""
        coreq_split = re.split(r"\bCoreq:\s*", prereq_text_full, maxsplit=1, flags=re.IGNORECASE)
        if len(coreq_split) == 2:
            prereq_part, coreq_part = coreq_split[0], coreq_split[1]

        prereq_tokens = [t for t in extract_course_tokens(prereq_part) if t != src]
        coreq_tokens = [t for t in extract_course_tokens(coreq_part) if t != src]
        if not prereq_tokens and not coreq_tokens:
            continue

        mapped_prereq_new: list[str] = []
        mapped_coreq_new: list[str] = []
        unmapped: list[str] = []

        for t in prereq_tokens:
            req_id = course_id_by_num.get(t)
            if not req_id:
                unmapped.append(t)
                continue
            if (src_id, req_id) in existing_pairs:
                continue
            mapped_prereq_new.append(t)

        for t in coreq_tokens:
            req_id = course_id_by_num.get(t)
            if not req_id:
                unmapped.append(t)
                continue
            if (src_id, req_id) in existing_pairs:
                continue
            mapped_coreq_new.append(t)

        if not mapped_prereq_new and not mapped_coreq_new:
            continue

        rows_out.append(
            {
                "action": "UPDATE",
                "course_number": title_case_course_number(src),
                "new_title": "",
                "course_id": src_id,
                "prereq_numbers_semicolon": "; ".join(mapped_prereq_new),
                "coreq_numbers_semicolon": "; ".join(mapped_coreq_new),
                "notes": "auto-prereq-candidate from COI text",
                "raw_prereq_text": prereq_text_full,
                "unmapped_numbers": "; ".join(unmapped),
            }
        )
        stats["candidate_rows"] += 1
        stats["total_candidate_pairs"] += len(mapped_prereq_new) + len(mapped_coreq_new)
        stats["unmapped_pairs"] += len(unmapped)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "action",
                "course_number",
                "new_title",
                "course_id",
                "prereq_numbers_semicolon",
                "coreq_numbers_semicolon",
                "notes",
                "raw_prereq_text",
                "unmapped_numbers",
            ],
        )
        writer.writeheader()
        writer.writerows(rows_out)

    OUT_REPORT.write_text(str(stats), encoding="utf-8")
    print(stats)
    print(f"Wrote {OUT_CSV}")
    print(f"Wrote {OUT_REPORT}")


if __name__ == "__main__":
    main()
