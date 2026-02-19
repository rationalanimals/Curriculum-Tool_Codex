from __future__ import annotations

import csv
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
COI_TEXT = ROOT / "coi_extracted.txt"
REPORT_CSV = ROOT / "docs" / "course_period_count_extraction_report.csv"
OUT_CSV = ROOT / "docs" / "course_period_count_manual_queue.csv"


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").replace("\u00a0", " ")).strip()


def normalize_course_number(raw: str) -> str:
    s = normalize_spaces(raw)
    m = re.match(r"^([A-Za-z][A-Za-z&.\- ]{0,40}?)\s*(\d{3}[A-Za-z]?)$", s)
    if not m:
        return s.strip(" .")
    return f"{normalize_spaces(m.group(1))} {m.group(2).upper()}"


def split_prefix_num(course_number: str) -> tuple[str | None, str | None]:
    s = normalize_course_number(course_number)
    m = re.match(r"^([A-Za-z][A-Za-z&.\- ]{0,40}?)\s+(\d{3}[A-Za-z]?)$", s)
    if not m:
        return None, None
    return normalize_spaces(m.group(1)).lower(), m.group(2).upper()


def title_tokens(title: str) -> set[str]:
    stop = {
        "and", "of", "the", "to", "in", "for", "with", "a", "an", "ii", "iii", "iv",
        "course", "student", "lab", "laboratory", "seminar", "studies"
    }
    toks = re.findall(r"[A-Za-z]{3,}", str(title or "").lower())
    return {t for t in toks if t not in stop}


@dataclass
class CoiCourse:
    number: str
    title: str
    credits: float
    periods: int
    start: int
    end: int


def parse_coi_courses(text: str) -> list[CoiCourse]:
    normalized = text.replace("\r", "\n").replace("\x0c", "\n")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    pattern = re.compile(
        r"(?P<num>[A-Za-z][A-Za-z&.\- ]{0,40}\s\d{3}[A-Za-z]?(?:/\d{3}[A-Za-z]?)*)\.\s+"
        r"(?P<title>[^.\n]{1,220})\.\s+"
        r"(?P<credits>\d+(?:\.\d+)?)\((?P<periods>\d+)\)",
        re.IGNORECASE,
    )
    out: list[CoiCourse] = []
    for m in pattern.finditer(normalized):
        raw_num = normalize_spaces(m.group("num"))
        title = normalize_spaces(m.group("title")).strip(". ")
        if title.lower().startswith(("prereq", "coreq", "sem hrs")):
            continue
        try:
            credits = float(m.group("credits"))
            periods = int(m.group("periods"))
        except Exception:
            continue
        nums = [raw_num]
        mm = re.match(r"^([A-Za-z][A-Za-z&.\- ]{0,40}?)\s+(\d{3}[A-Za-z]?(?:/\d{3}[A-Za-z]?)*)$", raw_num)
        if mm and "/" in mm.group(2):
            pfx = normalize_spaces(mm.group(1))
            nums = [f"{pfx} {n.strip()}" for n in mm.group(2).split("/") if n.strip()]
        for n in nums:
            out.append(CoiCourse(normalize_course_number(n), title, credits, periods, m.start(), m.end()))
    return out


def make_snippet(text: str, start: int, end: int, span: int = 140) -> str:
    lo = max(0, start - span)
    hi = min(len(text), end + span)
    return normalize_spaces(text[lo:hi])


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Missing DB: {DB_PATH}")
    if not COI_TEXT.exists():
        raise SystemExit(f"Missing COI text: {COI_TEXT}")
    if not REPORT_CSV.exists():
        raise SystemExit(f"Missing report CSV: {REPORT_CSV}")

    coi_raw = COI_TEXT.read_text(encoding="utf-8", errors="ignore")
    coi_courses = parse_coi_courses(coi_raw)

    by_number: dict[str, list[CoiCourse]] = {}
    by_num_only: dict[str, list[CoiCourse]] = {}
    for c in coi_courses:
        by_number.setdefault(c.number, []).append(c)
        _, n = split_prefix_num(c.number)
        if n:
            by_num_only.setdefault(n, []).append(c)

    rows = list(csv.DictReader(REPORT_CSV.open("r", encoding="utf-8")))
    unmatched = [r for r in rows if r.get("status") == "no_match"]

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    out_rows: list[dict[str, str]] = []
    for r in unmatched:
        cid = r["course_id"]
        crow = cur.execute(
            "select course_number,title,period_count from courses where id=?",
            (cid,),
        ).fetchone()
        if not crow:
            continue
        cnum = normalize_course_number(crow["course_number"])
        ctitle = str(crow["title"] or "")
        pfx, num = split_prefix_num(cnum)
        c_tokens = title_tokens(ctitle)

        direct = by_number.get(cnum, [])
        candidates = list(direct)
        if not candidates and num:
            candidates = list(by_num_only.get(num, []))

        scored: list[tuple[int, CoiCourse]] = []
        for cand in candidates:
            score = 0
            cpfx, cnum2 = split_prefix_num(cand.number)
            if cnum2 and num and cnum2 == num:
                score += 5
            if cpfx and pfx and (cpfx == pfx or cpfx.startswith(pfx[:4]) or pfx.startswith(cpfx[:4])):
                score += 3
            overlap = len(c_tokens & title_tokens(cand.title))
            score += min(4, overlap)
            scored.append((score, cand))
        scored.sort(key=lambda x: (-x[0], x[1].number, x[1].title))
        top = [c for _, c in scored[:5]]
        best = top[0] if top else None
        best_score = scored[0][0] if scored else 0

        suggested_periods = ""
        confidence = "none"
        if best:
            suggested_periods = str(best.periods)
            if best.number == cnum:
                confidence = "high"
            elif best_score >= 7:
                confidence = "medium"
            else:
                confidence = "low"

        out_rows.append(
            {
                "course_id": cid,
                "course_number": crow["course_number"],
                "current_title": ctitle,
                "current_period_count": "" if crow["period_count"] is None else str(crow["period_count"]),
                "suggested_period_count": suggested_periods,
                "confidence": confidence,
                "candidate_1": f"{top[0].number} | {top[0].title} | {top[0].credits:g}({top[0].periods})" if len(top) > 0 else "",
                "candidate_2": f"{top[1].number} | {top[1].title} | {top[1].credits:g}({top[1].periods})" if len(top) > 1 else "",
                "candidate_3": f"{top[2].number} | {top[2].title} | {top[2].credits:g}({top[2].periods})" if len(top) > 2 else "",
                "coi_snippet": make_snippet(coi_raw, best.start, best.end) if best else "",
                "notes": "review suggested_period_count against COI",
            }
        )

    con.close()

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "course_id",
                "course_number",
                "current_title",
                "current_period_count",
                "suggested_period_count",
                "confidence",
                "candidate_1",
                "candidate_2",
                "candidate_3",
                "coi_snippet",
                "notes",
            ],
        )
        w.writeheader()
        w.writerows(out_rows)

    print(f"Unmatched courses queued: {len(out_rows)}")
    print(f"Wrote: {OUT_CSV}")


if __name__ == "__main__":
    main()
