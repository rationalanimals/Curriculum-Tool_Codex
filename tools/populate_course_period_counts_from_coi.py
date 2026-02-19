from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "backend" / "cmt.db"
DEFAULT_COI_TEXT = ROOT / "coi_extracted.txt"
DEFAULT_REPORT = ROOT / "docs" / "course_period_count_extraction_report.csv"

PREFIX_ALIASES = {
    "ar stu": ["am st", "american studies"],
    "cs": ["comp sci", "computer science"],
    "sys engr": ["sys eng", "systems engineering"],
    "mech engr": ["mech eng", "mechanical engineering"],
    "ece": ["electrical and computer engineering", "elec engr"],
    "mss": ["mil & strat studies", "military & strategic studies"],
    "phy ed": ["pe", "physical education"],
    "meteor": ["meteorology"],
    "comm strt": ["comm strategies", "communication strategies"],
    "read strt": ["reading strategies"],
    "lrn strt": ["learning strategies"],
    "fye": ["first-year experience"],
    "for lang": ["foreign language", "foreign languages"],
}

NON_COI_CUSTOM_PREFIXES = {
    "genr",
    "for lang",
    "adv sociocultural option",
    "aero design elective",
    "aero engr elective",
}


def normalize_course_number(raw: str) -> str:
    s = " ".join(str(raw or "").replace("\u00a0", " ").split())
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s*/\s*", "/", s)
    m = re.match(r"^([A-Za-z][A-Za-z&.\- ]{0,30}?)\s*(\d{3}[A-Za-z]?)$", s)
    if not m:
        return s.strip(" .")
    prefix = re.sub(r"\s+", " ", m.group(1)).strip()
    return f"{prefix} {m.group(2)}"


def expand_course_numbers(raw: str) -> list[str]:
    s = normalize_course_number(raw)
    m = re.match(r"^([A-Za-z][A-Za-z&.\- ]{0,30}?)\s+(\d{3}[A-Za-z]?(?:/\d{3}[A-Za-z]?)+)$", s)
    if not m:
        return [s]
    prefix = re.sub(r"\s+", " ", m.group(1)).strip()
    nums = [x.strip() for x in m.group(2).split("/") if x.strip()]
    return [f"{prefix} {n}" for n in nums]


def parse_coi_period_counts(text: str) -> dict[str, Counter]:
    normalized = text.replace("\r", "\n").replace("\x0c", "\n")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)
    # Pattern example: "Math 141. Calculus I. 3(4)." where 4 is periods.
    pat = re.compile(
        r"(?P<num>[A-Za-z][A-Za-z&.\- ]{0,30}\s\d{3}[A-Za-z]?(?:/\d{3}[A-Za-z]?)*)\.\s+"
        r"(?P<title>[^.\n]{1,220})\.\s+"
        r"(?P<credits>\d+(?:\.\d+)?)\((?P<periods>\d+)\)",
        re.IGNORECASE,
    )
    out: dict[str, Counter] = defaultdict(Counter)
    for m in pat.finditer(normalized):
        title = str(m.group("title") or "").strip().lower()
        if title.startswith(("prereq", "coreq", "sem hrs")):
            continue
        periods = int(m.group("periods"))
        for number in expand_course_numbers(m.group("num")):
            out[number][periods] += 1
    return out


def split_prefix_number(course_number: str) -> tuple[str, str] | tuple[None, None]:
    s = normalize_course_number(course_number)
    m = re.match(r"^([A-Za-z][A-Za-z&.\- ]{0,40}?)\s+(\d{3}[A-Za-z]?)$", s)
    if not m:
        return None, None
    prefix = re.sub(r"\s+", " ", m.group(1)).strip().lower()
    num = m.group(2).upper()
    return prefix, num


def build_alias_candidates(course_number: str) -> list[str]:
    s = normalize_course_number(course_number)
    out = [s]
    prefix, num = split_prefix_number(s)
    if not prefix or not num:
        return out
    for alias in PREFIX_ALIASES.get(prefix, []):
        out.append(normalize_course_number(f"{alias} {num}"))
    return list(dict.fromkeys(out))


def is_custom_non_coi(course_number: str) -> bool:
    s = " ".join(str(course_number or "").lower().split())
    if not re.search(r"\d{3}", s):
        return True
    prefix, _ = split_prefix_number(s)
    if not prefix:
        return True
    return prefix in NON_COI_CUSTOM_PREFIXES


def select_period_value(counter: Counter) -> tuple[int | None, str]:
    if not counter:
        return None, "no_match"
    if len(counter) == 1:
        val = next(iter(counter.keys()))
        return int(val), "exact"
    ordered = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
    return int(ordered[0][0]), f"conflict:{dict(counter)}"


def main() -> None:
    ap = argparse.ArgumentParser(description="Populate courses.period_count from COI X(Y) entries")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--coi-text", type=Path, default=DEFAULT_COI_TEXT)
    ap.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    ap.add_argument("--version-id", type=str, default="")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.coi_text.exists():
        raise SystemExit(f"Missing COI text file: {args.coi_text}")
    if not args.db.exists():
        raise SystemExit(f"Missing database file: {args.db}")

    parsed = parse_coi_period_counts(args.coi_text.read_text(encoding="utf-8", errors="ignore"))
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    version_id = args.version_id.strip()
    if not version_id:
        row = cur.execute(
            "select id from curriculum_versions where status='ACTIVE' order by created_at desc limit 1"
        ).fetchone()
        if not row:
            raise SystemExit("No ACTIVE version found and no --version-id provided")
        version_id = str(row["id"])

    courses = cur.execute(
        "select id, course_number, title, period_count from courses where version_id=? order by course_number asc",
        (version_id,),
    ).fetchall()

    updates = 0
    exact = 0
    conflicts = 0
    unmatched = 0
    report_rows: list[dict[str, str]] = []

    for c in courses:
        course_id = str(c["id"])
        course_number = normalize_course_number(c["course_number"])
        counts = Counter()
        for cand in build_alias_candidates(course_number):
            counts.update(parsed.get(cand, Counter()))
        selected, status = select_period_value(counts)
        if status == "no_match" and is_custom_non_coi(c["course_number"]):
            status = "no_match_custom"
        if status == "exact":
            exact += 1
        elif status.startswith("conflict"):
            conflicts += 1
        else:
            unmatched += 1

        current = c["period_count"]
        changed = selected is not None and current != selected
        if changed and not args.dry_run:
            cur.execute("update courses set period_count=? where id=?", (int(selected), course_id))
            updates += 1
        elif changed and args.dry_run:
            updates += 1

        report_rows.append(
            {
                "course_id": course_id,
                "course_number": str(c["course_number"]),
                "title": str(c["title"] or ""),
                "current_period_count": "" if current is None else str(current),
                "selected_period_count": "" if selected is None else str(selected),
                "status": status,
                "candidate_counts": str(dict(counts)) if counts else "",
                "changed": "yes" if changed else "no",
            }
        )

    if not args.dry_run:
        con.commit()
    con.close()

    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "course_id",
                "course_number",
                "title",
                "current_period_count",
                "selected_period_count",
                "status",
                "candidate_counts",
                "changed",
            ],
        )
        w.writeheader()
        w.writerows(report_rows)

    print(f"Version: {version_id}")
    print(f"Courses scanned: {len(courses)}")
    print(f"Exact matches: {exact}")
    print(f"Conflicts (mode selected): {conflicts}")
    print(f"Unmatched: {unmatched}")
    print(f"{'Would update' if args.dry_run else 'Updated'} rows: {updates}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
