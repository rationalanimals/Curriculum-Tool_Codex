import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IN_FILE = ROOT / "docs" / "unresolved_course_references.csv"
OUT_FILE = ROOT / "docs" / "course_curation_from_unresolved_refs.csv"


def main() -> None:
    if not IN_FILE.exists():
        raise SystemExit(f"Missing input report: {IN_FILE}")

    rows = list(csv.DictReader(IN_FILE.open("r", encoding="utf-8-sig", newline="")))
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for r in rows:
        num = str(r.get("raw_reference") or "").strip()
        if not num or num in seen:
            continue
        seen.add(num)
        out.append(
            {
                "action": "UPSERT",
                "course_id": "",
                "course_number": num,
                "current_title": "",
                "current_credit_hours": "",
                "new_title": f"{num} (TBD Curate)",
                "new_credit_hours": "3.0",
                "new_designated_semester": "",
                "new_offered_periods_json": "",
                "new_standing_requirement": "",
                "new_additional_requirements_text": "Auto-seeded from unresolved populate reference; requires COI curation.",
                "prereq_numbers_semicolon": "",
                "coreq_numbers_semicolon": "",
                "notes": f"source_script={r.get('script','')}",
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
        w.writerows(out)
    print(f"Wrote {len(out)} UPSERT rows to {OUT_FILE}")


if __name__ == "__main__":
    main()

