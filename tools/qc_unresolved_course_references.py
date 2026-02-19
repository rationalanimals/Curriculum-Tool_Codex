import csv
import glob
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
os.chdir(BACKEND)
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import Course, CurriculumVersion, SessionLocal, normalize_course_number, select  # noqa: E402
from populate_ref_utils import resolve_single_course_id  # noqa: E402


COURSE_LITERAL_RE = re.compile(r"\"([A-Za-z][A-Za-z &/.-]*\s\d{3}[A-Z]?)\"")


def iter_populate_scripts() -> list[Path]:
    scripts = []
    scripts.extend(Path("..").glob("tools/populate*_from_coi.py"))
    scripts.extend(Path("..").glob("tools/populate_minors_batch*_from_coi.py"))
    return sorted(set(scripts))


def main() -> None:
    with SessionLocal() as db:
        version = db.scalar(
            select(CurriculumVersion)
            .where(CurriculumVersion.status == "ACTIVE")
            .order_by(CurriculumVersion.created_at.desc())
        ) or db.scalar(select(CurriculumVersion).order_by(CurriculumVersion.created_at.desc()))
        if not version:
            raise RuntimeError("No curriculum version found")
        courses = db.scalars(select(Course).where(Course.version_id == version.id)).all()
        catalog = {normalize_course_number(c.course_number): c.course_number for c in courses}
        by_number_suffix: dict[str, list[str]] = {}
        for c in courses:
            m = re.search(r"(\d{3}[A-Z]?)$", c.course_number.replace(" ", ""))
            if not m:
                continue
            by_number_suffix.setdefault(m.group(1), []).append(c.course_number)

    out_rows: list[dict[str, str]] = []
    for script_path in iter_populate_scripts():
        text = script_path.read_text(encoding="utf-8", errors="ignore")
        seen = set()
        for raw in COURSE_LITERAL_RE.findall(text):
            n = normalize_course_number(raw)
            if n in seen:
                continue
            seen.add(n)
            cid = resolve_single_course_id(catalog, raw, normalize_course_number)
            if cid:
                continue
            suffix = ""
            suffix_candidates: list[str] = []
            m = re.search(r"(\d{3}[A-Z]?)$", raw.replace(" ", ""))
            if m:
                suffix = m.group(1)
                suffix_candidates = sorted(by_number_suffix.get(suffix, []))
            suggestion = "ADD_OR_CURATE_COURSE"
            if suffix_candidates:
                suggestion = "REVIEW_ALIAS_OR_RENAME"
            out_rows.append(
                {
                    "script": str(script_path).replace("\\", "/"),
                    "raw_reference": raw,
                    "normalized_reference": n,
                    "status": "UNRESOLVED",
                    "suggested_action": suggestion,
                    "same_suffix_candidates": " | ".join(suffix_candidates[:8]),
                }
            )

    out_dir = Path("..") / "docs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "unresolved_course_references.csv"
    with out_file.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "script",
                "raw_reference",
                "normalized_reference",
                "status",
                "suggested_action",
                "same_suffix_candidates",
            ],
        )
        w.writeheader()
        w.writerows(out_rows)

    print(f"Wrote {len(out_rows)} unresolved references to {out_file}")


if __name__ == "__main__":
    main()
