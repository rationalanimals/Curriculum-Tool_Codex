import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
SEQUENCE_DIR = ROOT / "docs" / "canvas-sequences"
EXPECTED_TOTALS_FILE = SEQUENCE_DIR / "expected_semester_credits.json"
os.chdir(BACKEND)
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import (  # noqa: E402
    CanvasSequenceItemIn,
    Course,
    CurriculumVersion,
    SessionLocal,
    SuggestedCanvasSequence,
    normalize_course_number,
    select,
)


def parse_args():
    p = argparse.ArgumentParser(description="QC + import one suggested sequence JSON into DB.")
    p.add_argument("--version", default="COI 2025-2026", help="CurriculumVersion.name")
    p.add_argument("--file", help="Path to a single sequence JSON")
    p.add_argument("--major", help="Major name; resolves to '<Major> Rec Seq COI2526.json'")
    p.add_argument("--clear-all", action="store_true", help="Delete all suggested sequences for this version before import")
    p.add_argument("--clear-only", action="store_true", help="Delete all suggested sequences for this version and exit")
    p.add_argument("--apply", action="store_true", help="Actually write to DB (default is dry-run QC)")
    p.add_argument("--force", action="store_true", help="Allow import even when QC warnings are present")
    return p.parse_args()


def load_file_from_args(args) -> Path:
    if args.file:
        path = Path(args.file).resolve()
        if not path.exists():
            raise RuntimeError(f"File not found: {path}")
        return path
    if args.major:
        name = f"{args.major} Rec Seq COI2526.json"
        path = SEQUENCE_DIR / name
        if not path.exists():
            # fallback: case-insensitive match
            candidates = [p for p in SEQUENCE_DIR.glob("*.json") if p.name.lower() == name.lower()]
            if candidates:
                return candidates[0]
            raise RuntimeError(f"Could not resolve major sequence file for: {args.major}")
        return path
    raise RuntimeError("Provide --file or --major")


def qc_sequence(raw: dict, course_by_number: dict[str, str]) -> tuple[dict, list[dict]]:
    canvas = (raw.get("payload") or {}).get("canvas") or {}
    items = canvas.get("items") or []
    if not isinstance(items, list):
        items = []
    warnings = []

    total = 0
    matched = 0
    unmatched = set()
    genr = 0
    seen_pos = set()
    dup_pos = []
    normalized_items = []
    for row in items:
        if not isinstance(row, dict):
            continue
        try:
            parsed = CanvasSequenceItemIn(**row)
            obj = parsed.model_dump()
        except Exception:
            continue
        total += 1
        sem = int(obj.get("semester_index"))
        pos = int(obj.get("position") if obj.get("position") is not None else 0)
        if (sem, pos) in seen_pos:
            dup_pos.append((sem, pos))
        seen_pos.add((sem, pos))
        cn = str(obj.get("course_number") or "").strip()
        if cn.upper().startswith("GENR "):
            genr += 1
        if cn:
            key = normalize_course_number(cn)
            if key in course_by_number:
                matched += 1
            else:
                unmatched.add(cn)
        normalized_items.append(obj)

    if total == 0:
        warnings.append({"type": "empty_sequence", "message": "No valid canvas items found."})
    if unmatched:
        warnings.append(
            {
                "type": "unmatched_courses",
                "message": f"{len(unmatched)} course numbers not found in version catalog.",
                "samples": sorted(list(unmatched))[:20],
            }
        )
    if dup_pos:
        warnings.append(
            {
                "type": "duplicate_semester_positions",
                "message": f"{len(dup_pos)} duplicate (semester,position) pairs.",
                "samples": dup_pos[:20],
            }
        )
    summary = {
        "total_items": total,
        "matched_course_numbers": matched,
        "match_rate_percent": round((matched / total * 100.0), 1) if total else 0.0,
        "unmatched_count": len(unmatched),
        "genr_placeholder_count": genr,
    }
    return summary, warnings, normalized_items


def period_to_semester_label(period_idx: int) -> str:
    mapping = {
        1: "S1",
        2: "S2",
        6: "S3",
        7: "S4",
        11: "S5",
        12: "S6",
        16: "S7",
        17: "S8",
    }
    return mapping.get(period_idx, f"P{period_idx}")


def load_expected_totals(major_name: str | None) -> dict[str, float]:
    if not major_name or not EXPECTED_TOTALS_FILE.exists():
        return {}
    try:
        raw = json.loads(EXPECTED_TOTALS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    major_map = raw.get("majors") or {}
    entry = major_map.get(major_name) or major_map.get(str(major_name).strip())
    if not isinstance(entry, dict):
        return {}
    out = {}
    for k, v in entry.items():
        if isinstance(v, (int, float)):
            out[str(k).upper()] = float(v)
    return out


def main():
    args = parse_args()
    db = SessionLocal()
    try:
        version = db.scalar(select(CurriculumVersion).where(CurriculumVersion.name == args.version))
        if not version:
            raise RuntimeError(f"Version not found: {args.version}")
        if args.clear_only:
            for row in db.scalars(select(SuggestedCanvasSequence).where(SuggestedCanvasSequence.version_id == version.id)).all():
                db.delete(row)
            db.commit()
            print(f"Cleared all suggested sequences for version: {version.name}")
            return

        path = load_file_from_args(args)
        raw = json.loads(path.read_text(encoding="utf-8"))

        courses = db.scalars(select(Course).where(Course.version_id == version.id)).all()
        course_by_number = {normalize_course_number(c.course_number): c.id for c in courses}
        summary, warnings, normalized_items = qc_sequence(raw, course_by_number)

        by_number = {normalize_course_number(c.course_number): c for c in courses}
        period_totals = {}
        for row in normalized_items:
            sem = int(row.get("semester_index"))
            cn = normalize_course_number(row.get("course_number") or "")
            c = by_number.get(cn)
            if not c:
                continue
            period_totals[sem] = period_totals.get(sem, 0.0) + float(c.credit_hours or 0.0)
        semester_totals = {period_to_semester_label(k): round(v, 2) for k, v in sorted(period_totals.items())}
        summary["semester_totals"] = semester_totals

        expected = load_expected_totals(raw.get("major"))
        if expected:
            mismatches = []
            for sem_label, exp in expected.items():
                actual = semester_totals.get(sem_label)
                if actual is None:
                    mismatches.append({"semester": sem_label, "expected": exp, "actual": None})
                elif abs(float(actual) - float(exp)) > 0.01:
                    mismatches.append({"semester": sem_label, "expected": exp, "actual": actual})
            if mismatches:
                warnings.append(
                    {
                        "type": "semester_credit_mismatch",
                        "message": f"{len(mismatches)} semester credit total mismatches versus expected profile.",
                        "samples": mismatches[:20],
                    }
                )

        print(f"File: {path.name}")
        print(f"Version: {version.name} ({version.id})")
        print(f"QC summary: {summary}")
        if warnings:
            print("QC warnings:")
            for w in warnings:
                print(f"- {w['type']}: {w['message']}")
                if w.get("samples"):
                    print(f"  samples: {w['samples']}")
        else:
            print("QC warnings: none")

        if not args.apply:
            print("Dry-run only. Use --apply to write.")
            return

        if warnings and not args.force:
            raise RuntimeError("QC warnings present. Re-run with --force to import anyway.")

        if args.clear_all:
            for row in db.scalars(select(SuggestedCanvasSequence).where(SuggestedCanvasSequence.version_id == version.id)).all():
                db.delete(row)
            db.flush()

        name = str(raw.get("name") or path.stem).strip()
        major = str(raw.get("major") or "").strip() or None
        source_doc = str(raw.get("source_document") or "").strip() or None
        source_section = str(raw.get("source_section_title") or "").strip() or None
        options_note = (
            "Includes GENR placeholder option slots for COI open-choice requirements."
            if summary.get("genr_placeholder_count", 0) > 0
            else None
        )
        existing = db.execute(
            select(SuggestedCanvasSequence).where(
                SuggestedCanvasSequence.version_id == version.id,
                SuggestedCanvasSequence.name == name,
            )
        ).scalar_one_or_none()
        if existing:
            db.delete(existing)
            db.flush()
        max_sort = db.scalar(
            select(SuggestedCanvasSequence.sort_order)
            .where(SuggestedCanvasSequence.version_id == version.id)
            .order_by(SuggestedCanvasSequence.sort_order.desc())
            .limit(1)
        )
        obj = SuggestedCanvasSequence(
            version_id=version.id,
            name=name,
            major_name=major,
            source_document=source_doc,
            source_section_title=source_section,
            options_note=options_note,
            items_json=json.dumps(normalized_items),
            sort_order=(int(max_sort) + 1) if max_sort is not None else 0,
        )
        db.add(obj)
        db.commit()
        print(f"Imported suggested sequence: {name} (items={len(normalized_items)})")
    finally:
        db.close()


if __name__ == "__main__":
    main()
