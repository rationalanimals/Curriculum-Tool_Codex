import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
os.chdir(BACKEND)
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import (  # noqa: E402
    AcademicProgram,
    CurriculumVersion,
    SessionLocal,
    SuggestedCanvasSequence,
    select,
)


SEQUENCE_DIR = ROOT / "docs" / "canvas-sequences"
DEFAULT_VERSION_NAME = "COI 2025-2026"


def load_sequence_file(path: Path) -> dict:
    raw = json.loads(path.read_text(encoding="utf-8"))
    payload = raw.get("payload") or {}
    canvas = payload.get("canvas") or {}
    items = canvas.get("items") or []
    if not isinstance(items, list):
        items = []
    major = str(raw.get("major") or "").strip() or None
    name = str(raw.get("name") or path.stem).strip()
    source_document = str(raw.get("source_document") or "").strip() or None
    section_title = str(raw.get("source_section_title") or "").strip() or None
    has_genr = any(str(x.get("course_number") or "").upper().startswith("GENR ") for x in items if isinstance(x, dict))
    options_note = (
        "Includes GENR placeholder option slots for COI open-choice requirements."
        if has_genr
        else None
    )
    return {
        "name": name,
        "major_name": major,
        "source_document": source_document,
        "source_section_title": section_title,
        "options_note": options_note,
        "items": items,
    }


def ingest(version_name: str = DEFAULT_VERSION_NAME) -> None:
    if not SEQUENCE_DIR.exists():
        raise RuntimeError(f"Sequence directory not found: {SEQUENCE_DIR}")
    db = SessionLocal()
    try:
        version = db.scalar(select(CurriculumVersion).where(CurriculumVersion.name == version_name))
        if not version:
            raise RuntimeError(f"Version not found: {version_name}")

        for row in db.scalars(select(SuggestedCanvasSequence).where(SuggestedCanvasSequence.version_id == version.id)).all():
            db.delete(row)
        db.flush()

        files = sorted([p for p in SEQUENCE_DIR.glob("*.json") if p.is_file()])
        created = 0
        majors_with_sequences = set()
        for idx, path in enumerate(files):
            row = load_sequence_file(path)
            obj = SuggestedCanvasSequence(
                version_id=version.id,
                name=row["name"],
                major_name=row["major_name"],
                source_document=row["source_document"],
                source_section_title=row["source_section_title"],
                options_note=row["options_note"],
                items_json=json.dumps(row["items"]),
                sort_order=idx,
            )
            db.add(obj)
            created += 1
            if row["major_name"]:
                majors_with_sequences.add(row["major_name"].strip().lower())

        db.commit()

        major_programs = db.scalars(
            select(AcademicProgram).where(
                AcademicProgram.version_id == version.id,
                AcademicProgram.program_type == "MAJOR",
            )
        ).all()
        all_major_names = sorted({str(p.name or "").strip() for p in major_programs if str(p.name or "").strip()})
        missing = [m for m in all_major_names if m.strip().lower() not in majors_with_sequences]

        print(f"Ingested suggested sequences: {created}")
        print(f"Version: {version.name} ({version.id})")
        print(f"Major programs: {len(all_major_names)}")
        print(f"Majors with suggested sequences: {len(majors_with_sequences)}")
        print(f"Majors missing suggested sequences: {len(missing)}")
        for name in missing:
            print(f"- {name}")
    finally:
        db.close()


if __name__ == "__main__":
    ver = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_VERSION_NAME
    ingest(ver)

