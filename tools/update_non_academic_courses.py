import re
import sys
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
os.chdir(BACKEND)
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import Course, CurriculumVersion, SessionLocal, normalize_course_number, select  # noqa: E402


NON_ACAD_PREFIXES = ("Mil Tng", "Phy Ed", "PE", "AV", "AX", "CE", "CL", "SmrAcad")
PREFIX_CANON = {
    "mil tng": "Mil Tng",
    "phy ed": "Phy Ed",
    "pe": "PE",
    "av": "AV",
    "ax": "AX",
    "ce": "CE",
    "cl": "CL",
    "smracad": "SmrAcad",
}

# Keep these deterministic even when source text is noisy.
TITLE_OVERRIDES = {
    "AV 100": "Introduction to Powered Flight Operations",
    "AX 302": "Pattern Solo (RDI: ROTC - Aim High Flight Academy)",
    "AX 401": "Private Pilot Certificate (PPC) - The Academy",
    "CE 100": "Commissioning Education I",
    "CE 200": "Commissioning Education II",
    "CE 300": "Commissioning Education III",
    "CE 400": "Commissioning Education IV",
    "CL 100": "Character and Leadership I",
    "CL 200": "Character and Leadership II",
    "CL 300": "Character and Leadership III",
    "CL 400": "Character and Leadership IV",
}


def is_non_academic_course_number(course_number: str) -> bool:
    n = normalize_course_number(course_number)
    return bool(re.match(r"^(MIL TNG|PHY ED|PE|AV|AX|CE|CL|SMRACAD)\s+[0-9]{3}[A-Z]?$", n))


def default_credits(course_number: str) -> float:
    n = normalize_course_number(course_number)
    if n.startswith("PHY ED") or n.startswith("PE "):
        return 0.5
    return 0.0


def sanitize_title(raw: str) -> str:
    t = " ".join(str(raw or "").split()).strip(" .")
    if len(t) > 120 and ". " in t:
        t = t.split(". ", 1)[0].strip(" .")
    return t


def parse_non_academic_from_coi(text: str) -> dict[str, dict]:
    pattern = re.compile(
        r"(?i)(?<![A-Za-z])(Mil Tng|Phy Ed|PE|AV|AX|CE|CL|SmrAcad)\s+([0-9]{3}[A-Z]?)\.\s",
        re.MULTILINE,
    )
    matches = list(pattern.finditer(text))
    out: dict[str, dict] = {}
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block = text[start:end]
        raw_prefix = m.group(1).strip().lower()
        prefix = PREFIX_CANON.get(raw_prefix, m.group(1).strip())
        number = m.group(2).strip()
        course_number = f"{prefix} {number}"

        title = ""
        title_start = m.end()
        title_chunk = text[title_start:end]
        # Stop on standard metadata markers.
        splitters = [" 0(", " Sem hrs:", " Prereq:", " Coreq:", " Not Graded.", " Pass/fail."]
        cut = len(title_chunk)
        for s in splitters:
            idx = title_chunk.find(s)
            if idx >= 0:
                cut = min(cut, idx)
        title = sanitize_title(title_chunk[:cut])

        sem_match = re.search(r"(?i)Sem hrs:\s*([0-9]+(?:\.[0-9]+)?)", block)
        credits = float(sem_match.group(1)) if sem_match else default_credits(course_number)

        out[normalize_course_number(course_number)] = {
            "course_number": course_number,
            "title": title,
            "credit_hours": credits,
        }
    return out


def main() -> None:
    coi_path = ROOT / "coi_extracted.txt"
    if not coi_path.exists():
        raise FileNotFoundError(f"Missing {coi_path}")
    text = coi_path.read_text(encoding="utf-8", errors="ignore")
    parsed = parse_non_academic_from_coi(text)

    with SessionLocal() as db:
        active = db.scalar(
            select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc())
        )
        if not active:
            raise RuntimeError("No ACTIVE curriculum version found")
        version_id = active.id

        courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
        by_num = {normalize_course_number(c.course_number): c for c in courses}

        updated = 0
        created = 0
        touched = []

        # Ensure all parsed non-academic courses exist/updated.
        for norm, row in parsed.items():
            course_number = row["course_number"]
            title = TITLE_OVERRIDES.get(course_number, sanitize_title(row["title"]) or course_number)
            credit_hours = row["credit_hours"]
            existing = by_num.get(norm)
            if existing:
                changed = False
                if title and existing.title != title:
                    existing.title = title
                    changed = True
                if existing.credit_hours != credit_hours:
                    existing.credit_hours = credit_hours
                    changed = True
                if existing.min_section_size is None or existing.min_section_size > 1:
                    existing.min_section_size = 1
                    changed = True
                if changed:
                    updated += 1
                    touched.append(course_number)
            else:
                c = Course(
                    version_id=version_id,
                    course_number=course_number,
                    title=title,
                    credit_hours=credit_hours,
                    min_section_size=1,
                )
                db.add(c)
                created += 1
                touched.append(course_number)

        # For existing non-academic courses not parsed from text, enforce sane defaults.
        for c in courses:
            n = normalize_course_number(c.course_number)
            if not is_non_academic_course_number(c.course_number):
                continue
            if n in parsed:
                continue
            override_title = TITLE_OVERRIDES.get(c.course_number)
            target_credits = default_credits(c.course_number)
            changed = False
            if override_title and c.title != override_title:
                c.title = override_title
                changed = True
            if c.credit_hours != target_credits:
                c.credit_hours = target_credits
                changed = True
            if c.min_section_size is None or c.min_section_size > 1:
                c.min_section_size = 1
                changed = True
            if changed:
                updated += 1
                touched.append(c.course_number)

        db.commit()
        print(f"Active version: {active.name}")
        print(f"Non-academic courses updated: {updated}")
        print(f"Non-academic courses created: {created}")
        if touched:
            sample = sorted(set(touched))[:40]
            print("Sample touched:", ", ".join(sample))


if __name__ == "__main__":
    main()
