import json
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import (  # noqa: E402
    AcademicProgram,
    CanvasSequenceImportIn,
    CanvasSequenceItemIn,
    Course,
    CurriculumVersion,
    PlanItem,
    Requirement,
    RequirementFulfillment,
    RequirementSubstitution,
    SessionLocal,
    apply_canvas_sequence_import,
    design_checklist,
    design_feasibility,
    normalize_course_number,
    select,
)


MAJORS = [
    "Aeronautical Engineering",
    "Astronautical Engineering",
    "Basic Sciences",
    "Behavioral Sciences",
    "Biology",
    "Chemistry",
    "Civil Engineering",
    "Computer Science",
    "Cyber Science",
    "Data Science",
    "Economics",
    "Electrical And Computer Engineering",
    "English",
    "Foreign Area Studies",
    "General Engineering",
    "Geospatial Science",
    "History",
    "Humanities",
    "Legal Studies",
    "Management",
    "Mathematics",
    "Mechanical Engineering",
    "Meteorology",
    "Military & Strategic Studies",
    "Operations Research",
    "Philosophy",
    "Physics",
    "Political Science",
    "Social Sciences",
    "Systems Engineering",
]

MINORS = [
    "Aerospace Materials",
    "Airpower Studies",
    "American Studies",
    "Foreign Language",
    "Future Conflict",
    "Global Logistics Management",
    "High Performance Computing",
    "Nuclear Weapons And Strategy",
    "Philosophy",
    "Pre-Health Professions",
    "Quantum Technologies",
    "Religion Studies",
    "Robotics And Autonomous Systems",
    "Space Warfighting",
    "Sustainability",
]

DIVISION_MAP = {
    "Aeronautical Engineering": "ENGINEERING_SCIENCES",
    "Astronautical Engineering": "ENGINEERING_SCIENCES",
    "Basic Sciences": "BASIC_SCIENCES_AND_MATH",
    "Behavioral Sciences": "SOCIAL_SCIENCES",
    "Biology": "BASIC_SCIENCES_AND_MATH",
    "Chemistry": "BASIC_SCIENCES_AND_MATH",
    "Civil Engineering": "ENGINEERING_SCIENCES",
    "Computer Science": "ENGINEERING_SCIENCES",
    "Cyber Science": "ENGINEERING_SCIENCES",
    "Data Science": "ENGINEERING_SCIENCES",
    "Economics": "SOCIAL_SCIENCES",
    "Electrical And Computer Engineering": "ENGINEERING_SCIENCES",
    "English": "HUMANITIES",
    "Foreign Area Studies": "HUMANITIES",
    "General Engineering": "ENGINEERING_SCIENCES",
    "Geospatial Science": "SOCIAL_SCIENCES",
    "History": "HUMANITIES",
    "Humanities": "HUMANITIES",
    "Legal Studies": "HUMANITIES",
    "Management": "SOCIAL_SCIENCES",
    "Mathematics": "BASIC_SCIENCES_AND_MATH",
    "Mechanical Engineering": "ENGINEERING_SCIENCES",
    "Meteorology": "BASIC_SCIENCES_AND_MATH",
    "Military & Strategic Studies": "SOCIAL_SCIENCES",
    "Operations Research": "BASIC_SCIENCES_AND_MATH",
    "Philosophy": "HUMANITIES",
    "Physics": "BASIC_SCIENCES_AND_MATH",
    "Political Science": "SOCIAL_SCIENCES",
    "Social Sciences": "SOCIAL_SCIENCES",
    "Systems Engineering": "ENGINEERING_SCIENCES",
}

CORE_FALLBACK = {
    "Beh Sci 110",
    "History 100",
    "Math 141",
    "Comp Sci 110",
    "Chem 100",
    "Math 142",
    "Physics 110",
    "MSS 251",
    "English 111",
    "English 211",
    "Econ 201",
    "Pol Sci 211",
    "Philos 210",
    "Aero Engr 315",
    "Soc Sci 311",
    "Law 220",
    "History 300",
    "Astr Engr 310",
    "Math 356",
    "Ldrshp 300",
    "Ldrshp 400",
}


def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def load_major_bundle_items() -> dict[str, list[dict]]:
    seq_dir = ROOT / "docs" / "canvas-sequences"
    out: dict[str, list[dict]] = {}
    for p in seq_dir.glob("* Rec Seq COI2526.json"):
        data = json.loads(p.read_text(encoding="utf-8"))
        major = str(data.get("major") or p.stem.replace(" Rec Seq COI2526", "")).strip()
        items = (data.get("payload") or {}).get("canvas", {}).get("items") or []
        out[major] = items
    return out


def ensure_program(db, version_id: str, name: str, program_type: str, division: str | None) -> AcademicProgram:
    existing = db.scalar(
        select(AcademicProgram).where(
            AcademicProgram.version_id == version_id,
            AcademicProgram.program_type == program_type,
            AcademicProgram.name == name,
        )
    )
    if existing:
        existing.division = division
        return existing
    obj = AcademicProgram(version_id=version_id, name=name, program_type=program_type, division=division)
    db.add(obj)
    db.flush()
    return obj


def ensure_placeholder_course(db, version_id: str, token: str, title: str) -> Course:
    number = f"GENR {token}"
    existing = db.scalar(select(Course).where(Course.version_id == version_id, Course.course_number == number))
    if existing:
        return existing
    obj = Course(version_id=version_id, course_number=number, title=title, credit_hours=3.0, min_section_size=1)
    db.add(obj)
    db.flush()
    return obj


def flatten_unsat(nodes: list[dict]) -> list[str]:
    out = []
    for n in nodes or []:
        if not n.get("is_satisfied", True):
            out.append(n.get("name") or "Unnamed")
        out.extend(flatten_unsat(n.get("children") or []))
    return out


def main() -> None:
    bundle_items_by_major = load_major_bundle_items()
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id

        # Ensure complete COI major/minor program inventory.
        major_programs = {name: ensure_program(db, version_id, name, "MAJOR", DIVISION_MAP.get(name)) for name in MAJORS}
        minor_programs = {
            name: ensure_program(db, version_id, name, "MINOR", DIVISION_MAP.get(name, "HUMANITIES")) for name in MINORS
        }
        db.flush()

        # Rebuild requirement tree from scratch for this version.
        req_rows = db.scalars(select(Requirement).where(Requirement.version_id == version_id)).all()
        req_ids = {r.id for r in req_rows}
        if req_ids:
            for rf in db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_(req_ids))).all():
                db.delete(rf)
            for rs in db.scalars(select(RequirementSubstitution).where(RequirementSubstitution.requirement_id.in_(req_ids))).all():
                db.delete(rs)
            for r in req_rows:
                db.delete(r)
            db.flush()

        # Course lookup for linking by course number.
        courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
        course_id_by_num = {normalize_course_number(c.course_number): c.id for c in courses}

        # Core links from high-frequency sequence courses + fallback.
        freq = Counter()
        for rows in bundle_items_by_major.values():
            seen = set()
            for item in rows:
                cn = normalize_course_number(str(item.get("course_number") or ""))
                if cn:
                    seen.add(cn)
            for cn in seen:
                freq[cn] += 1
        core_candidates = {cn for cn, count in freq.items() if count >= 10}
        core_candidates |= {normalize_course_number(x) for x in CORE_FALLBACK}
        core_course_ids = [course_id_by_num[cn] for cn in sorted(core_candidates) if cn in course_id_by_num]

        sort_order = 0
        core_root = Requirement(
            version_id=version_id,
            parent_requirement_id=None,
            program_id=None,
            name="Core",
            logic_type="ALL_REQUIRED",
            sort_order=sort_order,
            category="CORE",
            major_mode=None,
            track_name=None,
        )
        sort_order += 1
        db.add(core_root)
        db.flush()
        for idx, cid in enumerate(core_course_ids):
            db.add(
                RequirementFulfillment(
                    requirement_id=core_root.id,
                    course_id=cid,
                    is_primary=(idx == 0),
                    sort_order=idx,
                )
            )

        # Build major + minor requirements.
        for name, prog in {**major_programs, **minor_programs}.items():
            root = Requirement(
                version_id=version_id,
                parent_requirement_id=None,
                program_id=prog.id,
                name=f"{prog.program_type.title()} - {name}",
                logic_type="ALL_REQUIRED",
                sort_order=sort_order,
                category=prog.program_type,
                major_mode=None,
                track_name=None,
            )
            sort_order += 1
            db.add(root)
            db.flush()

            child_label = "Major Requirement: All Required" if prog.program_type == "MAJOR" else "Minor Requirement: All Required"
            child = Requirement(
                version_id=version_id,
                parent_requirement_id=root.id,
                program_id=prog.id,
                name=child_label,
                logic_type="ALL_REQUIRED",
                sort_order=0,
                category=prog.program_type,
                major_mode="REQUIREMENT",
                track_name=None,
            )
            db.add(child)
            db.flush()

            linked = []
            if prog.program_type == "MAJOR":
                rows = bundle_items_by_major.get(name, [])
                seen_nums = []
                seen_set = set()
                for item in rows:
                    cn = normalize_course_number(str(item.get("course_number") or ""))
                    if cn and cn not in seen_set:
                        seen_set.add(cn)
                        seen_nums.append(cn)
                for cn in seen_nums:
                    cid = course_id_by_num.get(cn)
                    if cid:
                        linked.append(cid)

            if not linked:
                token = f"{100 + (sort_order % 800)}"
                ph = ensure_placeholder_course(db, version_id, token, f"{name} Option Placeholder")
                linked = [ph.id]

            for idx, cid in enumerate(linked):
                db.add(
                    RequirementFulfillment(
                        requirement_id=child.id,
                        course_id=cid,
                        is_primary=(idx == 0),
                        sort_order=idx,
                    )
                )

        db.commit()

        # Per-major QC: import major canvas sequence and evaluate checklist + feasibility.
        report = {
            "version_id": version_id,
            "version_name": active.name,
            "generated_at": None,
            "majors": [],
        }
        for name in MAJORS:
            prog = major_programs.get(name)
            if not prog:
                continue
            rows = bundle_items_by_major.get(name, [])
            items = [CanvasSequenceItemIn(**x) for x in rows if x.get("course_number")]
            if items:
                payload = CanvasSequenceImportIn(name=f"{name} Recommended Sequence", replace_existing=True, items=items)
                apply_canvas_sequence_import(version_id, payload, db)
            else:
                for pi in db.scalars(select(PlanItem).where(PlanItem.version_id == version_id)).all():
                    db.delete(pi)
                db.commit()

            checklist = design_checklist(version_id, program_ids=prog.id, include_core=True, db=db, _=None)
            unsat = flatten_unsat(checklist.get("items") or [])
            val_fails = [x for x in (checklist.get("validation_items") or []) if str(x.get("status") or "").upper() == "FAIL"]

            feas = design_feasibility(version_id, db, None)
            row = None
            for r in feas.get("rows") or []:
                ids = r.get("program_ids") or []
                if len(ids) == 1 and ids[0] == prog.id:
                    row = r
                    break
            report["majors"].append(
                {
                    "major": name,
                    "bundle_present": bool(rows),
                    "program_feasibility_status": (row or {}).get("status"),
                    "program_feasibility_consistency": (row or {}).get("consistency_status"),
                    "program_feasibility_issue_count": len((row or {}).get("issues") or []),
                    "course_of_study_unsatisfied_count": len(unsat),
                    "course_of_study_unsatisfied_examples": unsat[:8],
                    "course_of_study_validation_fail_count": len(val_fails),
                }
            )

        report["generated_at"] = str(Path().resolve())
        out_path = ROOT / "docs" / "coi_manual_population_report.json"
        out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Wrote {out_path}")
        major_pass = sum(
            1
            for x in report["majors"]
            if str(x.get("program_feasibility_status") or "").upper() == "PASS"
            and str(x.get("program_feasibility_consistency") or "").upper() == "CONSISTENT"
        )
        print(f"Major feasibility pass/consistent: {major_pass}/{len(report['majors'])}")


if __name__ == "__main__":
    main()
