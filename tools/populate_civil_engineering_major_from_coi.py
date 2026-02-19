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
    Course,
    CourseBasket,
    CourseBasketItem,
    CurriculumVersion,
    Requirement,
    RequirementBasketLink,
    RequirementFulfillment,
    RequirementSubstitution,
    SessionLocal,
    ValidationRule,
    normalize_course_number,
    select,
)

from populate_ref_utils import resolve_course_ids_strict  # noqa: E402


PROGRAM_NAME = "Civil Engineering"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "ENGINEERING_SCIENCES"

MANDATORY_MAIN = [
    "Civ Engr 330",
    "Civ Engr 351",
    "Civ Engr 361",
    "Civ Engr 362",
    "Civ Engr 372",
    "Civ Engr 385",
    "Civ Engr 390",
    "Civ Engr 451",
    "Civ Engr 452",
    "Civ Engr 461",
    "Civ Engr 485",
    "Civ Engr 488",
    "Civ Engr 405",
]
STRUCTURAL_OPTIONS = ["Civ Engr 373", "Civ Engr 474"]
DESIGN_OPTIONS = ["Civ Engr 373", "Civ Engr 462", "Civ Engr 463", "Civ Engr 464", "Civ Engr 473", "Civ Engr 474", "Civ Engr 493", "Civ Engr 496"]


def find_course_ids_by_number(db, version_id: str) -> dict[str, str]:
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    return {normalize_course_number(c.course_number): c.id for c in courses}


def require_course_ids(course_id_by_num: dict[str, str], numbers: list[str]) -> list[str]:
    missing = [n for n in numbers if normalize_course_number(n) not in course_id_by_num]
    if missing:
        raise RuntimeError(f"Missing required courses in catalog: {missing}")
    return [course_id_by_num[normalize_course_number(n)] for n in numbers]


def optional_course_ids(course_id_by_num: dict[str, str], numbers: list[str]) -> list[str]:
    return [course_id_by_num[normalize_course_number(n)] for n in numbers if normalize_course_number(n) in course_id_by_num]


def ensure_program(db, version_id: str) -> AcademicProgram:
    program = db.scalar(
        select(AcademicProgram).where(
            AcademicProgram.version_id == version_id,
            AcademicProgram.program_type == PROGRAM_TYPE,
            AcademicProgram.name == PROGRAM_NAME,
        )
    )
    if program:
        program.division = PROGRAM_DIVISION
        return program
    program = AcademicProgram(
        version_id=version_id,
        name=PROGRAM_NAME,
        program_type=PROGRAM_TYPE,
        division=PROGRAM_DIVISION,
    )
    db.add(program)
    db.flush()
    return program


def cleanup_program_requirements(db, version_id: str, program_id: str) -> None:
    reqs = db.scalars(select(Requirement).where(Requirement.version_id == version_id, Requirement.program_id == program_id)).all()
    if not reqs:
        return
    req_ids = [r.id for r in reqs]
    basket_links = db.scalars(select(RequirementBasketLink).where(RequirementBasketLink.requirement_id.in_(req_ids))).all()
    basket_ids = [x.basket_id for x in basket_links]
    for row in db.scalars(select(RequirementSubstitution).where(RequirementSubstitution.requirement_id.in_(req_ids))).all():
        db.delete(row)
    for row in db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_(req_ids))).all():
        db.delete(row)
    for row in basket_links:
        db.delete(row)
    for row in reqs:
        db.delete(row)
    db.flush()

    for basket_id in set(basket_ids):
        still_linked = db.scalar(select(RequirementBasketLink).where(RequirementBasketLink.basket_id == basket_id))
        if still_linked:
            continue
        for item in db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id == basket_id)).all():
            db.delete(item)
        basket = db.get(CourseBasket, basket_id)
        if basket:
            db.delete(basket)


def create_requirement(
    db,
    *,
    version_id: str,
    program_id: str,
    parent_id: str | None,
    name: str,
    logic_type: str,
    sort_order: int,
    category: str = "MAJOR",
    major_mode: str | None = None,
    track_name: str | None = None,
    pick_n: int | None = None,
) -> Requirement:
    row = Requirement(
        version_id=version_id,
        parent_requirement_id=parent_id,
        program_id=program_id,
        name=name,
        logic_type=logic_type,
        pick_n=pick_n,
        sort_order=sort_order,
        category=category,
        major_mode=major_mode,
        track_name=track_name,
    )
    db.add(row)
    db.flush()
    return row


def link_courses_all_required(db, requirement_id: str, course_ids: list[str]) -> None:
    for idx, cid in enumerate(course_ids):
        db.add(
            RequirementFulfillment(
                requirement_id=requirement_id,
                course_id=cid,
                is_primary=(idx == 0),
                sort_order=idx,
            )
        )


def create_basket_with_items(
    db,
    *,
    version_id: str,
    name: str,
    description: str,
    course_ids: list[str],
) -> CourseBasket:
    unique_ids = list(dict.fromkeys(course_ids))
    max_sort = db.scalar(
        select(CourseBasket.sort_order)
        .where(CourseBasket.version_id == version_id)
        .order_by(CourseBasket.sort_order.desc())
        .limit(1)
    )
    basket = CourseBasket(
        version_id=version_id,
        name=name,
        description=description,
        sort_order=(max_sort + 1) if max_sort is not None else 0,
    )
    db.add(basket)
    db.flush()
    for idx, cid in enumerate(unique_ids):
        db.add(CourseBasketItem(basket_id=basket.id, course_id=cid, sort_order=idx))
    return basket


def attach_basket(db, requirement_id: str, basket_id: str, min_count: int = 1) -> None:
    db.add(
        RequirementBasketLink(
            requirement_id=requirement_id,
            basket_id=basket_id,
            min_count=min_count,
            max_count=None,
            sort_order=0,
        )
    )


def find_core_req_id(db, version_id: str, req_name: str) -> str:
    row = db.scalar(
        select(Requirement).where(
            Requirement.version_id == version_id,
            Requirement.program_id.is_(None),
            Requirement.category == "CORE",
            Requirement.name == req_name,
        )
    )
    if not row:
        raise RuntimeError(f"Core requirement not found: {req_name}")
    return row.id


def recreate_ce_core_rules_validation_rule(db, version_id: str, program: AcademicProgram, course_id_by_num: dict[str, str]) -> None:
    existing = db.scalars(select(ValidationRule)).all()
    for vr in existing:
        cfg = {}
        try:
            cfg = json.loads(vr.config_json or "{}")
        except Exception:
            cfg = {}
        t = str(cfg.get("type") or "").upper()
        if t not in {"MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"}:
            continue
        pid = cfg.get("program_id")
        pname = str(cfg.get("program_name") or "").strip().lower()
        if pid == program.id or pname in {program.name.lower(), "civil engineering"}:
            db.delete(vr)
    db.flush()

    req_inter_sci = find_core_req_id(db, version_id, "Track - Intermediate Science: Pick N")
    req_inter_stats = find_core_req_id(db, version_id, "Track - Intermediate Stats: Any One")
    req_adv_stem = find_core_req_id(db, version_id, "Track - Advanced STEM: Any One")
    req_adv_open = find_core_req_id(db, version_id, "Track - Advanced: Any One")

    p1_nums = [n for n in ["Biology 215", "Physics 215"] if normalize_course_number(n) in course_id_by_num]
    if not p1_nums:
        return

    cfg = {
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": program.id,
        "program_name": program.name,
        "required_core_groups": [
            {
                "name": "Track - Intermediate Science: Pick 2 - Choice 1",
                "min_count": 1,
                "course_numbers": p1_nums,
                "source_requirement_id": req_inter_sci,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
            {
                "name": "Track - Intermediate Science: Pick 2 - Choice 2",
                "min_count": 1,
                "course_numbers": ["Chem 200"],
                "source_requirement_id": req_inter_sci,
                "slot_index": 1,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
            {
                "name": "Track - Intermediate Stats: Any One - Choice 1",
                "min_count": 1,
                "course_numbers": ["Math 356"],
                "source_requirement_id": req_inter_stats,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
            {
                "name": "Track - Advanced STEM: Any One - Choice 1",
                "min_count": 1,
                "course_numbers": ["Math 243", "Math 253"],
                "source_requirement_id": req_adv_stem,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
            {
                "name": "Track - Advanced: Any One - Choice 1",
                "min_count": 1,
                "course_numbers": ["Math 245"],
                "source_requirement_id": req_adv_open,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
        ],
    }
    db.add(
        ValidationRule(
            name="Program Pathway - Civil Engineering",
            tier=2,
            severity="FAIL",
            active=True,
            config_json=json.dumps(cfg),
        )
    )


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id

        program = ensure_program(db, version_id)
        cleanup_program_requirements(db, version_id, program.id)

        course_id_by_num = find_course_ids_by_number(db, version_id)
        mandatory_ids = require_course_ids(course_id_by_num, MANDATORY_MAIN)
        structural_ids = optional_course_ids(course_id_by_num, STRUCTURAL_OPTIONS)
        design_ids = optional_course_ids(course_id_by_num, DESIGN_OPTIONS)

        # Engineering Option: any engineering-division course (3.0+ hrs) not already fixed in mandatory core of CE.
        all_courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
        prefixes = ("Aero Engr ", "Astr Engr ", "Civ Engr ", "ECE ", "Engr ", "Mech Engr ", "Systems Engr ")
        mandatory_set = set(mandatory_ids)
        engineering_option_ids = [
            c.id
            for c in all_courses
            if float(c.credit_hours or 0.0) >= 3.0
            and str(c.course_number or "").startswith(prefixes)
            and c.id not in mandatory_set
        ]

        root = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=None,
            name="Major - Civil Engineering",
            logic_type="ALL_REQUIRED",
            sort_order=0,
            category="MAJOR",
        )
        major_req = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Major Requirement: All Required",
            logic_type="ALL_REQUIRED",
            sort_order=0,
            category="MAJOR",
            major_mode="REQUIREMENT",
        )
        link_courses_all_required(db, major_req.id, mandatory_ids)

        option_nodes = [
            ("Track - Structural Design Option: Any One", structural_ids, "ANY_ONE", None, 1),
            ("Track - Design Option: Pick N", design_ids, "PICK_N", 2, 2),
            ("Track - Engineering Option: Any One", engineering_option_ids, "ANY_ONE", None, 3),
        ]
        for node_name, ids, logic_type, pick_n, sort_order in option_nodes:
            req = create_requirement(
                db,
                version_id=version_id,
                program_id=program.id,
                parent_id=root.id,
                name=node_name,
                logic_type=logic_type,
                pick_n=pick_n,
                sort_order=sort_order,
                category="MAJOR",
                major_mode="TRACK",
                track_name=node_name.replace("Track - ", "").replace(": Any One", "").replace(": Pick N", ""),
            )
            basket = create_basket_with_items(
                db,
                version_id=version_id,
                name=f"Civil Engineering Major - {node_name} Options",
                description=f"COI 2025-2026 {PROGRAM_NAME}: {node_name}",
                course_ids=ids,
            )
            attach_basket(db, req.id, basket.id, min_count=(pick_n or 1))

        recreate_ce_core_rules_validation_rule(db, version_id, program, course_id_by_num)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")
        print("Created structure:")
        print("- Major Requirement: All Required (13 mandatory CE courses)")
        print("- Structural Design Option: Any One")
        print("- Design Option: Pick 2")
        print("- Engineering Option: Any One")
        print("Added/updated major-specific Core Rules validation rule: Program Pathway - Civil Engineering")


if __name__ == "__main__":
    main()
