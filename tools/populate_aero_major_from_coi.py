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


PROGRAM_NAME = "Aeronautical Engineering"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "ENGINEERING_SCIENCES"


MANDATORY_MAIN = [
    "Mech Engr 330",
    "Mech Engr 350",
    "Aero Engr 241",
    "Aero Engr 341",
    "Aero Engr 342",
    "Aero Engr 351",
    "Aero Engr 352",
    "Aero Engr 361",
    "Aero Engr 436",
    "Aero Engr 442",
    "Aero Engr 471",
]

PROGRAMMING_OPTIONS = ["Aero Engr 206", "Comp Sci 206", "Comp Sci 211"]
ENGINEERING_MATH_OPTIONS = ["Math 346", "Engr 346"]
DESIGN_SEQUENCE_INTRO_OPTIONS = ["Aero Engr 480", "Aero Engr 481"]
DESIGN_ELECTIVE_OPTIONS = ["Aero Engr 482", "Aero Engr 483"]
AERO_ELECTIVE_OPTIONS = [
    "Aero Engr 446",
    "Aero Engr 447",
    "Aero Engr 456",
    "Aero Engr 457",
    "Aero Engr 466",
    "Aero Engr 472",
    "Aero Engr 482",
    "Aero Engr 483",
    "Mech Engr 431",
    "Mech Engr 450",
    "Aero Engr 495",
    "Aero Engr 499",
]


def find_course_ids_by_number(db, version_id: str) -> dict[str, str]:
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    return {normalize_course_number(c.course_number): c.id for c in courses}


def require_course_ids(course_id_by_num: dict[str, str], numbers: list[str]) -> list[str]:
    missing = [n for n in numbers if normalize_course_number(n) not in course_id_by_num]
    if missing:
        raise RuntimeError(f"Missing required courses in catalog: {missing}")
    return [course_id_by_num[normalize_course_number(n)] for n in numbers]


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

    # Delete now-unreferenced baskets that were previously tied to this program.
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
    option_slot_key: str | None = None,
    option_slot_capacity: int | None = None,
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
        option_slot_key=option_slot_key,
        option_slot_capacity=option_slot_capacity,
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
    for idx, cid in enumerate(course_ids):
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


def find_core_adv_liberal_arts_numbers(db, version_id: str) -> list[str]:
    core_req = db.scalar(
        select(Requirement).where(
            Requirement.version_id == version_id,
            Requirement.program_id.is_(None),
            Requirement.category == "CORE",
            Requirement.name == "Track - Advanced Liberal Arts: Any One",
        )
    )
    if not core_req:
        return ["History 300", "Soc Sci 311", "Law 220", "Philos 210"]
    links = db.scalars(select(RequirementBasketLink).where(RequirementBasketLink.requirement_id == core_req.id)).all()
    if not links:
        return ["History 300", "Soc Sci 311", "Law 220", "Philos 210"]
    basket_id = links[0].basket_id
    items = db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id == basket_id).order_by(CourseBasketItem.sort_order.asc())).all()
    if not items:
        return ["History 300", "Soc Sci 311", "Law 220", "Philos 210"]
    out = []
    for it in items:
        c = db.get(Course, it.course_id)
        if c and c.course_number:
            out.append(c.course_number)
    return out or ["History 300", "Soc Sci 311", "Law 220", "Philos 210"]


def recreate_aero_core_rules_validation_rule(db, program: AcademicProgram) -> None:
    # Remove existing major core-pathway rules for this program.
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
        if pid == program.id or pname == program.name.lower():
            db.delete(vr)
    db.flush()

    adv_socio = find_core_adv_liberal_arts_numbers(db, program.version_id)
    cfg = {
        "domain": "Program/Major Pathway",
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": program.id,
        "program_name": program.name,
        "required_core_groups": [
            {"name": "P / C / B Option 1", "min_count": 1, "course_numbers": ["Chem 200"]},
            {"name": "P / C / B Option 2", "min_count": 1, "course_numbers": ["Physics 215"]},
            {"name": "Aero Engr", "min_count": 1, "course_numbers": ["Aero Engr 210", "Aero Engr 210S"]},
            {"name": "Statistics", "min_count": 1, "course_numbers": ["Math 356"]},
            {"name": "Adv STEM Option", "min_count": 1, "course_numbers": ["Math 243", "Math 253"]},
            {"name": "Adv Sociocultural Option", "min_count": 1, "course_numbers": adv_socio},
            {"name": "Adv Open Option", "min_count": 1, "course_numbers": ["Math 245"]},
        ],
    }
    rule = ValidationRule(
        name="Program Pathway - Aeronautical Engineering",
        tier=2,
        severity="FAIL",
        active=True,
        config_json=json.dumps(cfg),
    )
    db.add(rule)


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id

        program = ensure_program(db, version_id)
        cleanup_program_requirements(db, version_id, program.id)

        course_id_by_num = find_course_ids_by_number(db, version_id)
        mandatory_main_ids = require_course_ids(course_id_by_num, MANDATORY_MAIN)
        programming_ids = require_course_ids(course_id_by_num, PROGRAMMING_OPTIONS)
        eng_math_ids = require_course_ids(course_id_by_num, ENGINEERING_MATH_OPTIONS)
        design_intro_ids = require_course_ids(course_id_by_num, DESIGN_SEQUENCE_INTRO_OPTIONS)
        design_elective_ids = require_course_ids(course_id_by_num, DESIGN_ELECTIVE_OPTIONS)
        aero_elective_ids = require_course_ids(course_id_by_num, AERO_ELECTIVE_OPTIONS)

        root = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=None,
            name="Major - Aeronautical Engineering",
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
        link_courses_all_required(db, major_req.id, mandatory_main_ids)

        nodes_with_baskets = [
            ("Track - Programming Requirement: Any One", PROGRAMMING_OPTIONS, programming_ids),
            ("Track - Engineering Math Requirement: Any One", ENGINEERING_MATH_OPTIONS, eng_math_ids),
            ("Track - Design Sequence Intro: Any One", DESIGN_SEQUENCE_INTRO_OPTIONS, design_intro_ids),
            ("Track - Aero Design Elective: Any One", DESIGN_ELECTIVE_OPTIONS, design_elective_ids),
            ("Track - Aero Elective: Any One", AERO_ELECTIVE_OPTIONS, aero_elective_ids),
        ]
        for idx, (node_name, option_numbers, option_ids) in enumerate(nodes_with_baskets, start=1):
            req = create_requirement(
                db,
                version_id=version_id,
                program_id=program.id,
                parent_id=root.id,
                name=node_name,
                logic_type="ANY_ONE",
                sort_order=idx,
                category="MAJOR",
                major_mode="TRACK",
                track_name=node_name.replace("Track - ", "").replace(": Any One", ""),
            )
            basket = create_basket_with_items(
                db,
                version_id=version_id,
                name=f"Aero Major - {node_name} Options",
                description=f"COI 2025-2026 {PROGRAM_NAME}: {', '.join(option_numbers)}",
                course_ids=option_ids,
            )
            attach_basket(db, req.id, basket.id, min_count=1)

        # COI nuance: "other engineering/basic science with department approval".
        # Model this as an explicit option slot so it is visible/editable in the tree.
        create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Dept Approved Engineering/Basic Science: Option Slot (1)",
            logic_type="OPTION_SLOT",
            pick_n=1,
            option_slot_key="AERO_DEPT_APPROVED_ENG_OR_BASIC_SCI",
            option_slot_capacity=1,
            sort_order=len(nodes_with_baskets) + 1,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Dept Approved Engineering/Basic Science",
        )

        recreate_aero_core_rules_validation_rule(db, program)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")
        print("Created structure:")
        print("- Major Requirement: All Required (11 mandatory main courses)")
        print("- Programming Requirement: Any One (Aero Engr 206 / Comp Sci 206 / Comp Sci 211)")
        print("- Engineering Math Requirement: Any One (Math 346 / Engr 346)")
        print("- Design Sequence Intro: Any One (Aero Engr 480 / Aero Engr 481)")
        print("- Aero Design Elective: Any One (Aero Engr 482 / Aero Engr 483)")
        print("- Aero Elective: Any One (12-course supplemental list)")
        print("- Dept Approved Engineering/Basic Science: Option Slot (1)")
        print("Added/updated major-specific Core Rules validation rule: Program Pathway - Aeronautical Engineering")


if __name__ == "__main__":
    main()
