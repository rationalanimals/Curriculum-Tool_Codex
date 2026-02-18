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


PROGRAM_NAME = "Economics"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "SOCIAL_SCIENCES"

MANDATORY_MAIN = [
    "Econ 332",
    "Econ 333",
    "Econ 350",
    "Econ 361",
    "Econ 365",
    "Econ 465",
    "Econ 450",
    "Econ 440",
]
CAPSTONE_OPTIONS = ["Econ 480", "Econ 482"]

ELECTIVES_1_2 = [
    "Econ 340",
    "Econ 351",
    "Econ 367",
    "Econ 377",
    "Econ 411",
    "Econ 422",
    "Econ 447",
    "Econ 454",
    "Econ 466",
    "Econ 473",
    "Econ 475",
    "Econ 476",
    "Econ 477",
    "Econ 480",
    "Econ 481",
    "Econ 495",
    "Econ 499",
    "Soc Sci 420",
    "Soc Sci 444",
]

ELECTIVES_3_4_EXTRA = [
    "Law 340",
    "Math 243",
    "Math 253",
    "Math 245",
    "Math 320",
    "Math 340",
    "Math 342",
    "Math 344",
    "Math 359",
    "Math 360",
    "Math 366",
    "Math 378",
    "Mgt 337",
    "Mgt 341",
    "Mgt 342",
    "Mgt 375",
    "Mgt 382",
    "Ops Rsch 311",
    "Ops Rsch 312",
    "Ops Rsch 421",
    "Ops Rsch 422",
    "Philos 200",
    "Philos 370",
    "Soc Sci 483",
    "Sys Engr 301",
    "Sys Engr 310",
]


def find_course_ids_by_number(db, version_id: str) -> dict[str, str]:
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    return {normalize_course_number(c.course_number): c.id for c in courses}


def require_course_ids(course_id_by_num: dict[str, str], numbers: list[str]) -> list[str]:
    missing = [n for n in numbers if normalize_course_number(n) not in course_id_by_num]
    if missing:
        raise RuntimeError(f"Missing required courses in catalog: {missing}")
    return [course_id_by_num[normalize_course_number(n)] for n in numbers]


def optional_course_ids(course_id_by_num: dict[str, str], numbers: list[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for n in numbers:
        key = normalize_course_number(n)
        cid = course_id_by_num.get(key)
        if not cid or cid in seen:
            continue
        seen.add(cid)
        out.append(cid)
    return out


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


def recreate_econ_core_rules_validation_rule(db, version_id: str, program: AcademicProgram) -> None:
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
        if pid == program.id or pname in {program.name.lower(), "economics"}:
            db.delete(vr)
    db.flush()

    req_adv_stem = find_core_req_id(db, version_id, "Track - Advanced STEM: Any One")
    cfg = {
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": program.id,
        "program_name": program.name,
        "required_core_groups": [
            {
                "name": "Track - Advanced STEM: Any One - Choice 1",
                "min_count": 1,
                "course_numbers": ["Ops Rsch 310"],
                "source_requirement_id": req_adv_stem,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            }
        ],
    }
    db.add(
        ValidationRule(
            name="Program Pathway - Economics",
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
        capstone_ids = require_course_ids(course_id_by_num, CAPSTONE_OPTIONS)
        elect_12_ids = optional_course_ids(course_id_by_num, ELECTIVES_1_2)
        elect_34_ids = optional_course_ids(course_id_by_num, ELECTIVES_1_2 + ELECTIVES_3_4_EXTRA)

        all_courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
        academy_option_ids = [
            c.id
            for c in all_courses
            if float(c.credit_hours or 0.0) >= 3.0
        ]

        root = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=None,
            name="Major - Economics",
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

        capstone_track = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Capstone: Any One",
            logic_type="ANY_ONE",
            sort_order=1,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Capstone",
        )
        capstone_basket = create_basket_with_items(
            db,
            version_id=version_id,
            name="Economics Major - Capstone Pool",
            description="COI 2025-2026 Economics: capstone requirement",
            course_ids=capstone_ids,
        )
        attach_basket(db, capstone_track.id, capstone_basket.id, min_count=1)

        elective_12_track = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Economics Elective Set 1-2: Pick N",
            logic_type="PICK_N",
            pick_n=2,
            sort_order=2,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Economics Elective Set 1-2",
        )
        elective_12_basket = create_basket_with_items(
            db,
            version_id=version_id,
            name="Economics Major - Elective Set 1-2 Pool",
            description="COI 2025-2026 Economics: electives 1 and 2",
            course_ids=elect_12_ids,
        )
        attach_basket(db, elective_12_track.id, elective_12_basket.id, min_count=2)

        elective_34_track = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Economics Elective Set 3-4: Pick N",
            logic_type="PICK_N",
            pick_n=2,
            sort_order=3,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Economics Elective Set 3-4",
        )
        elective_34_basket = create_basket_with_items(
            db,
            version_id=version_id,
            name="Economics Major - Elective Set 3-4 Pool",
            description="COI 2025-2026 Economics: electives 3 and 4",
            course_ids=elect_34_ids,
        )
        attach_basket(db, elective_34_track.id, elective_34_basket.id, min_count=2)

        academy_track = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Academy Option: Any One",
            logic_type="ANY_ONE",
            sort_order=4,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Academy Option",
        )
        academy_basket = create_basket_with_items(
            db,
            version_id=version_id,
            name="Economics Major - Academy Option Pool",
            description="COI 2025-2026 Economics: academy option, any 3+ semester-hour course",
            course_ids=academy_option_ids,
        )
        attach_basket(db, academy_track.id, academy_basket.id, min_count=1)

        recreate_econ_core_rules_validation_rule(db, version_id, program)
        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

