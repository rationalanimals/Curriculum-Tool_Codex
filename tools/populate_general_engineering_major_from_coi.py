import json
import os
import re
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


PROGRAM_NAME = "General Engineering"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "ENGINEERING_SCIENCES"

ENGINEERING_PREFIXES = (
    "Aero Engr ",
    "Astr Engr ",
    "CE ",
    "Comp Sci ",
    "Cyber Sci ",
    "ECE ",
    "Engr ",
    "Mech Engr ",
    "Ops Rsch ",
    "Sys Engr ",
)
BASIC_SCI_PREFIXES = (
    "Biology ",
    "Chem ",
    "Math ",
    "Physics ",
    "Meteor ",
    "Geo ",
)


def find_course_ids_by_number(db, version_id: str) -> dict[str, str]:
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    return {normalize_course_number(c.course_number): c.id for c in courses}


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


def recreate_general_eng_core_rules_validation_rule(db, version_id: str, program: AcademicProgram) -> None:
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
        if pid == program.id or pname in {program.name.lower(), "general engineering"}:
            db.delete(vr)
    db.flush()

    req_inter_stats = find_core_req_id(db, version_id, "Track - Intermediate Stats: Any One")
    cfg = {
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": program.id,
        "program_name": program.name,
        "required_core_groups": [
            {
                "name": "Track - Intermediate Stats: Any One - Choice 1",
                "min_count": 1,
                "course_numbers": ["Math 356", "Math 377"],
                "source_requirement_id": req_inter_stats,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            }
        ],
    }
    db.add(
        ValidationRule(
            name="Program Pathway - General Engineering",
            tier=2,
            severity="FAIL",
            active=True,
            config_json=json.dumps(cfg),
        )
    )


def level_200_or_higher(course_number: str) -> bool:
    m = re.search(r"(\d{3})", course_number or "")
    if not m:
        return False
    return int(m.group(1)) >= 200


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id
        course_map = find_course_ids_by_number(db, version_id)

        program = ensure_program(db, version_id)
        cleanup_program_requirements(db, version_id, program.id)

        all_courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
        engr_ids: list[str] = []
        engr_basic_ids: list[str] = []
        math_option_ids: list[str] = []
        open_option_ids: list[str] = []
        for c in all_courses:
            num = str(c.course_number or "")
            if level_200_or_higher(num):
                open_option_ids.append(c.id)
            if num.startswith(ENGINEERING_PREFIXES):
                engr_ids.append(c.id)
                engr_basic_ids.append(c.id)
            if num.startswith(BASIC_SCI_PREFIXES):
                engr_basic_ids.append(c.id)
            if num.startswith("Math ") and level_200_or_higher(num):
                math_option_ids.append(c.id)

        for extra in ["Geo 351", "Geo 353"]:
            cid = course_map.get(normalize_course_number(extra))
            if cid:
                engr_basic_ids.append(cid)
        for extra in ["ECE 245", "ECE 332", "ECE 346", "Engr 346", "Cyber Sci 431"]:
            cid = course_map.get(normalize_course_number(extra))
            if cid:
                math_option_ids.append(cid)

        root = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=None,
            name="Major - General Engineering",
            logic_type="ALL_REQUIRED",
            sort_order=0,
            category="MAJOR",
        )

        req_engr = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Engineering Options: Pick N",
            logic_type="PICK_N",
            pick_n=7,
            sort_order=0,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Engineering Options",
        )
        b_engr = create_basket_with_items(
            db,
            version_id=version_id,
            name="General Engineering Major - Engineering Options Pool",
            description="COI 2025-2026 General Engineering: seven engineering options",
            course_ids=engr_ids,
        )
        attach_basket(db, req_engr.id, b_engr.id, min_count=7)

        req_engr_basic = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Engineering/Basic Science Options: Pick N",
            logic_type="PICK_N",
            pick_n=2,
            sort_order=1,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Engineering/Basic Science Options",
        )
        b_engr_basic = create_basket_with_items(
            db,
            version_id=version_id,
            name="General Engineering Major - Engineering/Basic Science Options Pool",
            description="COI 2025-2026 General Engineering: two engineering/basic science options",
            course_ids=engr_basic_ids,
        )
        attach_basket(db, req_engr_basic.id, b_engr_basic.id, min_count=2)

        req_math = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Math Options: Pick N",
            logic_type="PICK_N",
            pick_n=2,
            sort_order=2,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Math Options",
        )
        b_math = create_basket_with_items(
            db,
            version_id=version_id,
            name="General Engineering Major - Math Options Pool",
            description="COI 2025-2026 General Engineering: two math options",
            course_ids=math_option_ids,
        )
        attach_basket(db, req_math.id, b_math.id, min_count=2)

        req_open = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Open Option: Any One",
            logic_type="ANY_ONE",
            sort_order=3,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Open Option",
        )
        b_open = create_basket_with_items(
            db,
            version_id=version_id,
            name="General Engineering Major - Open Option Pool",
            description="COI 2025-2026 General Engineering: any 200-level or higher course",
            course_ids=open_option_ids,
        )
        attach_basket(db, req_open.id, b_open.id, min_count=1)

        recreate_general_eng_core_rules_validation_rule(db, version_id, program)
        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

