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


PROGRAM_NAME = "Chemistry"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "BASIC_SCIENCES_AND_MATH"

CHEM_TRACK_MANDATORY = [
    "Chem 233",
    "Chem 234",
    "Chem 243",
    "Chem 335",
    "Chem 336",
    "Chem 344",
    "Chem 431",
    "Chem 432",
    "Chem 444",
    "Chem 445",
    "Chem 481",
    "Chem 499",
]
CHEM_TRACK_OPTIONS = [
    "Chem 325",
    "Chem 350",
    "Chem 381",
    "Chem 433",
    "Chem 435",
    "Chem 440",
    "Chem 465",
    "Chem 491",
    "Math 243",
    "Math 245",
    "Chem 499",
]

BIOCHEM_TRACK_MANDATORY = [
    "Chem 233",
    "Chem 234",
    "Chem 243",
    "Chem 335",
    "Chem 344",
    "Chem 431",
    "Chem 444",
    "Chem 445",
    "Chem 481",
    "Chem 482",
    "Chem 491",
    "Chem 499",
]
BIOCHEM_TRACK_OPTIONS = [
    "Chem 336",
    "Biology 332",
    "Biology 345",
    "Biology 360",
    "Biology 363",
    "Biology 364",
    "Biology 410",
    "Biology 431",
    "Biology 440",
]

ENGR_CHEM_TRACK_MANDATORY = [
    "Chem 233",
    "Chem 234",
    "Chem 243",
    "Chem 335",
    "Chem 336",
    "Chem 344",
    "Chem 431",
    "Chem 432",
    "Chem 440",
    "Chem 444",
    "Chem 445",
    "Chem 465",
    "Chem 481",
    "Chem 499",
]
ENGR_CHEM_TRACK_OPTIONS = [
    "Mech Engr 340",
    "Mech Engr 440",
    "Astr Engr 331",
    "Astr Engr 351",
    "Astr Engr 436",
    "Aero Engr 361",
    "Aero Engr 466",
    "ECE 332",
    "ECE 321",
    "ECE 281",
    "ECE 382",
    "Civ Engr 362",
    "Civ Engr 463",
    "Chem 499",
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


def recreate_chemistry_core_rules_validation_rule(db, version_id: str, program: AcademicProgram) -> None:
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
        if pid == program.id or pname in {program.name.lower(), "chemistry"}:
            db.delete(vr)
    db.flush()

    req_inter_sci = find_core_req_id(db, version_id, "Track - Intermediate Science: Pick N")
    req_inter_stats = find_core_req_id(db, version_id, "Track - Intermediate Stats: Any One")
    req_adv_stem = find_core_req_id(db, version_id, "Track - Advanced STEM: Any One")

    # Common cross-track core pathway constraints from COI.
    cfg = {
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": program.id,
        "program_name": program.name,
        "required_core_groups": [
            {
                "name": "Track - Intermediate Science: Pick 2 - Choice 1",
                "min_count": 1,
                "course_numbers": ["Chem 200"],
                "source_requirement_id": req_inter_sci,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
            {
                "name": "Track - Intermediate Science: Pick 2 - Choice 2",
                "min_count": 1,
                "course_numbers": ["Physics 215"],
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
                "course_numbers": ["Chem 222"],
                "source_requirement_id": req_adv_stem,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
        ],
    }
    db.add(
        ValidationRule(
            name="Program Pathway - Chemistry",
            tier=2,
            severity="FAIL",
            active=True,
            config_json=json.dumps(cfg),
        )
    )


def build_track(
    db,
    *,
    version_id: str,
    program_id: str,
    parent_id: str,
    track_name: str,
    sort_order: int,
    mandatory_ids: list[str],
    option_ids: list[str],
    option_pick_n: int,
) -> None:
    track = create_requirement(
        db,
        version_id=version_id,
        program_id=program_id,
        parent_id=parent_id,
        name=f"Track - {track_name}: All Required",
        logic_type="ALL_REQUIRED",
        sort_order=sort_order,
        category="MAJOR",
        major_mode="TRACK",
        track_name=track_name,
    )
    link_courses_all_required(db, track.id, mandatory_ids)
    track_opts = create_requirement(
        db,
        version_id=version_id,
        program_id=program_id,
        parent_id=track.id,
        name=f"Track - {track_name} Options: Pick N",
        logic_type="PICK_N",
        pick_n=option_pick_n,
        sort_order=1,
        category="MAJOR",
        major_mode="TRACK",
        track_name=f"{track_name} Options",
    )
    basket = create_basket_with_items(
        db,
        version_id=version_id,
        name=f"Chemistry Major - {track_name} Option Pool",
        description=f"COI 2025-2026 Chemistry: {track_name} options",
        course_ids=option_ids,
    )
    attach_basket(db, track_opts.id, basket.id, min_count=option_pick_n)


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id

        program = ensure_program(db, version_id)
        cleanup_program_requirements(db, version_id, program.id)

        course_id_by_num = find_course_ids_by_number(db, version_id)
        chem_track_mand = require_course_ids(course_id_by_num, CHEM_TRACK_MANDATORY)
        chem_track_opts = optional_course_ids(course_id_by_num, CHEM_TRACK_OPTIONS)
        biochem_track_mand = require_course_ids(course_id_by_num, BIOCHEM_TRACK_MANDATORY)
        biochem_track_opts = optional_course_ids(course_id_by_num, BIOCHEM_TRACK_OPTIONS)
        engr_track_mand = require_course_ids(course_id_by_num, ENGR_CHEM_TRACK_MANDATORY)
        engr_track_opts = optional_course_ids(course_id_by_num, ENGR_CHEM_TRACK_OPTIONS)

        root = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=None,
            name="Major - Chemistry",
            logic_type="ALL_REQUIRED",
            sort_order=0,
            category="MAJOR",
        )
        concentration = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Chemistry Major Track: Any One",
            logic_type="ANY_ONE",
            sort_order=1,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Chemistry Major Track",
        )

        build_track(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=concentration.id,
            track_name="Chemistry",
            sort_order=0,
            mandatory_ids=chem_track_mand,
            option_ids=chem_track_opts,
            option_pick_n=2,
        )
        build_track(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=concentration.id,
            track_name="Biochemistry",
            sort_order=1,
            mandatory_ids=biochem_track_mand,
            option_ids=biochem_track_opts,
            option_pick_n=3,
        )
        build_track(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=concentration.id,
            track_name="Engineering Chemistry",
            sort_order=2,
            mandatory_ids=engr_track_mand,
            option_ids=engr_track_opts,
            option_pick_n=2,
        )

        recreate_chemistry_core_rules_validation_rule(db, version_id, program)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")
        print("Created structure:")
        print("- No shared mandatory major-requirement node (requirements are track-specific)")
        print("- Track selector: Chemistry / Biochemistry / Engineering Chemistry")
        print("- Track-specific mandatory course sets + options")
        print("Added/updated major-specific Core Rules validation rule: Program Pathway - Chemistry")


if __name__ == "__main__":
    main()
