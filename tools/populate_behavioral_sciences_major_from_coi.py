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


PROGRAM_NAME = "Behavioral Sciences"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "SOCIAL_SCIENCES"

MANDATORY_MAIN = ["Beh Sci 280", "Beh Sci 361", "Beh Sci 498"]

COGNITION_NEURO = ["Beh Sci 355", "Beh Sci 390", "Beh Sci 435"]
APPLIED_PSYCH = ["Beh Sci 330", "Beh Sci 345", "Beh Sci 352", "Beh Sci 380", "Beh Sci 440"]
SOCIOLOGY = ["Beh Sci 358", "Beh Sci 362", "Beh Sci 363", "Beh Sci 364", "Beh Sci 365", "Beh Sci 366"]
BEHAVIORAL_ELECTIVES = [
    "Beh Sci 314",
    "Beh Sci 330",
    "Beh Sci 332",
    "Beh Sci 345",
    "Beh Sci 352",
    "Beh Sci 355",
    "Beh Sci 358",
    "Beh Sci 360",
    "Beh Sci 361",
    "Beh Sci 362",
    "Beh Sci 363",
    "Beh Sci 364",
    "Beh Sci 365",
    "Beh Sci 366",
    "Beh Sci 373",
    "Beh Sci 375",
    "Beh Sci 380",
    "Beh Sci 390",
    "Beh Sci 414",
    "Beh Sci 435",
    "Beh Sci 440",
    "Beh Sci 471",
    "Beh Sci 473",
    "Beh Sci 495",
    "Beh Sci 499",
]

HUMAN_FACTORS_REQUIRED = ["Beh Sci 373", "Beh Sci 375", "Beh Sci 390", "Beh Sci 471", "Beh Sci 473"]


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


def recreate_behavioral_core_rules_validation_rule(db, version_id: str, program: AcademicProgram, course_id_by_num: dict[str, str]) -> None:
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
        if pid == program.id or pname in {program.name.lower(), "behavioral sciences"}:
            db.delete(vr)
    db.flush()

    req_inter_sci = find_core_req_id(db, version_id, "Track - Intermediate Science: Pick N")
    req_inter_stats = find_core_req_id(db, version_id, "Track - Intermediate Stats: Any One")
    req_adv_socio = find_core_req_id(db, version_id, "Track - Advanced Liberal Arts: Any One")

    p2_options = [n for n in ["Chem 200", "Physics 215"] if normalize_course_number(n) in course_id_by_num]
    if not p2_options:
        return

    cfg = {
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": program.id,
        "program_name": program.name,
        "required_core_groups": [
            {
                "name": "Track - Intermediate Science: Pick 2 - Choice 1",
                "min_count": 1,
                "course_numbers": ["Biology 215"],
                "source_requirement_id": req_inter_sci,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
            {
                "name": "Track - Intermediate Science: Pick 2 - Choice 2",
                "min_count": 1,
                "course_numbers": p2_options,
                "source_requirement_id": req_inter_sci,
                "slot_index": 1,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
            {
                "name": "Track - Intermediate Stats: Any One - Choice 1",
                "min_count": 1,
                "course_numbers": ["Math 300"],
                "source_requirement_id": req_inter_stats,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
            {
                "name": "Track - Advanced Liberal Arts: Any One - Choice 1",
                "min_count": 1,
                "course_numbers": ["Beh Sci 360"],
                "source_requirement_id": req_adv_socio,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            },
        ],
    }
    db.add(
        ValidationRule(
            name="Program Pathway - Behavioral Sciences",
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
        cognition_ids = optional_course_ids(course_id_by_num, COGNITION_NEURO)
        applied_ids = optional_course_ids(course_id_by_num, APPLIED_PSYCH)
        sociology_ids = optional_course_ids(course_id_by_num, SOCIOLOGY)
        hf_required_ids = require_course_ids(course_id_by_num, HUMAN_FACTORS_REQUIRED)
        elective_ids = optional_course_ids(course_id_by_num, BEHAVIORAL_ELECTIVES)
        all_academic_ids = [
            c.id
            for c in db.scalars(select(Course).where(Course.version_id == version_id)).all()
            if float(c.credit_hours or 0.0) > 0.0
        ]

        root = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=None,
            name="Major - Behavioral Sciences",
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

        concentration_parent = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=major_req.id,
            name="Track - Concentration: Any One",
            logic_type="ANY_ONE",
            sort_order=1,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Concentration",
        )

        # Psychology concentration
        psych = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=concentration_parent.id,
            name="Track - Psychology Concentration: All Required",
            logic_type="ALL_REQUIRED",
            sort_order=0,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Psychology",
        )
        for idx, (nm, ids, need) in enumerate(
            [
                ("Track - Cognition and Neuroscience Courses: Pick N", cognition_ids, 2),
                ("Track - Applied Psychology Courses: Pick N", applied_ids, 2),
                ("Track - Sociology Courses: Pick N", sociology_ids, 2),
                ("Track - Behavioral Sciences Electives: Pick N", elective_ids, 3),
            ]
        ):
            req = create_requirement(
                db,
                version_id=version_id,
                program_id=program.id,
                parent_id=psych.id,
                name=nm,
                logic_type="PICK_N",
                pick_n=need,
                sort_order=idx,
                category="MAJOR",
                major_mode="TRACK",
                track_name=nm.replace("Track - ", "").replace(": Pick N", ""),
            )
            basket = create_basket_with_items(
                db,
                version_id=version_id,
                name=f"Behavioral Sciences - {nm} Options (Psych)",
                description=f"COI 2025-2026 {PROGRAM_NAME}: psychology concentration options",
                course_ids=ids,
            )
            attach_basket(db, req.id, basket.id, min_count=need)

        # Sociology concentration
        soc = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=concentration_parent.id,
            name="Track - Sociology Concentration: All Required",
            logic_type="ALL_REQUIRED",
            sort_order=1,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Sociology",
        )
        link_courses_all_required(db, soc.id, require_course_ids(course_id_by_num, ["Beh Sci 365"]))
        for idx, (nm, ids, need) in enumerate(
            [
                ("Track - Additional Sociology Courses: Pick N", sociology_ids, 3),
                ("Track - Cognition and Neuroscience Courses: Pick N", cognition_ids, 1),
                ("Track - Applied Psychology Courses: Pick N", applied_ids, 1),
                ("Track - Behavioral Sciences Electives: Pick N", elective_ids, 3),
            ],
            start=1,
        ):
            req = create_requirement(
                db,
                version_id=version_id,
                program_id=program.id,
                parent_id=soc.id,
                name=nm,
                logic_type="PICK_N",
                pick_n=need,
                sort_order=idx,
                category="MAJOR",
                major_mode="TRACK",
                track_name=nm.replace("Track - ", "").replace(": Pick N", ""),
            )
            basket = create_basket_with_items(
                db,
                version_id=version_id,
                name=f"Behavioral Sciences - {nm} Options (Soc)",
                description=f"COI 2025-2026 {PROGRAM_NAME}: sociology concentration options",
                course_ids=ids,
            )
            attach_basket(db, req.id, basket.id, min_count=need)

        # Human Factors concentration
        hf = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=concentration_parent.id,
            name="Track - Human Factors Concentration: All Required",
            logic_type="ALL_REQUIRED",
            sort_order=2,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Human Factors",
        )
        link_courses_all_required(db, hf.id, hf_required_ids)
        for idx, (nm, ids, need) in enumerate(
            [
                ("Track - Additional Cognition and Neuroscience Courses: Pick N", cognition_ids, 1),
                ("Track - Sociology Courses: Pick N", sociology_ids, 1),
                ("Track - Behavioral Sciences Electives: Pick N", elective_ids, 2),
            ],
            start=1,
        ):
            req = create_requirement(
                db,
                version_id=version_id,
                program_id=program.id,
                parent_id=hf.id,
                name=nm,
                logic_type="PICK_N",
                pick_n=need,
                sort_order=idx,
                category="MAJOR",
                major_mode="TRACK",
                track_name=nm.replace("Track - ", "").replace(": Pick N", ""),
            )
            basket = create_basket_with_items(
                db,
                version_id=version_id,
                name=f"Behavioral Sciences - {nm} Options (HF)",
                description=f"COI 2025-2026 {PROGRAM_NAME}: human factors concentration options",
                course_ids=ids,
            )
            attach_basket(db, req.id, basket.id, min_count=need)

        academy_opts = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=major_req.id,
            name="Track - Academy Options: Pick N",
            logic_type="PICK_N",
            pick_n=2,
            sort_order=2,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Academy Options",
        )
        academy_basket = create_basket_with_items(
            db,
            version_id=version_id,
            name="Behavioral Sciences - Academy Options",
            description="COI 2025-2026 Behavioral Sciences: any two additional 3.0+ semester hour courses",
            course_ids=all_academic_ids,
        )
        attach_basket(db, academy_opts.id, academy_basket.id, min_count=2)

        recreate_behavioral_core_rules_validation_rule(db, version_id, program, course_id_by_num)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")
        print("Created structure:")
        print("- Major Requirement: All Required (Beh Sci 280, 361, 498)")
        print("- Concentration selector: Psychology / Sociology / Human Factors")
        print("- Concentration-specific Pick N baskets per COI")
        print("- Academy Options: Pick 2 (3.0+ semester hour course pool)")
        print("Added/updated major-specific Core Rules validation rule: Program Pathway - Behavioral Sciences")


if __name__ == "__main__":
    main()
