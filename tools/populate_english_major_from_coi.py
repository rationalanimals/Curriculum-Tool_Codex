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


PROGRAM_NAME = "English"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "HUMANITIES"


def find_course_ids_by_number(db, version_id: str) -> dict[str, str]:
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    return {normalize_course_number(c.course_number): c.id for c in courses}


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


def recreate_english_core_rules_validation_rule(db, version_id: str, program: AcademicProgram) -> None:
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
        if pid == program.id or pname in {program.name.lower(), "english"}:
            db.delete(vr)
    db.flush()

    req_adv_lib_arts = find_core_req_id(db, version_id, "Track - Advanced Liberal Arts: Any One")
    cfg = {
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": program.id,
        "program_name": program.name,
        "required_core_groups": [
            {
                "name": "Track - Advanced Liberal Arts: Any One - Choice 1",
                "min_count": 1,
                "course_numbers": ["English 411", "English 370"],
                "source_requirement_id": req_adv_lib_arts,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            }
        ],
    }
    db.add(
        ValidationRule(
            name="Program Pathway - English",
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
        course_map = find_course_ids_by_number(db, version_id)

        program = ensure_program(db, version_id)
        cleanup_program_requirements(db, version_id, program.id)

        pre1800 = optional_course_ids(course_map, ["English 303", "English 308", "English 313"])
        post1800 = optional_course_ids(course_map, ["English 319", "English 324", "English 330"])
        social_context = optional_course_ids(course_map, ["English 355", "English 360", "English 365"])

        all_courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
        english_creative_law_ids: list[str] = []
        for c in all_courses:
            num = str(c.course_number or "")
            if num.startswith("English ") or num.startswith("Creative Art "):
                english_creative_law_ids.append(c.id)
        law360 = course_map.get(normalize_course_number("Law 360"))
        if law360:
            english_creative_law_ids.append(law360)
        academy_option_ids = [c.id for c in all_courses if float(c.credit_hours or 0.0) >= 3.0]

        root = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=None,
            name="Major - English",
            logic_type="ALL_REQUIRED",
            sort_order=0,
            category="MAJOR",
        )

        req_pre1800 = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Pre-1800: Any One",
            logic_type="ANY_ONE",
            sort_order=0,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Pre-1800",
        )
        b_pre1800 = create_basket_with_items(
            db,
            version_id=version_id,
            name="English Major - Pre-1800 Pool",
            description="COI 2025-2026 English: one pre-1800 requirement",
            course_ids=pre1800,
        )
        attach_basket(db, req_pre1800.id, b_pre1800.id, min_count=1)

        req_post1800 = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Post-1800: Any One",
            logic_type="ANY_ONE",
            sort_order=1,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Post-1800",
        )
        b_post1800 = create_basket_with_items(
            db,
            version_id=version_id,
            name="English Major - Post-1800 Pool",
            description="COI 2025-2026 English: one post-1800 requirement",
            course_ids=post1800,
        )
        attach_basket(db, req_post1800.id, b_post1800.id, min_count=1)

        req_social = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Social/Cultural Contexts: Any One",
            logic_type="ANY_ONE",
            sort_order=2,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Social/Cultural Contexts",
        )
        b_social = create_basket_with_items(
            db,
            version_id=version_id,
            name="English Major - Social/Cultural Pool",
            description="COI 2025-2026 English: one social/cultural requirement",
            course_ids=social_context,
        )
        attach_basket(db, req_social.id, b_social.id, min_count=1)

        req_eng_opts = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - English and Creative Arts Options: Pick N",
            logic_type="PICK_N",
            pick_n=7,
            sort_order=3,
            category="MAJOR",
            major_mode="TRACK",
            track_name="English and Creative Arts Options",
        )
        b_eng_opts = create_basket_with_items(
            db,
            version_id=version_id,
            name="English Major - English and Creative Arts Options Pool",
            description="COI 2025-2026 English: choose seven non-core English/Creative Arts options",
            course_ids=english_creative_law_ids,
        )
        attach_basket(db, req_eng_opts.id, b_eng_opts.id, min_count=7)

        req_academy = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Academy Options: Pick N",
            logic_type="PICK_N",
            pick_n=4,
            sort_order=4,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Academy Options",
        )
        b_academy = create_basket_with_items(
            db,
            version_id=version_id,
            name="English Major - Academy Options Pool",
            description="COI 2025-2026 English: choose four academy options",
            course_ids=academy_option_ids,
        )
        attach_basket(db, req_academy.id, b_academy.id, min_count=4)

        recreate_english_core_rules_validation_rule(db, version_id, program)
        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

