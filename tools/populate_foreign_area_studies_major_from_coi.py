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


PROGRAM_NAME = "Foreign Area Studies"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "HUMANITIES"

POL_REGION = ["Pol Sci 479", "Pol Sci 473", "Pol Sci 471", "Pol Sci 475", "Pol Sci 477", "Pol Sci 469"]
HIST_REGION = ["History 280", "History 270", "History 230", "History 260", "History 250", "History 240"]
MSS_REGION = ["MSS 494", "MSS 490", "MSS 493", "MSS 491"]

FAS_ELECTIVES = [
    "Econ 351", "Econ 450", "Econ 454",
    "Geo 380", "Geo 412", "Geo 470", "Geo 471", "Geo 475", "Geo 480",
    "Hum 200S", "Hum 400S", "Hum 430", "Hum 461", "Hum 463",
    "MSS 369", "MSS 421", "MSS 422", "MSS 423", "MSS 490", "MSS 491", "MSS 493", "MSS 494",
    "Pol Sci 301", "Pol Sci 302", "Pol Sci 390", "Pol Sci 394", "Pol Sci 421", "Pol Sci 423",
    "Pol Sci 445", "Pol Sci 460", "Pol Sci 469", "Pol Sci 471", "Pol Sci 473", "Pol Sci 475", "Pol Sci 477", "Pol Sci 479", "Pol Sci 496",
    "History 230", "History 240", "History 250", "History 260", "History 270", "History 280", "History 290",
    "History 324", "History 325", "History 326", "History 327", "History 328", "History 329", "History 330", "History 340",
    "History 360", "History 361", "History 362", "History 363", "History 364", "History 365", "History 366", "History 367", "History 368", "History 369",
    "Beh Sci 360", "Beh Sci 362",
    "Law 363", "Law 463",
    "English 303", "English 308", "English 350",
    "Philos 391", "Philos 392", "Philos 393", "Philos 401",
    "Mgt 498",
    "Creative Art 330", "Creative Art 335",
    "Soc Sci 444",
]


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


def add_track_bundle(
    db,
    *,
    version_id: str,
    program_id: str,
    parent_id: str,
    name: str,
    sort_order: int,
    fixed_courses: list[str],
    r4_region_pool: list[str],
    r8_pool: list[str],
    r10_pool: list[str],
    r11_pool: list[str],
) -> None:
    course_map = find_course_ids_by_number(db, version_id)
    track = create_requirement(
        db,
        version_id=version_id,
        program_id=program_id,
        parent_id=parent_id,
        name=name,
        logic_type="ALL_REQUIRED",
        sort_order=sort_order,
        category="MAJOR",
        major_mode="TRACK",
        track_name=name.replace("Track - ", ""),
    )

    r4 = create_requirement(
        db,
        version_id=version_id,
        program_id=program_id,
        parent_id=track.id,
        name="Track - Regional Course: Any One",
        logic_type="ANY_ONE",
        sort_order=0,
        category="MAJOR",
        major_mode="TRACK",
        track_name="Regional Course",
    )
    b_r4 = create_basket_with_items(
        db,
        version_id=version_id,
        name=f"{name} - Regional Course Pool",
        description="FAS regional course pool",
        course_ids=optional_course_ids(course_map, r4_region_pool),
    )
    attach_basket(db, r4.id, b_r4.id, min_count=1)

    fixed_ids = optional_course_ids(course_map, fixed_courses)
    if fixed_ids:
        fixed_node = create_requirement(
            db,
            version_id=version_id,
            program_id=program_id,
            parent_id=track.id,
            name="Track - Disciplinary Methods/Capstone: All Required",
            logic_type="ALL_REQUIRED",
            sort_order=1,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Disciplinary Methods/Capstone",
        )
        link_courses_all_required(db, fixed_node.id, fixed_ids)

    r8 = create_requirement(
        db,
        version_id=version_id,
        program_id=program_id,
        parent_id=track.id,
        name="Track - Economics Requirement: Any One",
        logic_type="ANY_ONE",
        sort_order=2,
        category="MAJOR",
        major_mode="TRACK",
        track_name="Economics Requirement",
    )
    b_r8 = create_basket_with_items(
        db,
        version_id=version_id,
        name=f"{name} - Economics Requirement Pool",
        description="FAS economics requirement pool",
        course_ids=optional_course_ids(course_map, r8_pool),
    )
    attach_basket(db, r8.id, b_r8.id, min_count=1)

    geo250_id = course_map.get(normalize_course_number("Geo 250"))
    if geo250_id:
        r9 = create_requirement(
            db,
            version_id=version_id,
            program_id=program_id,
            parent_id=track.id,
            name="Track - Human Geography: All Required",
            logic_type="ALL_REQUIRED",
            sort_order=3,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Human Geography",
        )
        link_courses_all_required(db, r9.id, [geo250_id])

    r10 = create_requirement(
        db,
        version_id=version_id,
        program_id=program_id,
        parent_id=track.id,
        name="Track - Additional Regional Requirement A: Any One",
        logic_type="ANY_ONE",
        sort_order=4,
        category="MAJOR",
        major_mode="TRACK",
        track_name="Additional Regional Requirement A",
    )
    b_r10 = create_basket_with_items(
        db,
        version_id=version_id,
        name=f"{name} - Additional Regional Requirement A Pool",
        description="FAS additional regional requirement A pool",
        course_ids=optional_course_ids(course_map, r10_pool),
    )
    attach_basket(db, r10.id, b_r10.id, min_count=1)

    r11 = create_requirement(
        db,
        version_id=version_id,
        program_id=program_id,
        parent_id=track.id,
        name="Track - Additional Regional Requirement B: Any One",
        logic_type="ANY_ONE",
        sort_order=5,
        category="MAJOR",
        major_mode="TRACK",
        track_name="Additional Regional Requirement B",
    )
    b_r11 = create_basket_with_items(
        db,
        version_id=version_id,
        name=f"{name} - Additional Regional Requirement B Pool",
        description="FAS additional regional requirement B pool",
        course_ids=optional_course_ids(course_map, r11_pool),
    )
    attach_basket(db, r11.id, b_r11.id, min_count=1)

    r12 = create_requirement(
        db,
        version_id=version_id,
        program_id=program_id,
        parent_id=track.id,
        name="Track - Foreign Policy Requirement: Any One",
        logic_type="ANY_ONE",
        sort_order=6,
        category="MAJOR",
        major_mode="TRACK",
        track_name="Foreign Policy Requirement",
    )
    b_r12 = create_basket_with_items(
        db,
        version_id=version_id,
        name=f"{name} - Foreign Policy Requirement Pool",
        description="FAS foreign policy requirement pool",
        course_ids=optional_course_ids(course_map, ["History 340", "Pol Sci 302"]),
    )
    attach_basket(db, r12.id, b_r12.id, min_count=1)


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
        language_ids = []
        for c in all_courses:
            num = str(c.course_number or "")
            if num.startswith("For Lang "):
                m = re.search(r"(\d{3})", num)
                if m and int(m.group(1)) >= 200:
                    language_ids.append(c.id)

        root = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=None,
            name="Major - Foreign Area Studies",
            logic_type="ALL_REQUIRED",
            sort_order=0,
            category="MAJOR",
        )

        lang_track = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Language Sequence: Pick N",
            logic_type="PICK_N",
            pick_n=3,
            sort_order=0,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Language Sequence",
        )
        b_lang = create_basket_with_items(
            db,
            version_id=version_id,
            name="FAS Major - Language Sequence Pool",
            description="COI 2025-2026 FAS: For Lang I/II/III",
            course_ids=language_ids,
        )
        attach_basket(db, lang_track.id, b_lang.id, min_count=3)

        discipline_wrapper = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Disciplinary Track: Any One",
            logic_type="ANY_ONE",
            sort_order=1,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Disciplinary Track",
        )

        add_track_bundle(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=discipline_wrapper.id,
            name="Track - Political Science Track: All Required",
            sort_order=0,
            fixed_courses=["Pol Sci 394", "Pol Sci 300", "Pol Sci 491"],
            r4_region_pool=POL_REGION,
            r8_pool=["Econ 374", "Soc Sci 444"],
            r10_pool=HIST_REGION,
            r11_pool=MSS_REGION,
        )
        add_track_bundle(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=discipline_wrapper.id,
            name="Track - History Track: All Required",
            sort_order=1,
            fixed_courses=["History 362", "History 363", "History 200", "History 402"],
            r4_region_pool=HIST_REGION,
            r8_pool=["Econ 374"],
            r10_pool=POL_REGION,
            r11_pool=MSS_REGION,
        )
        add_track_bundle(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=discipline_wrapper.id,
            name="Track - MSS Track: All Required",
            sort_order=2,
            fixed_courses=["MSS 421", "MSS 298", "MSS 498"],
            r4_region_pool=MSS_REGION,
            r8_pool=["Econ 374"],
            r10_pool=HIST_REGION,
            r11_pool=POL_REGION,
        )

        fas_electives = optional_course_ids(course_map, FAS_ELECTIVES)
        for c in all_courses:
            num = str(c.course_number or "")
            if num.startswith("FAS "):
                fas_electives.append(c.id)
            if num.startswith("For Lang "):
                m = re.search(r"(\d{3})", num)
                if m and int(m.group(1)) >= 300:
                    fas_electives.append(c.id)

        electives_track = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - FAS Electives: Pick N",
            logic_type="PICK_N",
            pick_n=2,
            sort_order=2,
            category="MAJOR",
            major_mode="TRACK",
            track_name="FAS Electives",
        )
        b_electives = create_basket_with_items(
            db,
            version_id=version_id,
            name="FAS Major - Electives Pool",
            description="COI 2025-2026 FAS: choose two FAS electives",
            course_ids=fas_electives,
        )
        attach_basket(db, electives_track.id, b_electives.id, min_count=2)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

