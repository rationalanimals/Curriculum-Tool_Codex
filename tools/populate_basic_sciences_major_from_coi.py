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


PROGRAM_NAME = "Basic Sciences"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "BASIC_SCIENCES_AND_MATH"

BIO_RECOMMENDED = ["Biology 345", "Biology 370", "Biology 380"]
CHEM_RECOMMENDED = ["Chem 222", "Chem 230", "Chem 325", "Chem 350", "Chem 434"]
MATH_RECOMMENDED = ["Math 243", "Math 253", "Math 245", "Math 344", "Math 359", "Math 378"]
PHYS_RECOMMENDED = ["Physics 264", "Physics 370", "Meteor 320"]
STATS_ALLOWED = ["Math 356", "Math 377"]


def parse_course_level(course_number: str) -> int:
    m = re.search(r"(\d{3})", str(course_number or ""))
    if not m:
        return 0
    try:
        return int(m.group(1))
    except Exception:
        return 0


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


def recreate_basic_sciences_core_rules_validation_rule(db, version_id: str, program: AcademicProgram, course_id_by_num: dict[str, str]) -> None:
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
        if pid == program.id or pname in {program.name.lower(), "basic sciences"}:
            db.delete(vr)
    db.flush()

    req_inter_stats = find_core_req_id(db, version_id, "Track - Intermediate Stats: Any One")
    stats_nums = [n for n in STATS_ALLOWED if normalize_course_number(n) in course_id_by_num]
    if not stats_nums:
        return

    cfg = {
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": program.id,
        "program_name": program.name,
        "required_core_groups": [
            {
                "name": "Track - Intermediate Stats: Any One - Choice 1",
                "min_count": 1,
                "course_numbers": stats_nums,
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
            name="Program Pathway - Basic Sciences",
            tier=2,
            severity="FAIL",
            active=True,
            config_json=json.dumps(cfg),
        )
    )


def course_ids_for_prefix(courses: list[Course], prefix: str, *, min_level: int = 0, exclude_numbers: set[str] | None = None) -> list[str]:
    out = []
    exclude_numbers = exclude_numbers or set()
    prefix_token = str(prefix or "").strip().upper() + " "
    for c in courses:
        num = normalize_course_number(c.course_number)
        if not str(num or "").upper().startswith(prefix_token):
            continue
        if num in exclude_numbers:
            continue
        lvl = parse_course_level(c.course_number)
        if lvl < min_level:
            continue
        out.append(c.id)
    return list(dict.fromkeys(out))


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id

        program = ensure_program(db, version_id)
        cleanup_program_requirements(db, version_id, program.id)

        courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
        course_id_by_num = {normalize_course_number(c.course_number): c.id for c in courses}

        # Non-core discipline pools (COI: one non-core course from each primary discipline).
        bio_noncore = course_ids_for_prefix(courses, "Biology", min_level=300)
        chem_noncore = course_ids_for_prefix(courses, "Chem", min_level=200, exclude_numbers={normalize_course_number("Chem 200"), normalize_course_number("Chem 100")})
        math_noncore = course_ids_for_prefix(courses, "Math", min_level=200, exclude_numbers={normalize_course_number("Math 141"), normalize_course_number("Math 142"), normalize_course_number("Math 152"), normalize_course_number("Math 300"), normalize_course_number("Math 356"), normalize_course_number("Math 377")})
        phys_noncore = course_ids_for_prefix(courses, "Physics", min_level=200, exclude_numbers={normalize_course_number("Physics 110"), normalize_course_number("Physics 215")})
        # COI explicitly lists Meteor 320 as an additional physics-distribution option.
        if normalize_course_number("Meteor 320") in course_id_by_num:
            phys_noncore.append(course_id_by_num[normalize_course_number("Meteor 320")])
            phys_noncore = list(dict.fromkeys(phys_noncore))

        # Division pools used for depth/breadth constraints.
        division_pool = list(dict.fromkeys([*bio_noncore, *chem_noncore, *math_noncore, *phys_noncore]))
        upper_pool = [cid for cid in division_pool if parse_course_level(next((c.course_number for c in courses if c.id == cid), "")) >= 300]

        # Keep COI-listed recommended options near the top of each basket ordering.
        def preferred_first(pool: list[str], preferred_numbers: list[str]) -> list[str]:
            pref_ids = []
            for n in preferred_numbers:
                cid = course_id_by_num.get(normalize_course_number(n))
                if cid and cid in pool:
                    pref_ids.append(cid)
            rest = [cid for cid in pool if cid not in pref_ids]
            return [*pref_ids, *rest]

        bio_noncore = preferred_first(bio_noncore, BIO_RECOMMENDED)
        chem_noncore = preferred_first(chem_noncore, CHEM_RECOMMENDED)
        math_noncore = preferred_first(math_noncore, MATH_RECOMMENDED)
        phys_noncore = preferred_first(phys_noncore, PHYS_RECOMMENDED)

        root = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=None,
            name="Major - Basic Sciences",
            logic_type="ALL_REQUIRED",
            sort_order=0,
            category="MAJOR",
        )

        discipline_nodes = [
            ("Track - Biology Non-Core Course: Any One", "Biology", bio_noncore, 1),
            ("Track - Chemistry Non-Core Course: Any One", "Chemistry", chem_noncore, 2),
            ("Track - Mathematics Non-Core Course: Any One", "Mathematics", math_noncore, 3),
            ("Track - Physics Non-Core Course: Any One", "Physics", phys_noncore, 4),
        ]
        for node_name, label, ids, sort_order in discipline_nodes:
            req = create_requirement(
                db,
                version_id=version_id,
                program_id=program.id,
                parent_id=root.id,
                name=node_name,
                logic_type="ANY_ONE",
                sort_order=sort_order,
                category="MAJOR",
                major_mode="TRACK",
                track_name=f"{label} Non-Core",
            )
            basket = create_basket_with_items(
                db,
                version_id=version_id,
                name=f"Basic Sciences - {label} Non-Core Options",
                description=f"COI 2025-2026 {PROGRAM_NAME}: non-core {label} options",
                course_ids=ids,
            )
            attach_basket(db, req.id, basket.id, min_count=1)

        depth_req = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Basic Sciences Depth (300/400): Pick N",
            logic_type="PICK_N",
            pick_n=6,  # 18 semester hours at 3 credits each.
            sort_order=5,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Depth 300/400",
        )
        depth_basket = create_basket_with_items(
            db,
            version_id=version_id,
            name="Basic Sciences - Depth 300/400 Pool",
            description="COI 2025-2026 Basic Sciences: at least 18 semester hours at 300/400 level",
            course_ids=upper_pool,
        )
        attach_basket(db, depth_req.id, depth_basket.id, min_count=6)

        division_req = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Basic Sciences Division Courses: Pick N",
            logic_type="PICK_N",
            pick_n=8,  # 24 semester hours at 3 credits each.
            sort_order=6,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Division Courses",
        )
        division_basket = create_basket_with_items(
            db,
            version_id=version_id,
            name="Basic Sciences - Division Course Pool",
            description="COI 2025-2026 Basic Sciences: at least 24 semester hours from Biology/Chemistry/Math/Physics",
            course_ids=division_pool,
        )
        attach_basket(db, division_req.id, division_basket.id, min_count=8)

        coherent_parent = create_requirement(
            db,
            version_id=version_id,
            program_id=program.id,
            parent_id=root.id,
            name="Track - Coherent Study in One Discipline (12h): Any One",
            logic_type="ANY_ONE",
            sort_order=7,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Coherent Study",
        )
        coherent_children = [
            ("Track - Coherent Study Biology (12h): Pick N", bio_noncore),
            ("Track - Coherent Study Chemistry (12h): Pick N", chem_noncore),
            ("Track - Coherent Study Mathematics (12h): Pick N", math_noncore),
            ("Track - Coherent Study Physics (12h): Pick N", phys_noncore),
        ]
        for idx, (name, ids) in enumerate(coherent_children):
            req = create_requirement(
                db,
                version_id=version_id,
                program_id=program.id,
                parent_id=coherent_parent.id,
                name=name,
                logic_type="PICK_N",
                pick_n=4,  # 12 semester hours
                sort_order=idx,
                category="MAJOR",
                major_mode="TRACK",
                track_name=name.replace("Track - ", "").replace(": Pick N", ""),
            )
            basket = create_basket_with_items(
                db,
                version_id=version_id,
                name=f"Basic Sciences - {name} Pool",
                description=f"COI 2025-2026 {PROGRAM_NAME}: coherent study track options",
                course_ids=ids,
            )
            attach_basket(db, req.id, basket.id, min_count=4)

        # Program-specific Core Rules: only constrain the Intermediate Stats choice for Basic Sciences.
        recreate_basic_sciences_core_rules_validation_rule(db, version_id, program, course_id_by_num)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")
        print("Created structure:")
        print("- No shared mandatory major-requirement node (all requirements are track-based)")
        print("- Biology/Chemistry/Math/Physics non-core: Any One each")
        print("- Basic Sciences Depth (300/400): Pick 6")
        print("- Basic Sciences Division Courses: Pick 8")
        print("- Coherent Study in One Discipline: Any One of 4 discipline tracks, each Pick 4")
        print("Added/updated major-specific Core Rules validation rule: Program Pathway - Basic Sciences")


if __name__ == "__main__":
    main()
