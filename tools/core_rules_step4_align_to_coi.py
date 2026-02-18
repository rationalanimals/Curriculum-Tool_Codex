import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
os.chdir(BACKEND)
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import (  # noqa: E402
    Course,
    CourseBasket,
    CourseBasketItem,
    CurriculumVersion,
    Requirement,
    RequirementBasketLink,
    RequirementFulfillment,
    RequirementSubstitution,
    SessionLocal,
    normalize_course_number,
    select,
)

REQ_NAMES = {
    "core_req": "Core Requirement: All Required",
    "basic": "Track - Basic: All Required",
    "int_sci": "Track - Intermediate Science: Pick N",
    "int_la": "Track - Intermediate Liberal Arts: Pick N",
    "adv_stem": "Track - Advanced STEM: Any One",
    "adv_la": "Track - Advanced Liberal Arts: Any One",
    "adv_any": "Track - Advanced: Any One",
}

BASKET_SPECS = [
    {
        "name": "Core Track - Intermediate Science Options",
        "description": "COI-aligned: Pick 2 from Chem 200, Physics 215, Biology 215",
        "courses": ["Chem 200", "Physics 215", "Biology 215"],
        "requirement_key": "int_sci",
        "min_count": 2,
    },
    {
        "name": "Core Track - Intermediate Liberal Arts Options",
        "description": "COI-aligned draft liberal-arts intermediate options",
        "courses": ["History 300", "Soc Sci 311", "Soc Sci 212", "Law 220", "Philos 210"],
        "requirement_key": "int_la",
        "min_count": 2,
    },
    {
        "name": "Core Track - Advanced STEM Options",
        "description": "COI-aligned draft advanced STEM options",
        "courses": ["Astr Engr 310", "Aero Engr 315", "ECE 315", "Math 356"],
        "requirement_key": "adv_stem",
        "min_count": 1,
    },
    {
        "name": "Core Track - Advanced Liberal Arts Options",
        "description": "COI-aligned draft advanced liberal-arts options",
        "courses": ["History 300", "Soc Sci 311", "Law 220", "Philos 210"],
        "requirement_key": "adv_la",
        "min_count": 1,
    },
    {
        "name": "Core Track - Advanced Any Options",
        "description": "COI-aligned union of advanced STEM and advanced liberal-arts options",
        "courses": [
            "Astr Engr 310", "Aero Engr 315", "ECE 315", "Math 356", "History 300", "Soc Sci 311", "Law 220", "Philos 210"
        ],
        "requirement_key": "adv_any",
        "min_count": 1,
    },
]

BASIC_REQUIRED = [
    "Beh Sci 110",
    "Chem 100",
    "Comp Sci 110",
    "Econ 201",
    "English 111",
    "History 100",
    "Ldrshp 100",
    "Ldrshp 200",
    "Math 141",
    "Mech Engr 220",
    "Physics 110",
    "Pol Sci 211",
    "MSS 251",
    "CE 100",
    "CE 200",
]

CORE_REQ_REQUIRED = [
    "Ldrshp 300",
    "Ldrshp 400",
    "Mil Tng 100",
    "For Lang 131",
    "For Lang 132",
    "English 211",
    "Math 142",
    "Philos 210",
    "CL 400",
]

MISSING_COURSE_STUBS = [
    ("For Lang 131", "Foreign Language 131", 3.0),
    ("For Lang 132", "Foreign Language 132", 3.0),
]


def get_active_version(db):
    return db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))


def normalize(s: str) -> str:
    return normalize_course_number(s)


def ensure_stub_courses(db, version_id: str):
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    by_num = {normalize(c.course_number): c for c in courses}
    created = 0
    for num, title, hours in MISSING_COURSE_STUBS:
        if normalize(num) in by_num:
            continue
        row = Course(
            id=str(uuid.uuid4()),
            version_id=version_id,
            course_number=num,
            title=title,
            credit_hours=float(hours),
            designated_semester=None,
            min_section_size=1,
            offered_periods_json=None,
        )
        db.add(row)
        created += 1
    if created:
        db.flush()
    return created


def map_courses(db, version_id: str):
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    by = {normalize(c.course_number): c for c in courses}
    return by


def ensure_core_requirements(db, version_id: str):
    rows = db.scalars(select(Requirement).where(Requirement.version_id == version_id, Requirement.category == "CORE")).all()
    by_name = {r.name: r for r in rows}
    missing = [name for name in REQ_NAMES.values() if name not in by_name]
    if missing:
        raise RuntimeError(f"Missing required core nodes: {missing}. Run build_core_program_rules.py first.")
    return by_name


def sync_requirement_fulfillments(db, requirement_id: str, ordered_course_ids: list[str]):
    rows = db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id == requirement_id)).all()
    by_course = {r.course_id: r for r in rows}
    target = set(ordered_course_ids)
    deleted = 0
    created = 0
    updated = 0

    for r in rows:
        if r.course_id not in target:
            db.delete(r)
            deleted += 1

    for idx, cid in enumerate(ordered_course_ids):
        existing = by_course.get(cid)
        if existing:
            changed = False
            if (existing.sort_order or 0) != idx:
                existing.sort_order = idx
                changed = True
            if bool(existing.is_primary) != (idx == 0):
                existing.is_primary = (idx == 0)
                changed = True
            if changed:
                updated += 1
            continue
        db.add(
            RequirementFulfillment(
                id=str(uuid.uuid4()),
                requirement_id=requirement_id,
                course_id=cid,
                is_primary=(idx == 0),
                sort_order=idx,
            )
        )
        created += 1
    return created, updated, deleted


def ensure_basket(db, version_id: str, name: str, description: str):
    row = db.scalar(select(CourseBasket).where(CourseBasket.version_id == version_id, CourseBasket.name == name))
    if row:
        row.description = description
        return row
    max_sort = db.scalar(
        select(CourseBasket.sort_order).where(CourseBasket.version_id == version_id).order_by(CourseBasket.sort_order.desc()).limit(1)
    )
    row = CourseBasket(
        id=str(uuid.uuid4()),
        version_id=version_id,
        name=name,
        description=description,
        sort_order=(max_sort + 1) if max_sort is not None else 0,
    )
    db.add(row)
    db.flush()
    return row


def sync_basket_items(db, basket_id: str, ordered_course_ids: list[str]):
    rows = db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id == basket_id)).all()
    by_course = {r.course_id: r for r in rows}
    target = set(ordered_course_ids)
    created = 0
    deleted = 0
    updated = 0

    for r in rows:
        if r.course_id not in target:
            db.delete(r)
            deleted += 1

    for idx, cid in enumerate(ordered_course_ids):
        existing = by_course.get(cid)
        if existing:
            if (existing.sort_order or 0) != idx:
                existing.sort_order = idx
                updated += 1
            continue
        db.add(CourseBasketItem(id=str(uuid.uuid4()), basket_id=basket_id, course_id=cid, sort_order=idx))
        created += 1
    return created, updated, deleted


def sync_requirement_basket_link(db, requirement_id: str, basket_id: str, min_count: int):
    rows = db.scalars(select(RequirementBasketLink).where(RequirementBasketLink.requirement_id == requirement_id)).all()
    target_row = None
    for r in rows:
        if r.basket_id == basket_id and target_row is None:
            target_row = r
            continue
        db.delete(r)

    if target_row is None:
        target_row = RequirementBasketLink(
            id=str(uuid.uuid4()),
            requirement_id=requirement_id,
            basket_id=basket_id,
            min_count=min_count,
            max_count=None,
            sort_order=0,
        )
        db.add(target_row)
        return 1, 0

    changed = 0
    if target_row.min_count != min_count:
        target_row.min_count = min_count
        changed += 1
    if (target_row.sort_order or 0) != 0:
        target_row.sort_order = 0
        changed += 1
    if target_row.max_count is not None:
        target_row.max_count = None
        changed += 1
    return 0, changed


def sync_substitutions(db, requirement_id: str, pairs: list[tuple[str, str]], course_by_num: dict[str, Course]):
    existing = db.scalars(select(RequirementSubstitution).where(RequirementSubstitution.requirement_id == requirement_id)).all()
    for r in existing:
        db.delete(r)

    created = 0
    for primary, sub in pairs:
        p = course_by_num.get(normalize(primary))
        s = course_by_num.get(normalize(sub))
        if not p or not s:
            continue
        db.add(
            RequirementSubstitution(
                id=str(uuid.uuid4()),
                requirement_id=requirement_id,
                primary_course_id=p.id,
                substitute_course_id=s.id,
                is_bidirectional=True,
            )
        )
        created += 1
    return created


def main():
    with SessionLocal() as db:
        active = get_active_version(db)
        if not active:
            raise RuntimeError("No ACTIVE curriculum version found")

        created_stubs = ensure_stub_courses(db, active.id)
        course_by_num = map_courses(db, active.id)
        reqs = ensure_core_requirements(db, active.id)

        def ids(nums):
            missing = [n for n in nums if normalize(n) not in course_by_num]
            if missing:
                raise RuntimeError(f"Missing courses for core alignment: {missing}")
            return [course_by_num[normalize(n)].id for n in nums]

        stats = {}
        stats["basic_links"] = sync_requirement_fulfillments(db, reqs[REQ_NAMES["basic"]].id, ids(BASIC_REQUIRED))
        stats["core_req_links"] = sync_requirement_fulfillments(db, reqs[REQ_NAMES["core_req"]].id, ids(CORE_REQ_REQUIRED))

        basket_stats = []
        link_stats = []
        for spec in BASKET_SPECS:
            basket = ensure_basket(db, active.id, spec["name"], spec["description"])
            bstat = sync_basket_items(db, basket.id, ids(spec["courses"]))
            lstat = sync_requirement_basket_link(db, reqs[REQ_NAMES[spec["requirement_key"]]].id, basket.id, int(spec["min_count"]))
            basket_stats.append((spec["name"], bstat))
            link_stats.append((spec["name"], lstat))

        sub_count = 0
        sub_count += sync_substitutions(db, reqs[REQ_NAMES["core_req"]].id, [("English 211", "English 212"), ("Math 142", "Math 152")], course_by_num)
        sub_count += sync_substitutions(
            db,
            reqs[REQ_NAMES["adv_stem"]].id,
            [("Aero Engr 315", "Aero Engr 316"), ("Aero Engr 315", "Aero Engr 210S"), ("ECE 315", "ECE 215"), ("Math 356", "Math 300"), ("Math 356", "Math 377")],
            course_by_num,
        )

        db.commit()

        print(f"Active version: {active.name}")
        print(f"Stub courses created: {created_stubs}")
        print(f"Basic links (created,updated,deleted): {stats['basic_links']}")
        print(f"Core Req links (created,updated,deleted): {stats['core_req_links']}")
        for name, s in basket_stats:
            print(f"Basket items {name} (created,updated,deleted): {s}")
        for name, s in link_stats:
            print(f"Req-basket link {name} (created,updated): {s}")
        print(f"Requirement substitutions recreated: {sub_count}")
        print("Note: COI generic baskets (STEM/Socio-Cultural/Open Academic/Leadership Credit) and non-course programs (Human Relations, NCLS) remain modeled as draft abstractions.")


if __name__ == "__main__":
    main()
