import os
import sys
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
    SessionLocal,
    normalize_course_number,
    select,
)


REQ_NAMES = {
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
        "description": "Draft basket for Core Intermediate Science Pick N",
        "courses": ["Aero Engr 315", "Astr Engr 310", "ECE 315", "Math 356", "Physics 110", "Mech Engr 220"],
        "requirement_key": "int_sci",
        "min_count": 2,
    },
    {
        "name": "Core Track - Intermediate Liberal Arts Options",
        "description": "Draft basket for Core Intermediate Liberal Arts Pick N",
        "courses": ["History 300", "Soc Sci 311", "Law 220", "Philos 210", "Pol Sci 211"],
        "requirement_key": "int_la",
        "min_count": 2,
    },
    {
        "name": "Core Track - Advanced STEM Options",
        "description": "Draft basket for Core Advanced STEM Any One",
        "courses": ["Aero Engr 315", "Astr Engr 310", "ECE 315", "Math 356"],
        "requirement_key": "adv_stem",
        "min_count": 1,
    },
    {
        "name": "Core Track - Advanced Liberal Arts Options",
        "description": "Draft basket for Core Advanced Liberal Arts Any One",
        "courses": ["History 300", "Soc Sci 311", "Law 220", "Philos 210", "Pol Sci 211"],
        "requirement_key": "adv_la",
        "min_count": 1,
    },
    {
        "name": "Core Track - Advanced Any Options",
        "description": "Draft basket combining Advanced STEM and Advanced Liberal Arts",
        "courses": [
            "Aero Engr 315",
            "Astr Engr 310",
            "ECE 315",
            "Math 356",
            "History 300",
            "Soc Sci 311",
            "Law 220",
            "Philos 210",
            "Pol Sci 211",
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
    "English 211",
    "History 100",
    "Ldrshp 100",
    "Ldrshp 200",
    "MSS 251",
    "Math 141",
    "Math 142",
    "Mech Engr 220",
    "Physics 110",
    "Pol Sci 211",
    "Philos 210",
]


def ensure_fulfillment_links(db, requirement_id: str, course_ids: list[str]) -> tuple[int, int]:
    rows = db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id == requirement_id)).all()
    by_course = {r.course_id: r for r in rows}
    created = 0
    updated = 0
    next_sort = (max((r.sort_order or 0) for r in rows) + 1) if rows else 0
    for idx, cid in enumerate(course_ids):
        existing = by_course.get(cid)
        if existing:
            if (existing.sort_order or 0) != idx:
                existing.sort_order = idx
                updated += 1
            continue
        db.add(
            RequirementFulfillment(
                requirement_id=requirement_id,
                course_id=cid,
                is_primary=(idx == 0),
                sort_order=next_sort,
            )
        )
        next_sort += 1
        created += 1
    return created, updated


def ensure_basket(db, version_id: str, name: str, description: str) -> CourseBasket:
    row = db.scalar(select(CourseBasket).where(CourseBasket.version_id == version_id, CourseBasket.name == name))
    if row:
        row.description = description
        return row
    max_sort = db.scalar(
        select(CourseBasket.sort_order).where(CourseBasket.version_id == version_id).order_by(CourseBasket.sort_order.desc()).limit(1)
    )
    row = CourseBasket(version_id=version_id, name=name, description=description, sort_order=(max_sort + 1) if max_sort is not None else 0)
    db.add(row)
    db.flush()
    return row


def sync_basket_items(db, basket_id: str, course_ids: list[str]) -> tuple[int, int]:
    rows = db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id == basket_id)).all()
    by_course = {r.course_id: r for r in rows}
    created = 0
    deleted = 0
    target = set(course_ids)
    for r in rows:
        if r.course_id not in target:
            db.delete(r)
            deleted += 1
    for idx, cid in enumerate(course_ids):
        existing = by_course.get(cid)
        if existing:
            existing.sort_order = idx
            continue
        db.add(CourseBasketItem(basket_id=basket_id, course_id=cid, sort_order=idx))
        created += 1
    return created, deleted


def ensure_requirement_basket_link(db, requirement_id: str, basket_id: str, min_count: int) -> tuple[int, int]:
    rows = db.scalars(
        select(RequirementBasketLink).where(
            RequirementBasketLink.requirement_id == requirement_id,
            RequirementBasketLink.basket_id == basket_id,
        )
    ).all()
    if rows:
        changed = 0
        for r in rows:
            if r.min_count != min_count:
                r.min_count = min_count
                changed += 1
        # Remove accidental duplicates while preserving first.
        for extra in rows[1:]:
            db.delete(extra)
            changed += 1
        return 0, changed
    max_sort = db.scalar(
        select(RequirementBasketLink.sort_order)
        .where(RequirementBasketLink.requirement_id == requirement_id)
        .order_by(RequirementBasketLink.sort_order.desc())
        .limit(1)
    )
    db.add(
        RequirementBasketLink(
            requirement_id=requirement_id,
            basket_id=basket_id,
            min_count=min_count,
            sort_order=(max_sort + 1) if max_sort is not None else 0,
        )
    )
    return 1, 0


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE curriculum version found")
        version_id = active.id

        reqs = db.scalars(select(Requirement).where(Requirement.version_id == version_id, Requirement.category == "CORE")).all()
        req_by_name = {r.name: r for r in reqs}
        missing = [v for v in REQ_NAMES.values() if v not in req_by_name]
        if missing:
            raise RuntimeError(f"Missing core requirement nodes: {missing}. Run build_core_program_rules.py first.")

        courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
        cid_by_num = {normalize_course_number(c.course_number): c.id for c in courses}

        basic_ids = [cid_by_num[n] for n in map(normalize_course_number, BASIC_REQUIRED) if n in cid_by_num]
        c_new, c_upd = ensure_fulfillment_links(db, req_by_name[REQ_NAMES["basic"]].id, basic_ids)

        basket_created = 0
        basket_item_created = 0
        basket_item_deleted = 0
        link_created = 0
        link_updated = 0
        for spec in BASKET_SPECS:
            b = ensure_basket(db, version_id, spec["name"], spec["description"])
            basket_created += 0 if db.is_modified(b, include_collections=False) else 0
            course_ids = [cid_by_num[n] for n in map(normalize_course_number, spec["courses"]) if n in cid_by_num]
            ci, di = sync_basket_items(db, b.id, course_ids)
            basket_item_created += ci
            basket_item_deleted += di
            lc, lu = ensure_requirement_basket_link(
                db,
                req_by_name[REQ_NAMES[spec["requirement_key"]]].id,
                b.id,
                int(spec["min_count"]),
            )
            link_created += lc
            link_updated += lu

        db.commit()
        print(f"Active version: {active.name}")
        print(f"Basic requirement links created: {c_new}, updated sort: {c_upd}")
        print(f"Basket items created: {basket_item_created}, removed extras: {basket_item_deleted}")
        print(f"Requirement-basket links created: {link_created}, updated/deduped: {link_updated}")


if __name__ == "__main__":
    main()

