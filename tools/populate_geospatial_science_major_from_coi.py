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
    normalize_course_number,
    select,
)

from populate_ref_utils import resolve_course_ids_strict  # noqa: E402

PROGRAM_NAME = "Geospatial Science"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "SOCIAL_SCIENCES"

FOUNDATIONAL = ["Geo 310", "Geo 340", "Geo 382", "Geo 250", "Geo 412", "Geo 351", "Geo 353"]
DEPTH_LIST = [
    "Geo 385", "Geo 440", "Geo 482", "Geo 360", "Geo 366", "Geo 375", "Geo 380", "Geo 355",
    "Geo 452", "Meteor 320", "Meteor 352", "Civ Engr 356", "Econ 340", "Geo 365", "Geo 488",
    "Geo 495", "Geo 498", "Geo 499", "For Lang 365", "History 230", "History 240", "History 250",
    "History 260", "History 270", "History 280", "History 290", "Pol Sci 469", "Pol Sci 471",
    "Pol Sci 473", "Pol Sci 475", "Pol Sci 477", "Pol Sci 479",
]
CAPSTONE = ["Geo 491", "Geo 497"]


def find_course_ids_by_number(db, version_id: str) -> dict[str, str]:
    return {
        normalize_course_number(c.course_number): c.id
        for c in db.scalars(select(Course).where(Course.version_id == version_id)).all()
    }


def optional_course_ids(course_map: dict[str, str], numbers: list[str]) -> list[str]:
    return resolve_course_ids_strict(
        course_map,
        numbers,
        normalize_course_number,
        label="populate_geospatial_science_major_from_coi course refs",
    )


def ensure_program(db, version_id: str) -> AcademicProgram:
    p = db.scalar(
        select(AcademicProgram).where(
            AcademicProgram.version_id == version_id,
            AcademicProgram.program_type == PROGRAM_TYPE,
            AcademicProgram.name == PROGRAM_NAME,
        )
    )
    if p:
        p.division = PROGRAM_DIVISION
        return p
    p = AcademicProgram(version_id=version_id, name=PROGRAM_NAME, program_type=PROGRAM_TYPE, division=PROGRAM_DIVISION)
    db.add(p)
    db.flush()
    return p


def cleanup(db, version_id: str, program_id: str) -> None:
    reqs = db.scalars(select(Requirement).where(Requirement.version_id == version_id, Requirement.program_id == program_id)).all()
    if not reqs:
        return
    ids = [r.id for r in reqs]
    links = db.scalars(select(RequirementBasketLink).where(RequirementBasketLink.requirement_id.in_(ids))).all()
    basket_ids = [l.basket_id for l in links]
    for row in db.scalars(select(RequirementSubstitution).where(RequirementSubstitution.requirement_id.in_(ids))).all():
        db.delete(row)
    for row in db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_(ids))).all():
        db.delete(row)
    for l in links:
        db.delete(l)
    for r in reqs:
        db.delete(r)
    db.flush()
    for bid in set(basket_ids):
        if db.scalar(select(RequirementBasketLink).where(RequirementBasketLink.basket_id == bid)):
            continue
        for it in db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id == bid)).all():
            db.delete(it)
        b = db.get(CourseBasket, bid)
        if b:
            db.delete(b)


def create_requirement(db, **kwargs) -> Requirement:
    r = Requirement(**kwargs)
    db.add(r)
    db.flush()
    return r


def link_all_required(db, req_id: str, course_ids: list[str]) -> None:
    for i, cid in enumerate(course_ids):
        db.add(RequirementFulfillment(requirement_id=req_id, course_id=cid, is_primary=(i == 0), sort_order=i))


def create_basket(db, version_id: str, name: str, description: str, course_ids: list[str]) -> str:
    ids = list(dict.fromkeys(course_ids))
    max_sort = db.scalar(select(CourseBasket.sort_order).where(CourseBasket.version_id == version_id).order_by(CourseBasket.sort_order.desc()).limit(1))
    b = CourseBasket(version_id=version_id, name=name, description=description, sort_order=(max_sort + 1) if max_sort is not None else 0)
    db.add(b)
    db.flush()
    for i, cid in enumerate(ids):
        db.add(CourseBasketItem(basket_id=b.id, course_id=cid, sort_order=i))
    return b.id


def attach_basket(db, req_id: str, basket_id: str, min_count: int) -> None:
    db.add(RequirementBasketLink(requirement_id=req_id, basket_id=basket_id, min_count=min_count, max_count=None, sort_order=0))


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id
        p = ensure_program(db, version_id)
        cleanup(db, version_id, p.id)
        cmap = find_course_ids_by_number(db, version_id)
        all_courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()

        foundational_ids = optional_course_ids(cmap, FOUNDATIONAL)
        depth_ids = optional_course_ids(cmap, DEPTH_LIST)
        for c in all_courses:
            num = str(c.course_number or "")
            if num.startswith("Geo "):
                m = re.search(r"(\d{3})", num)
                if m and int(m.group(1)) >= 300:
                    depth_ids.append(c.id)
        capstone_ids = optional_course_ids(cmap, CAPSTONE)
        academy_ids = [c.id for c in all_courses if float(c.credit_hours or 0.0) >= 3.0]

        root = create_requirement(
            db,
            version_id=version_id,
            parent_requirement_id=None,
            program_id=p.id,
            name="Major - Geospatial Science",
            logic_type="ALL_REQUIRED",
            pick_n=None,
            sort_order=0,
            category="MAJOR",
            major_mode=None,
            track_name=None,
        )
        r_found = create_requirement(
            db,
            version_id=version_id,
            parent_requirement_id=root.id,
            program_id=p.id,
            name="Major Requirement: All Required",
            logic_type="ALL_REQUIRED",
            pick_n=None,
            sort_order=0,
            category="MAJOR",
            major_mode="REQUIREMENT",
            track_name=None,
        )
        link_all_required(db, r_found.id, foundational_ids)

        r_depth = create_requirement(
            db,
            version_id=version_id,
            parent_requirement_id=root.id,
            program_id=p.id,
            name="Track - Further Depth in Geospatial Science: Pick N",
            logic_type="PICK_N",
            pick_n=4,
            sort_order=1,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Further Depth in Geospatial Science",
        )
        b_depth = create_basket(db, version_id, "Geospatial Science Major - Further Depth Pool", "COI 2025-2026 Geospatial Science depth pool", depth_ids)
        attach_basket(db, r_depth.id, b_depth, 4)

        r_cap = create_requirement(
            db,
            version_id=version_id,
            parent_requirement_id=root.id,
            program_id=p.id,
            name="Track - Synthesis/Capstone: Any One",
            logic_type="ANY_ONE",
            pick_n=None,
            sort_order=2,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Synthesis/Capstone",
        )
        b_cap = create_basket(db, version_id, "Geospatial Science Major - Capstone Pool", "COI 2025-2026 Geospatial capstone pool", capstone_ids)
        attach_basket(db, r_cap.id, b_cap, 1)

        r_acad = create_requirement(
            db,
            version_id=version_id,
            parent_requirement_id=root.id,
            program_id=p.id,
            name="Track - Academy Options: Pick N",
            logic_type="PICK_N",
            pick_n=2,
            sort_order=3,
            category="MAJOR",
            major_mode="TRACK",
            track_name="Academy Options",
        )
        b_acad = create_basket(db, version_id, "Geospatial Science Major - Academy Options Pool", "COI 2025-2026 Geospatial academy options", academy_ids)
        attach_basket(db, r_acad.id, b_acad, 2)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

