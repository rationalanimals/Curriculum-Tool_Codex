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

PROGRAM_NAME = "Legal Studies"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "HUMANITIES"

REQUIRED = ["Law 221", "Law 321", "Law 351", "Law 421", "Law 485"]
LAW_OPTIONS = [
    "Law 331", "Law 340", "Law 360", "Law 363", "Law 412", "Law 419", "Law 440", "Law 442",
    "Law 456", "Law 463", "Law 466", "Law 480", "Law 495", "Law 499", "Philos 200", "Philos 395",
    "Soc Sci 420", "Soc Sci 483",
]


def find_course_ids_by_number(db, version_id: str) -> dict[str, str]:
    return {
        normalize_course_number(c.course_number): c.id
        for c in db.scalars(select(Course).where(Course.version_id == version_id)).all()
    }


def optional_course_ids(course_map: dict[str, str], numbers: list[str]) -> list[str]:
    out, seen = [], set()
    for n in numbers:
        cid = course_map.get(normalize_course_number(n))
        if cid and cid not in seen:
            seen.add(cid)
            out.append(cid)
    return out


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
    bids = [l.basket_id for l in links]
    for row in db.scalars(select(RequirementSubstitution).where(RequirementSubstitution.requirement_id.in_(ids))).all():
        db.delete(row)
    for row in db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_(ids))).all():
        db.delete(row)
    for l in links:
        db.delete(l)
    for r in reqs:
        db.delete(r)
    db.flush()
    for bid in set(bids):
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

        required_ids = optional_course_ids(cmap, REQUIRED)
        law_option_ids = optional_course_ids(cmap, LAW_OPTIONS)
        social_hum_300_400 = []
        for c in all_courses:
            num = str(c.course_number or "")
            m = re.search(r"(\d{3})", num)
            level = int(m.group(1)) if m else 0
            if level >= 300 and level < 500 and num.startswith(("Law ", "Soc Sci ", "Pol Sci ", "History ", "Hum ", "English ", "Philos ", "MSS ", "Econ ", "Beh Sci ")):
                social_hum_300_400.append(c.id)
        academy = [c.id for c in all_courses if float(c.credit_hours or 0.0) >= 3.0]

        root = create_requirement(
            db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Major - Legal Studies",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode=None, track_name=None
        )
        r_fixed = create_requirement(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Major Requirement: All Required",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode="REQUIREMENT", track_name=None
        )
        link_all_required(db, r_fixed.id, required_ids)

        r_opts = create_requirement(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Law Options: Pick N",
            logic_type="PICK_N", pick_n=7, sort_order=1, category="MAJOR", major_mode="TRACK", track_name="Law Options"
        )
        b_opts = create_basket(db, version_id, "Legal Studies Major - Law Options Pool", "COI 2025-2026 Legal Studies law options", law_option_ids)
        attach_basket(db, r_opts.id, b_opts, 7)

        r13 = create_requirement(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id,
            name="Track - Social Sciences/Humanities 300/400-Level Option: Any One", logic_type="ANY_ONE",
            pick_n=None, sort_order=2, category="MAJOR", major_mode="TRACK", track_name="Social Sciences/Humanities Option"
        )
        b13 = create_basket(db, version_id, "Legal Studies Major - Social/Humanities 300/400 Pool", "COI 2025-2026 Legal Studies section 13", social_hum_300_400)
        attach_basket(db, r13.id, b13, 1)

        r14 = create_requirement(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id,
            name="Track - Academy Option: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=3,
            category="MAJOR", major_mode="TRACK", track_name="Academy Option"
        )
        b14 = create_basket(db, version_id, "Legal Studies Major - Academy Option Pool", "COI 2025-2026 Legal Studies section 14", academy)
        attach_basket(db, r14.id, b14, 1)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

