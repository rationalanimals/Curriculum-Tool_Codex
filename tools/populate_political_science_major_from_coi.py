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

PROGRAM_NAME = "Political Science"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "SOCIAL_SCIENCES"

REQUIRED = ["Pol Sci 300", "Pol Sci 301", "Pol Sci 302", "Pol Sci 394", "Pol Sci 491"]
AMERICAN = ["Pol Sci 392", "Pol Sci 451", "Pol Sci 481", "Pol Sci 482", "Pol Sci 483", "Pol Sci 484"]
COMPARATIVE = ["Pol Sci 464", "Pol Sci 469", "Pol Sci 471", "Pol Sci 473", "Pol Sci 475", "Pol Sci 477", "Pol Sci 479"]
INTL = ["Pol Sci 390", "Pol Sci 421", "Pol Sci 445", "Pol Sci 496", "Soc Sci 444"]
OPTION3 = ["Soc Sci 444", "Soc Sci 467"]


def find_map(db, version_id: str) -> dict[str, str]:
    return {normalize_course_number(c.course_number): c.id for c in db.scalars(select(Course).where(Course.version_id == version_id)).all()}


def opt_ids(m: dict[str, str], nums: list[str]) -> list[str]:
    out, seen = [], set()
    for n in nums:
        cid = m.get(normalize_course_number(n))
        if cid and cid not in seen:
            seen.add(cid)
            out.append(cid)
    return out


def ensure_program(db, version_id: str) -> AcademicProgram:
    p = db.scalar(select(AcademicProgram).where(AcademicProgram.version_id == version_id, AcademicProgram.program_type == PROGRAM_TYPE, AcademicProgram.name == PROGRAM_NAME))
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


def mk_req(db, **kwargs) -> Requirement:
    r = Requirement(**kwargs)
    db.add(r)
    db.flush()
    return r


def link_all(db, req_id: str, course_ids: list[str]) -> None:
    for i, cid in enumerate(course_ids):
        db.add(RequirementFulfillment(requirement_id=req_id, course_id=cid, is_primary=(i == 0), sort_order=i))


def mk_basket(db, version_id: str, name: str, desc: str, ids: list[str]) -> str:
    u = list(dict.fromkeys(ids))
    max_sort = db.scalar(select(CourseBasket.sort_order).where(CourseBasket.version_id == version_id).order_by(CourseBasket.sort_order.desc()).limit(1))
    b = CourseBasket(version_id=version_id, name=name, description=desc, sort_order=(max_sort + 1) if max_sort is not None else 0)
    db.add(b)
    db.flush()
    for i, cid in enumerate(u):
        db.add(CourseBasketItem(basket_id=b.id, course_id=cid, sort_order=i))
    return b.id


def attach(db, req_id: str, basket_id: str, min_count: int) -> None:
    db.add(RequirementBasketLink(requirement_id=req_id, basket_id=basket_id, min_count=min_count, max_count=None, sort_order=0))


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id
        m = find_map(db, version_id)
        p = ensure_program(db, version_id)
        cleanup(db, version_id, p.id)
        all_courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()

        root = mk_req(
            db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Major - Political Science",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode=None, track_name=None
        )
        r_fixed = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Major Requirement: All Required",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode="REQUIREMENT", track_name=None
        )
        link_all(db, r_fixed.id, opt_ids(m, REQUIRED))

        def add_any(name: str, track: str, ids: list[str], order: int):
            rr = mk_req(
                db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name=name,
                logic_type="ANY_ONE", pick_n=None, sort_order=order, category="MAJOR", major_mode="TRACK", track_name=track
            )
            bb = mk_basket(db, version_id, f"Political Science Major - {track} Pool", name, ids)
            attach(db, rr.id, bb, 1)

        add_any("Track - American Government Basket: Any One", "American Government Basket", opt_ids(m, AMERICAN), 1)
        add_any("Track - Comparative Politics Basket: Any One", "Comparative Politics Basket", opt_ids(m, COMPARATIVE), 2)
        add_any("Track - International Relations Basket: Any One", "International Relations Basket", opt_ids(m, INTL), 3)

        pol_only_ids = [c.id for c in all_courses if str(c.course_number or "").startswith("Pol Sci ")]
        r_opt12 = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Pol Sci Options 1-2: Pick N",
            logic_type="PICK_N", pick_n=2, sort_order=4, category="MAJOR", major_mode="TRACK", track_name="Pol Sci Options 1-2"
        )
        b_opt12 = mk_basket(db, version_id, "Political Science Major - Options 1-2 Pool", "Any Pol Sci course", pol_only_ids)
        attach(db, r_opt12.id, b_opt12, 2)

        opt3_ids = list(dict.fromkeys(pol_only_ids + opt_ids(m, OPTION3)))
        add_any("Track - Pol Sci Option 3: Any One", "Pol Sci Option 3", opt3_ids, 5)

        broad_ids = []
        for c in all_courses:
            num = str(c.course_number or "")
            m3 = re.search(r"(\d{3})", num)
            lvl = int(m3.group(1)) if m3 else 0
            if lvl >= 200 and num.startswith(("Pol Sci ", "FAS ", "For Lang ", "History ", "Econ ", "Law ", "Philos ", "English ", "Beh Sci ", "Geo ", "Hum ", "MSS ", "Soc Sci ")):
                broad_ids.append(c.id)
        r_opt45 = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Pol Sci Options 4-5: Pick N",
            logic_type="PICK_N", pick_n=2, sort_order=6, category="MAJOR", major_mode="TRACK", track_name="Pol Sci Options 4-5"
        )
        b_opt45 = mk_basket(db, version_id, "Political Science Major - Options 4-5 Pool", "COI broad options 4-5", broad_ids)
        attach(db, r_opt45.id, b_opt45, 2)

        add_any("Track - DF Option: Any One", "DF Option", [c.id for c in all_courses if float(c.credit_hours or 0.0) >= 3.0], 7)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

