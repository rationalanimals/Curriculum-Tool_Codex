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
    normalize_course_number,
    select,
)

PROGRAM_NAME = "Military & Strategic Studies"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "SOCIAL_SCIENCES"

FOUNDATION_FIXED = ["MSS 298", "MSS 343", "MSS 444", "MSS 498"]
FOUNDATION_INNOV = ["MSS 302", "MSS 353"]
SPECIALIZATION = ["MSS 302", "MSS 353", "MSS 363", "MSS 369", "MSS 371", "MSS 372", "MSS 377", "MSS 381", "Soc Sci 483"]
CONTEXT = ["MSS 421", "MSS 422", "MSS 423"]
SYNTH = ["MSS 490", "MSS 491", "MSS 493", "MSS 494"]


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
            db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Major - Military & Strategic Studies",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode=None, track_name=None
        )
        r_found_fixed = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Foundations of Strategy: Core Set",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode="TRACK", track_name="Foundations of Strategy Core Set"
        )
        link_all(db, r_found_fixed.id, opt_ids(m, FOUNDATION_FIXED))

        r_found_any = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Foundations of Strategy: Innovation Course: Any One",
            logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MAJOR", major_mode="TRACK", track_name="Foundations Innovation Course"
        )
        b_found_any = mk_basket(db, version_id, "MSS Major - Foundations Innovation Pool", "COI MSS: MSS 302 or MSS 353", opt_ids(m, FOUNDATION_INNOV))
        attach(db, r_found_any.id, b_found_any, 1)

        r_spec = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Employing Military Capabilities: Pick N",
            logic_type="PICK_N", pick_n=3, sort_order=2, category="MAJOR", major_mode="TRACK", track_name="Employing Military Capabilities"
        )
        b_spec = mk_basket(db, version_id, "MSS Major - Specialization Pool", "COI MSS specialization list", opt_ids(m, SPECIALIZATION))
        attach(db, r_spec.id, b_spec, 3)

        for idx, (nm, courses, order) in enumerate([
            ("Track - Contextualizing Military Strategy: Any One", CONTEXT, 3),
            ("Track - Synthesizing National Strategy: Any One", SYNTH, 4),
        ]):
            rr = mk_req(
                db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name=nm,
                logic_type="ANY_ONE", pick_n=None, sort_order=order, category="MAJOR", major_mode="TRACK", track_name=nm.replace("Track - ", "")
            )
            bb = mk_basket(db, version_id, f"MSS Major - {idx} Pool", nm, opt_ids(m, courses))
            attach(db, rr.id, bb, 1)

        breadth_ids = [c.id for c in all_courses if str(c.course_number or "").startswith("MSS ")]
        r_breadth = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - MSS Breadth Electives: Pick N",
            logic_type="PICK_N", pick_n=4, sort_order=5, category="MAJOR", major_mode="TRACK", track_name="MSS Breadth Electives"
        )
        b_breadth = mk_basket(db, version_id, "MSS Major - Breadth Electives Pool", "COI MSS breadth electives", breadth_ids)
        attach(db, r_breadth.id, b_breadth, 4)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

