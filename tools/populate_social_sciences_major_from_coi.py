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
    RequirementSubstitution,
    RequirementFulfillment,
    SessionLocal,
    select,
)

PROGRAM_NAME = "Social Sciences"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "SOCIAL_SCIENCES"


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
        p = ensure_program(db, version_id)
        cleanup(db, version_id, p.id)
        all_courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()

        breadth_map = {}
        for c in all_courses:
            num = str(c.course_number or "")
            m = re.search(r"(\d{3})", num)
            lvl = int(m.group(1)) if m else 0
            if lvl < 200 or float(c.credit_hours or 0.0) < 3.0:
                continue
            group = None
            if num.startswith("Beh Sci "):
                group = "BEH"
            elif num.startswith("Econ "):
                group = "ECO"
            elif num.startswith("Geo "):
                group = "GEO"
            elif num.startswith("Law "):
                group = "LAW"
            elif num.startswith("Mgt "):
                group = "MGT"
            elif num.startswith("MSS "):
                group = "MSS"
            elif num.startswith("Pol Sci "):
                group = "PSC"
            if group:
                breadth_map.setdefault(group, []).append(c.id)

        social_science_ids = []
        for c in all_courses:
            num = str(c.course_number or "")
            if num.startswith(("Beh Sci ", "Econ ", "Geo ", "Law ", "Mgt ", "MSS ", "Pol Sci ", "Soc Sci ")):
                if float(c.credit_hours or 0.0) >= 3.0:
                    social_science_ids.append(c.id)

        sh_ids = []
        for c in all_courses:
            num = str(c.course_number or "")
            if num.startswith(("Beh Sci ", "Econ ", "Geo ", "Law ", "Mgt ", "MSS ", "Pol Sci ", "Soc Sci ", "English ", "History ", "Hum ", "Philos ", "For Lang ", "Creat Art ")):
                if float(c.credit_hours or 0.0) >= 3.0:
                    sh_ids.append(c.id)

        academy_ids = [c.id for c in all_courses if float(c.credit_hours or 0.0) >= 3.0]

        root = mk_req(
            db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Major - Social Sciences",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode=None, track_name=None
        )

        breadth_wrapper = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Social Sciences Breadth Electives: Pick N",
            logic_type="PICK_N", pick_n=5, sort_order=0, category="MAJOR", major_mode="TRACK", track_name="Social Sciences Breadth Electives"
        )
        # Broad basket plus discipline-specific child tracks to make the breadth intent visible/editable.
        b_breadth = mk_basket(db, version_id, "Social Sciences Major - Breadth Pool", "Breadth across social sciences disciplines", social_science_ids)
        attach(db, breadth_wrapper.id, b_breadth, 5)
        order = 0
        for g in sorted(breadth_map.keys()):
            rr = mk_req(
                db, version_id=version_id, parent_requirement_id=breadth_wrapper.id, program_id=p.id,
                name=f"Track - Breadth Discipline {g}: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=order,
                category="MAJOR", major_mode="TRACK", track_name=f"Breadth Discipline {g}"
            )
            bb = mk_basket(db, version_id, f"Social Sciences Major - Breadth {g} Pool", f"Discipline {g}", breadth_map[g])
            attach(db, rr.id, bb, 1)
            order += 1

        r_depth = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Social Sciences Depth Electives: Pick N",
            logic_type="PICK_N", pick_n=5, sort_order=1, category="MAJOR", major_mode="TRACK", track_name="Social Sciences Depth Electives"
        )
        b_depth = mk_basket(db, version_id, "Social Sciences Major - Depth Pool", "Any social sciences division courses", social_science_ids)
        attach(db, r_depth.id, b_depth, 5)

        r_sh = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Social Sciences/Humanities Division Option: Any One",
            logic_type="ANY_ONE", pick_n=None, sort_order=2, category="MAJOR", major_mode="TRACK", track_name="Social Sciences/Humanities Division Option"
        )
        b_sh = mk_basket(db, version_id, "Social Sciences Major - Social/Humanities Option Pool", "COI social sciences/humanities division option", sh_ids)
        attach(db, r_sh.id, b_sh, 1)

        r_acad = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Academy Option: Any One",
            logic_type="ANY_ONE", pick_n=None, sort_order=3, category="MAJOR", major_mode="TRACK", track_name="Academy Option"
        )
        b_acad = mk_basket(db, version_id, "Social Sciences Major - Academy Option Pool", "COI academy option", academy_ids)
        attach(db, r_acad.id, b_acad, 1)

        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

