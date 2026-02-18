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

PROGRAM_NAME = "Mathematics"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "BASIC_SCIENCES_AND_MATH"

COMMON_ONE_OF = ["Comp Sci 210", "Comp Sci 211", "Comp Sci 212"]
MATH_REQUIRED = ["Math 320", "Math 360", "Math 366", "Math 378", "Math 465", "Math 420", "Math 421"]
MATH_15_LIST = ["Math 346", "Math 359", "Math 443", "Math 451", "Math 467", "Math 470", "Ops Rsch 311", "Ops Rsch 312", "Ops Rsch 417", "Cyber Sci 431", "Econ 411", "Philos 370", "Math 472", "Math 473", "Math 474", "Math 342", "Math 468", "Math 469"]
APPLIED_REQUIRED = ["Math 320", "Math 342", "Math 360", "Math 366", "Math 378", "Math 420", "Math 421"]
APPLIED_9_LIST = ["Math 346", "Math 359", "Math 443", "Math 451", "Math 465", "Math 467", "Math 468", "Math 469", "Math 470", "Math 472", "Math 473", "Math 474", "Ops Rsch 311", "Ops Rsch 312", "Ops Rsch 417", "Cyber Sci 431", "Econ 411", "Philos 370"]


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


def find_core_req(db, version_id: str, name: str) -> str:
    r = db.scalar(select(Requirement).where(Requirement.version_id == version_id, Requirement.program_id.is_(None), Requirement.category == "CORE", Requirement.name == name))
    if not r:
        raise RuntimeError(name)
    return r.id


def upsert_core_rule(db, version_id: str, p: AcademicProgram) -> None:
    for vr in db.scalars(select(ValidationRule)).all():
        try:
            cfg = json.loads(vr.config_json or "{}")
        except Exception:
            cfg = {}
        if str(cfg.get("type") or "").upper() not in {"MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"}:
            continue
        if cfg.get("program_id") == p.id or str(cfg.get("program_name") or "").strip().lower() == "mathematics":
            db.delete(vr)
    db.flush()
    req_stats = find_core_req(db, version_id, "Track - Intermediate Stats: Any One")
    req_adv_stem = find_core_req(db, version_id, "Track - Advanced STEM: Any One")
    req_adv_open = find_core_req(db, version_id, "Track - Advanced: Any One")
    cfg = {
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": p.id,
        "program_name": p.name,
        "required_core_groups": [
            {"name": "Track - Intermediate Stats: Any One - Choice 1", "min_count": 1, "course_numbers": ["Math 377"], "source_requirement_id": req_stats, "slot_index": 0, "required_semester": None, "required_semester_min": None, "required_semester_max": None},
            {"name": "Track - Advanced STEM: Any One - Choice 1", "min_count": 1, "course_numbers": ["Math 243", "Math 253"], "source_requirement_id": req_adv_stem, "slot_index": 0, "required_semester": None, "required_semester_min": None, "required_semester_max": None},
            {"name": "Track - Advanced: Any One - Choice 1", "min_count": 1, "course_numbers": ["Math 245"], "source_requirement_id": req_adv_open, "slot_index": 0, "required_semester": None, "required_semester_min": None, "required_semester_max": None},
        ],
    }
    db.add(ValidationRule(name="Program Pathway - Mathematics", tier=2, severity="FAIL", active=True, config_json=json.dumps(cfg)))


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
            db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Major - Mathematics",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode=None, track_name=None
        )
        track_wrapper = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Degree Track: Any One",
            logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MAJOR", major_mode="TRACK", track_name="Degree Track"
        )

        # Mathematics track
        t_math = mk_req(
            db, version_id=version_id, parent_requirement_id=track_wrapper.id, program_id=p.id, name="Track - Mathematics: All Required",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode="TRACK", track_name="Mathematics"
        )
        r_choice = mk_req(
            db, version_id=version_id, parent_requirement_id=t_math.id, program_id=p.id, name="Track - Programming Requirement: Any One",
            logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MAJOR", major_mode="TRACK", track_name="Programming Requirement"
        )
        b_choice = mk_basket(db, version_id, "Mathematics Major - Programming Requirement Pool", "COI mathematics: comp sci choice", opt_ids(m, COMMON_ONE_OF))
        attach(db, r_choice.id, b_choice, 1)

        r_fixed = mk_req(
            db, version_id=version_id, parent_requirement_id=t_math.id, program_id=p.id, name="Track - Mathematics Required Courses: All Required",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=1, category="MAJOR", major_mode="TRACK", track_name="Mathematics Required Courses"
        )
        link_all(db, r_fixed.id, opt_ids(m, MATH_REQUIRED))

        r_opts = mk_req(
            db, version_id=version_id, parent_requirement_id=t_math.id, program_id=p.id, name="Track - Mathematics Options: Pick N",
            logic_type="PICK_N", pick_n=5, sort_order=2, category="MAJOR", major_mode="TRACK", track_name="Mathematics Options"
        )
        b_opts = mk_basket(db, version_id, "Mathematics Major - Mathematics Options Pool", "COI mathematics: 15 semester hours", opt_ids(m, MATH_15_LIST))
        attach(db, r_opts.id, b_opts, 5)

        r_open = mk_req(
            db, version_id=version_id, parent_requirement_id=t_math.id, program_id=p.id, name="Track - Open Electives: Pick N",
            logic_type="PICK_N", pick_n=2, sort_order=3, category="MAJOR", major_mode="TRACK", track_name="Open Electives"
        )
        b_open = mk_basket(db, version_id, "Mathematics Major - Open Electives Pool", "COI mathematics: 6 semester hours open electives", [c.id for c in all_courses if float(c.credit_hours or 0.0) >= 3.0])
        attach(db, r_open.id, b_open, 2)

        # Applied Mathematics track
        t_applied = mk_req(
            db, version_id=version_id, parent_requirement_id=track_wrapper.id, program_id=p.id, name="Track - Applied Mathematics: All Required",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=1, category="MAJOR", major_mode="TRACK", track_name="Applied Mathematics"
        )
        r_choice2 = mk_req(
            db, version_id=version_id, parent_requirement_id=t_applied.id, program_id=p.id, name="Track - Programming Requirement: Any One",
            logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MAJOR", major_mode="TRACK", track_name="Programming Requirement"
        )
        b_choice2 = mk_basket(db, version_id, "Applied Mathematics Major - Programming Requirement Pool", "COI applied mathematics: comp sci choice", opt_ids(m, COMMON_ONE_OF))
        attach(db, r_choice2.id, b_choice2, 1)

        r_fixed2 = mk_req(
            db, version_id=version_id, parent_requirement_id=t_applied.id, program_id=p.id, name="Track - Applied Mathematics Required Courses: All Required",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=1, category="MAJOR", major_mode="TRACK", track_name="Applied Mathematics Required Courses"
        )
        link_all(db, r_fixed2.id, opt_ids(m, APPLIED_REQUIRED))

        r_opts2 = mk_req(
            db, version_id=version_id, parent_requirement_id=t_applied.id, program_id=p.id, name="Track - Applied Mathematics Options: Pick N",
            logic_type="PICK_N", pick_n=3, sort_order=2, category="MAJOR", major_mode="TRACK", track_name="Applied Mathematics Options"
        )
        b_opts2 = mk_basket(db, version_id, "Applied Mathematics Major - Options Pool", "COI applied mathematics: 9 semester hours", opt_ids(m, APPLIED_9_LIST))
        attach(db, r_opts2.id, b_opts2, 3)

        r_inter = mk_req(
            db, version_id=version_id, parent_requirement_id=t_applied.id, program_id=p.id, name="Track - Interdisciplinary Concentration: Pick N",
            logic_type="PICK_N", pick_n=4, sort_order=3, category="MAJOR", major_mode="TRACK", track_name="Interdisciplinary Concentration"
        )
        b_inter = mk_basket(db, version_id, "Applied Mathematics Major - Interdisciplinary Concentration Pool", "COI applied mathematics: 12 semester hours concentration", [c.id for c in all_courses if float(c.credit_hours or 0.0) >= 3.0])
        attach(db, r_inter.id, b_inter, 4)

        upsert_core_rule(db, version_id, p)
        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

