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

PROGRAM_NAME = "Systems Engineering"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "ENGINEERING_SCIENCES"

FIXED = ["Sys Engr 310", "Comp Sci 211", "Sys Engr 301", "Ops Rsch 312", "Sys Engr 320", "Beh Sci 373", "Sys Engr 311", "Sys Engr 336", "Sys Engr 491", "Sys Engr 492"]
MATH_OPT = ["Math 245", "Math 340", "Math 344", "Math 359", "ECE 245", "ECE 332"]


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
        if cfg.get("program_id") == p.id or str(cfg.get("program_name") or "").strip().lower() == "systems engineering":
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
            {"name": "Track - Intermediate Stats: Any One - Choice 1", "min_count": 1, "course_numbers": ["Math 300", "Math 356", "Math 377", "Beh Sci 332"], "source_requirement_id": req_stats, "slot_index": 0, "required_semester": None, "required_semester_min": None, "required_semester_max": None},
            {"name": "Track - Advanced STEM: Any One - Choice 1", "min_count": 1, "course_numbers": ["Math 243", "Math 253"], "source_requirement_id": req_adv_stem, "slot_index": 0, "required_semester": None, "required_semester_min": None, "required_semester_max": None},
            {"name": "Track - Advanced: Any One - Choice 1", "min_count": 1, "course_numbers": ["Ops Rsch 310"], "source_requirement_id": req_adv_open, "slot_index": 0, "required_semester": None, "required_semester_min": None, "required_semester_max": None},
        ],
    }
    db.add(ValidationRule(name="Program Pathway - Systems Engineering", tier=2, severity="FAIL", active=True, config_json=json.dumps(cfg)))


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
            db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Major - Systems Engineering",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode=None, track_name=None
        )
        r_fixed = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Major Requirement: All Required",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode="REQUIREMENT", track_name=None
        )
        link_all(db, r_fixed.id, opt_ids(m, FIXED))

        r_math = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Sys Engr Math Option: Any One",
            logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MAJOR", major_mode="TRACK", track_name="Sys Engr Math Option"
        )
        b_math = mk_basket(db, version_id, "Systems Engineering Major - Math Option Pool", "COI systems engineering math option", opt_ids(m, MATH_OPT))
        attach(db, r_math.id, b_math, 1)

        depth_pool = []
        for c in all_courses:
            num = str(c.course_number or "")
            m3 = re.search(r"(\d{3})", num)
            lvl = int(m3.group(1)) if m3 else 0
            if lvl >= 300 and num.startswith(("Sys Engr ", "Aero Engr ", "Astr Engr ", "Comp Sci ", "ECE ", "Civ Engr ", "Mech Engr ", "Beh Sci ", "MSS ", "Ops Rsch ", "Math ")):
                depth_pool.append(c.id)
        r_depth = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Depth Option Courses: Pick N",
            logic_type="PICK_N", pick_n=4, sort_order=2, category="MAJOR", major_mode="TRACK", track_name="Depth Option Courses"
        )
        b_depth = mk_basket(db, version_id, "Systems Engineering Major - Depth Option Pool", "COI depth options across approved domains", depth_pool)
        attach(db, r_depth.id, b_depth, 4)

        # Colloquium courses exist as zero-credit in many catalogs; keep as optional track to preserve visibility.
        coll_ids = opt_ids(m, ["Sys Engr 405", "Sys Engr 406"])
        if coll_ids:
            r_col = mk_req(
                db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Systems Engineering Colloquium: All Required",
                logic_type="ALL_REQUIRED", pick_n=None, sort_order=3, category="MAJOR", major_mode="TRACK", track_name="Systems Engineering Colloquium"
            )
            link_all(db, r_col.id, coll_ids)

        upsert_core_rule(db, version_id, p)
        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

