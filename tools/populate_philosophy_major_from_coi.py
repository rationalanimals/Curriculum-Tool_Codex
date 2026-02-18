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

PROGRAM_NAME = "Philosophy"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "HUMANITIES"

FIXED = ["Philos 311", "Philos 370", "Philos 495"]
BLOCK_A = ["Philos 391", "Philos 392", "Philos 393", "Philos 394"]
BLOCK_B = ["Philos 401", "Philos 402"]
OPTIONS = ["Law 463", "Mgt 411", "Pol Sci 301", "Pol Sci 423", "Pol Sci 451", "Econ 440"]


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
        if cfg.get("program_id") == p.id or str(cfg.get("program_name") or "").strip().lower() == "philosophy":
            db.delete(vr)
    db.flush()
    req_adv_lib = find_core_req(db, version_id, "Track - Advanced Liberal Arts: Any One")
    cfg = {
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": p.id,
        "program_name": p.name,
        "required_core_groups": [
            {"name": "Track - Advanced Liberal Arts: Any One - Choice 1", "min_count": 1, "course_numbers": ["English 411"], "source_requirement_id": req_adv_lib, "slot_index": 0, "required_semester": None, "required_semester_min": None, "required_semester_max": None},
        ],
    }
    db.add(ValidationRule(name="Program Pathway - Philosophy", tier=2, severity="FAIL", active=True, config_json=json.dumps(cfg)))


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
            db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Major - Philosophy",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode=None, track_name=None
        )
        r_fixed = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Major Requirement: Core Set",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode="REQUIREMENT", track_name=None
        )
        link_all(db, r_fixed.id, opt_ids(m, FIXED))

        for idx, (nm, items) in enumerate([
            ("Track - Philosophy Core Block A: Any One", BLOCK_A),
            ("Track - Philosophy Core Block B: Any One", BLOCK_B),
        ]):
            rr = mk_req(
                db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name=nm,
                logic_type="ANY_ONE", pick_n=None, sort_order=idx + 1, category="MAJOR", major_mode="TRACK", track_name=nm.replace("Track - ", "")
            )
            bb = mk_basket(db, version_id, f"Philosophy Major - Block {idx} Pool", nm, opt_ids(m, items))
            attach(db, rr.id, bb, 1)

        # COI text is flexible; represent options as a broad editable basket.
        philos_course_ids = [c.id for c in all_courses if str(c.course_number or "").startswith("Philos ")]
        option_pool = list(dict.fromkeys(philos_course_ids + opt_ids(m, OPTIONS)))
        r_opts = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Philosophy Options: Pick N",
            logic_type="PICK_N", pick_n=7, sort_order=3, category="MAJOR", major_mode="TRACK", track_name="Philosophy Options"
        )
        b_opts = mk_basket(db, version_id, "Philosophy Major - Options Pool", "COI philosophy options (editable)", option_pool)
        attach(db, r_opts.id, b_opts, 7)

        r_open = mk_req(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Open Academic Option: Any One",
            logic_type="ANY_ONE", pick_n=None, sort_order=4, category="MAJOR", major_mode="TRACK", track_name="Open Academic Option"
        )
        b_open = mk_basket(db, version_id, "Philosophy Major - Open Academic Option Pool", "COI philosophy open academic option", [c.id for c in all_courses if float(c.credit_hours or 0.0) >= 3.0])
        attach(db, r_open.id, b_open, 1)

        upsert_core_rule(db, version_id, p)
        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

