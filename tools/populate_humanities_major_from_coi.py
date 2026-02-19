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
    RequirementSubstitution,
    RequirementFulfillment,
    SessionLocal,
    ValidationRule,
    normalize_course_number,
    select,
)

from populate_ref_utils import resolve_course_ids_strict  # noqa: E402

PROGRAM_NAME = "Humanities"
PROGRAM_TYPE = "MAJOR"
PROGRAM_DIVISION = "HUMANITIES"


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
        label="populate_humanities_major_from_coi course refs",
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


def find_core_req_id(db, version_id: str, req_name: str) -> str:
    row = db.scalar(
        select(Requirement).where(
            Requirement.version_id == version_id,
            Requirement.program_id.is_(None),
            Requirement.category == "CORE",
            Requirement.name == req_name,
        )
    )
    if not row:
        raise RuntimeError(f"Core requirement not found: {req_name}")
    return row.id


def recreate_core_path_rule(db, version_id: str, program: AcademicProgram) -> None:
    for vr in db.scalars(select(ValidationRule)).all():
        try:
            cfg = json.loads(vr.config_json or "{}")
        except Exception:
            cfg = {}
        if str(cfg.get("type") or "").upper() not in {"MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"}:
            continue
        if cfg.get("program_id") == program.id or str(cfg.get("program_name") or "").strip().lower() == "humanities":
            db.delete(vr)
    db.flush()
    req_adv_lib = find_core_req_id(db, version_id, "Track - Advanced Liberal Arts: Any One")
    cfg = {
        "type": "MAJOR_PATHWAY_CORE",
        "program_id": program.id,
        "program_name": program.name,
        "required_core_groups": [
            {
                "name": "Track - Advanced Liberal Arts: Any One - Choice 1",
                "min_count": 1,
                "course_numbers": ["English 411"],
                "source_requirement_id": req_adv_lib,
                "slot_index": 0,
                "required_semester": None,
                "required_semester_min": None,
                "required_semester_max": None,
            }
        ],
    }
    db.add(ValidationRule(name="Program Pathway - Humanities", tier=2, severity="FAIL", active=True, config_json=json.dumps(cfg)))


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

        rmce = optional_course_ids(cmap, ["English 300", "History 200", "Philos 200", "Pol Sci 300"])
        for c in all_courses:
            num = str(c.course_number or "")
            if num.startswith("Philos "):
                m = re.search(r"(\d{3})", num)
                if m and int(m.group(1)) >= 300 and int(m.group(1)) != 370:
                    rmce.append(c.id)

        creat_art = []
        for c in all_courses:
            num = str(c.course_number or "")
            if num.startswith("Creat Art "):
                m = re.search(r"(\d{3})", num)
                if m and int(m.group(1)) >= 310:
                    creat_art.append(c.id)

        english_dist = []
        history_dist = []
        philos_dist = []
        hum_options = []
        for c in all_courses:
            num = str(c.course_number or "")
            m = re.search(r"(\d{3})", num)
            level = int(m.group(1)) if m else 0
            if num.startswith("English ") and level >= 300 and level not in {411, 489, 490}:
                english_dist.append(c.id)
            if num.startswith("History ") and level >= 300:
                history_dist.append(c.id)
            if num.startswith("Philos ") and level >= 200:
                philos_dist.append(c.id)
            if num.startswith(("English ", "History ", "Philos ", "Hum ", "For Lang ", "Creat Art ")) and level >= 200:
                hum_options.append(c.id)

        root = create_requirement(
            db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Major - Humanities",
            logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MAJOR", major_mode=None, track_name=None
        )

        def add_any(name: str, sort_order: int, ids: list[str], label: str):
            rr = create_requirement(
                db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name=name,
                logic_type="ANY_ONE", pick_n=None, sort_order=sort_order, category="MAJOR", major_mode="TRACK", track_name=label
            )
            bb = create_basket(db, version_id, f"Humanities Major - {label} Pool", f"COI 2025-2026 Humanities {label}", ids)
            attach_basket(db, rr.id, bb, 1)

        add_any("Track - RMCE Requirement: Any One", 0, rmce, "RMCE Requirement")

        rr_creative = create_requirement(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id,
            name="Track - Creative Arts Courses: Pick N", logic_type="PICK_N", pick_n=2, sort_order=1, category="MAJOR",
            major_mode="TRACK", track_name="Creative Arts Courses"
        )
        bb_creative = create_basket(db, version_id, "Humanities Major - Creative Arts Pool", "COI 2025-2026 Humanities creative arts", creat_art)
        attach_basket(db, rr_creative.id, bb_creative, 2)

        add_any("Track - Distribution: English: Any One", 2, english_dist, "Distribution English")
        add_any("Track - Distribution: History: Any One", 3, history_dist, "Distribution History")
        add_any("Track - Distribution: Philosophy: Any One", 4, philos_dist, "Distribution Philosophy")

        rr_opts = create_requirement(
            db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id,
            name="Track - Humanities Options: Pick N", logic_type="PICK_N", pick_n=8, sort_order=5, category="MAJOR",
            major_mode="TRACK", track_name="Humanities Options"
        )
        bb_opts = create_basket(db, version_id, "Humanities Major - Humanities Options Pool", "COI 2025-2026 Humanities options", hum_options)
        attach_basket(db, rr_opts.id, bb_opts, 8)

        recreate_core_path_rule(db, version_id, p)
        db.commit()
        print(f"Populated {PROGRAM_NAME} in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

