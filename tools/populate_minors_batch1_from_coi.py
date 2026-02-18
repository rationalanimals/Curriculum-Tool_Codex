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


def find_map(db, version_id: str) -> dict[str, str]:
    return {normalize_course_number(c.course_number): c.id for c in db.scalars(select(Course).where(Course.version_id == version_id)).all()}


def opt_ids(course_map: dict[str, str], numbers: list[str]) -> list[str]:
    out, seen = [], set()
    for n in numbers:
        cid = course_map.get(normalize_course_number(n))
        if cid and cid not in seen:
            seen.add(cid)
            out.append(cid)
    return out


def ensure_minor(db, version_id: str, name: str) -> AcademicProgram:
    p = db.scalar(
        select(AcademicProgram).where(
            AcademicProgram.version_id == version_id,
            AcademicProgram.program_type == "MINOR",
            AcademicProgram.name == name,
        )
    )
    if p:
        return p
    p = AcademicProgram(version_id=version_id, name=name, program_type="MINOR", division=None)
    db.add(p)
    db.flush()
    return p


def cleanup_program_requirements(db, version_id: str, program_id: str) -> None:
    reqs = db.scalars(select(Requirement).where(Requirement.version_id == version_id, Requirement.program_id == program_id)).all()
    if not reqs:
        return
    req_ids = [r.id for r in reqs]
    links = db.scalars(select(RequirementBasketLink).where(RequirementBasketLink.requirement_id.in_(req_ids))).all()
    basket_ids = [x.basket_id for x in links]
    for row in db.scalars(select(RequirementSubstitution).where(RequirementSubstitution.requirement_id.in_(req_ids))).all():
        db.delete(row)
    for row in db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_(req_ids))).all():
        db.delete(row)
    for row in links:
        db.delete(row)
    for row in reqs:
        db.delete(row)
    db.flush()

    for basket_id in set(basket_ids):
        still_linked = db.scalar(select(RequirementBasketLink).where(RequirementBasketLink.basket_id == basket_id))
        if still_linked:
            continue
        for item in db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id == basket_id)).all():
            db.delete(item)
        basket = db.get(CourseBasket, basket_id)
        if basket:
            db.delete(basket)


def mk_req(db, **kwargs) -> Requirement:
    row = Requirement(**kwargs)
    db.add(row)
    db.flush()
    return row


def link_all_required(db, requirement_id: str, course_ids: list[str]) -> None:
    for idx, cid in enumerate(course_ids):
        db.add(
            RequirementFulfillment(
                requirement_id=requirement_id,
                course_id=cid,
                is_primary=(idx == 0),
                sort_order=idx,
            )
        )


def mk_basket(db, *, version_id: str, name: str, description: str, course_ids: list[str]) -> str:
    unique_ids = list(dict.fromkeys(course_ids))
    max_sort = db.scalar(
        select(CourseBasket.sort_order)
        .where(CourseBasket.version_id == version_id)
        .order_by(CourseBasket.sort_order.desc())
        .limit(1)
    )
    basket = CourseBasket(
        version_id=version_id,
        name=name,
        description=description,
        sort_order=(max_sort + 1) if max_sort is not None else 0,
    )
    db.add(basket)
    db.flush()
    for idx, cid in enumerate(unique_ids):
        db.add(CourseBasketItem(basket_id=basket.id, course_id=cid, sort_order=idx))
    return basket.id


def attach(db, requirement_id: str, basket_id: str, min_count: int = 1) -> None:
    db.add(
        RequirementBasketLink(
            requirement_id=requirement_id,
            basket_id=basket_id,
            min_count=min_count,
            max_count=None,
            sort_order=0,
        )
    )


def populate_aerospace_materials_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Aerospace Materials"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(
        db,
        version_id=version_id,
        parent_requirement_id=None,
        program_id=p.id,
        name="Minor - Aerospace Materials",
        logic_type="ALL_REQUIRED",
        pick_n=None,
        sort_order=0,
        category="MINOR",
        major_mode=None,
        track_name=None,
    )
    fixed = mk_req(
        db,
        version_id=version_id,
        parent_requirement_id=root.id,
        program_id=p.id,
        name="Minor Requirement: Foundational Course",
        logic_type="ALL_REQUIRED",
        pick_n=None,
        sort_order=0,
        category="MINOR",
        major_mode="REQUIREMENT",
        track_name=None,
    )
    link_all_required(db, fixed.id, opt_ids(cmap, ["Mech Engr 340"]))

    wrapper = mk_req(
        db,
        version_id=version_id,
        parent_requirement_id=root.id,
        program_id=p.id,
        name="Track - Materials Track: Any One",
        logic_type="ANY_ONE",
        pick_n=None,
        sort_order=1,
        category="MINOR",
        major_mode="TRACK",
        track_name="Materials Track",
    )
    tracks = {
        "Track - Structural Materials: Pick N": ["Aero Engr 482", "Astr Engr 351", "Chem 465", "Civ Engr 373", "Civ Engr 474", "Mech Engr 350", "Mech Engr 440", "Mech Engr 445", "Mech Engr 450"],
        "Track - Functional Materials: Pick N": ["Chem 440", "Chem 465", "ECE 321", "ECE 322", "ECE 373", "Mech Engr 440", "Physics 473", "Phys 473"],
        "Track - Computational Materials & Informatics: Pick N": ["Chem 440", "Chem 465", "Comp Sci 362", "Comp Sci 471", "Math 346", "Math 378", "Physics 473", "Phys 473"],
    }
    sort = 0
    for nm, arr in tracks.items():
        rr = mk_req(
            db,
            version_id=version_id,
            parent_requirement_id=wrapper.id,
            program_id=p.id,
            name=nm,
            logic_type="PICK_N",
            pick_n=3,
            sort_order=sort,
            category="MINOR",
            major_mode="TRACK",
            track_name=nm.replace("Track - ", "").replace(": Pick N", ""),
        )
        bb = mk_basket(db, version_id=version_id, name=f"{name} - {sort}", description=nm, course_ids=opt_ids(cmap, arr))
        attach(db, rr.id, bb, min_count=3)
        sort += 1

    hum = mk_req(
        db,
        version_id=version_id,
        parent_requirement_id=root.id,
        program_id=p.id,
        name="Track - Humanities/Social Science Choice: Any One",
        logic_type="ANY_ONE",
        pick_n=None,
        sort_order=2,
        category="MINOR",
        major_mode="TRACK",
        track_name="Humanities/Social Science Choice",
    )
    hum_ids = opt_ids(cmap, ["English 375", "Philos 330", "History 321", "Creat Art 315", "Creative Art 315", "Mgt 478", "MSS 302", "Pol Sci 445"])
    b_hum = mk_basket(db, version_id=version_id, name=f"{name} - Humanities", description="COI humanities/social science choice", course_ids=hum_ids)
    attach(db, hum.id, b_hum, min_count=1)


def populate_airpower_studies_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Airpower Studies"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Airpower Studies", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    fixed = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Minor Requirement: Foundational Courses", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="REQUIREMENT", track_name=None)
    link_all_required(db, fixed.id, opt_ids(cmap, ["History 320", "MSS 377"]))
    tech = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Technical Option: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Technical Option")
    b_tech = mk_basket(db, version_id=version_id, name=f"{name} - Technical", description="COI technical option", course_ids=opt_ids(cmap, ["Aero Engr 241", "Aero Engr 446", "Biology 345", "Chem 350", "Meteor 320", "Physics 370"]))
    attach(db, tech.id, b_tech, min_count=1)
    depth = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Depth Options: Pick N", logic_type="PICK_N", pick_n=2, sort_order=2, category="MINOR", major_mode="TRACK", track_name="Depth Options")
    depth_list = ["Aero Engr 241", "Aero Engr 351", "Aero Engr 446", "Beh Sci 375", "Biology 345", "Chem 350", "History 321", "History 327", "History 328", "History 329", "History 330", "Meteor 320", "MSS 343", "MSS 371", "MSS 381", "Physics 370", "Pol Sci 462", "Pol Sci 496"]
    b_depth = mk_basket(db, version_id=version_id, name=f"{name} - Depth", description="COI depth options", course_ids=opt_ids(cmap, depth_list))
    attach(db, depth.id, b_depth, min_count=2)


def populate_american_studies_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "American Studies"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - American Studies", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    all_courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()

    lit_ids = []
    for c in all_courses:
        num = str(c.course_number or "")
        if num.startswith("English "):
            m = re.search(r"(\d{3})", num)
            lvl = int(m.group(1)) if m else 0
            if lvl >= 300:
                lit_ids.append(c.id)
    lit = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - U.S. Literature: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="U.S. Literature")
    b_lit = mk_basket(db, version_id=version_id, name=f"{name} - Literature", description="Approved U.S. literature list (editable)", course_ids=lit_ids)
    attach(db, lit.id, b_lit, min_count=1)

    us_hist = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - U.S. History: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="U.S. History")
    b_hist = mk_basket(db, version_id=version_id, name=f"{name} - US History", description="COI U.S. history", course_ids=opt_ids(cmap, ["History 210", "History 220"]))
    attach(db, us_hist.id, b_hist, min_count=1)

    gov_law = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - U.S. Government/Law: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=2, category="MINOR", major_mode="TRACK", track_name="U.S. Government/Law")
    b_gl = mk_basket(db, version_id=version_id, name=f"{name} - GovtLaw", description="COI U.S. government/law", course_ids=opt_ids(cmap, ["Pol Sci 392", "Pol Sci 451", "Law 331", "Law 351"]))
    attach(db, gov_law.id, b_gl, min_count=1)

    elec = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - U.S.-Focused Electives: Pick N", logic_type="PICK_N", pick_n=2, sort_order=3, category="MINOR", major_mode="TRACK", track_name="U.S.-Focused Electives")
    elective_ids = opt_ids(cmap, ["Econ 340", "History 328", "History 340", "History 341", "History 342", "History 343", "History 344", "History 345", "History 346", "History 347", "Pol Sci 481", "Pol Sci 482", "Pol Sci 483", "Pol Sci 484", "Law 340", "Law 360", "Law 456", "MSS 491", "Philos 382"])
    elective_ids = list(dict.fromkeys(elective_ids + lit_ids))
    b_el = mk_basket(db, version_id=version_id, name=f"{name} - Electives", description="COI U.S.-focused electives", course_ids=elective_ids)
    attach(db, elec.id, b_el, min_count=2)


def populate_foreign_language_minor(db, version_id: str) -> None:
    name = "Foreign Language"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Foreign Language", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    all_courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    lang_ids = []
    for c in all_courses:
        num = str(c.course_number or "")
        if not num.startswith("For Lang "):
            continue
        m = re.search(r"(\d{3})", num)
        lvl = int(m.group(1)) if m else 0
        if lvl >= 200:
            lang_ids.append(c.id)
    rr = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Language Courses (200+): Pick N", logic_type="PICK_N", pick_n=5, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Language Courses")
    bb = mk_basket(db, version_id=version_id, name=f"{name} - Language Course Pool", description="COI: five For Lang courses at or above 200-level", course_ids=lang_ids)
    attach(db, rr.id, bb, min_count=5)


def populate_future_conflict_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Future Conflict"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Future Conflict", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    fixed = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Minor Requirement: Required Courses", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="REQUIREMENT", track_name=None)
    link_all_required(db, fixed.id, opt_ids(cmap, ["Interdis 331", "Interdis 350", "Creat Art 340", "Interdis 421"]))

    elect = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Elective Tracks: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Elective Tracks")
    track_lists = {
        "Track - Technology, Tactics, and Tools: Any One": ["Biology 365", "Civ Engr 351", "Civ Engr 356", "Chem 325", "Chem 350", "Comp Sci 220", "Comp Sci 471", "Comp Sci 472", "ECE 343", "ECE 386", "ECE 447", "Geo 488", "History 321", "Law 495", "Mgt 391", "MSS 302", "Philos 320", "Physics 242", "Physics 310", "Pol Sci 462", "Pol Sci 466", "Pol Sci 465"],
        "Track - Critical Thinking: Any One": ["Beh Sci 314", "Beh Sci 435", "Econ 411", "Econ 480", "English 370", "Mgt 400", "Philos 311", "Pol Sci 427", "Pol Sci 390"],
        "Track - Changing Character of War: Any One": ["Beh Sci 358", "Civ Engr 363", "Econ 454", "Geo 250", "English 411", "History 240", "History 250", "History 270", "History 322", "History 324", "History 325", "History 333", "Mgt 476", "Pol Sci 423", "Pol Sci 496", "Pol Sci 464"],
        "Track - National Security and Strategy: Any One": ["Biology 495", "Econ 374", "Econ 450", "Geo 375", "History 323", "History 340", "History 345", "Law 419", "Law 440", "Mgt 448", "MSS 490", "MSS 491", "MSS 493", "MSS 494", "Meteor 352", "Pol Sci 302", "Pol Sci 460"],
    }
    idx = 0
    for nm, arr in track_lists.items():
        rr = mk_req(
            db,
            version_id=version_id,
            parent_requirement_id=elect.id,
            program_id=p.id,
            name=nm,
            logic_type="ANY_ONE",
            pick_n=None,
            sort_order=idx,
            category="MINOR",
            major_mode="TRACK",
            track_name=nm.replace("Track - ", "").replace(": Any One", ""),
        )
        bb = mk_basket(db, version_id=version_id, name=f"{name} - Track {idx}", description=nm, course_ids=opt_ids(cmap, arr))
        attach(db, rr.id, bb, min_count=1)
        idx += 1


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id
        cmap = find_map(db, version_id)

        populate_aerospace_materials_minor(db, version_id, cmap)
        populate_airpower_studies_minor(db, version_id, cmap)
        populate_american_studies_minor(db, version_id, cmap)
        populate_foreign_language_minor(db, version_id)
        populate_future_conflict_minor(db, version_id, cmap)

        db.commit()
        print(f"Populated minor batch 1 in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

