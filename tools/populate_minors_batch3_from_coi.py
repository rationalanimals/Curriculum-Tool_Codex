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

from populate_ref_utils import resolve_course_ids_strict  # noqa: E402



def find_map(db, version_id: str) -> dict[str, str]:
    return {normalize_course_number(c.course_number): c.id for c in db.scalars(select(Course).where(Course.version_id == version_id)).all()}


def opt_ids(course_map: dict[str, str], numbers: list[str]) -> list[str]:
    return resolve_course_ids_strict(
        course_map,
        numbers,
        normalize_course_number,
        label="minors batch3 course refs",
    )


def ensure_minor(db, version_id: str, name: str) -> AcademicProgram:
    p = db.scalar(select(AcademicProgram).where(AcademicProgram.version_id == version_id, AcademicProgram.program_type == "MINOR", AcademicProgram.name == name))
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
        db.add(RequirementFulfillment(requirement_id=requirement_id, course_id=cid, is_primary=(idx == 0), sort_order=idx))


def mk_basket(db, *, version_id: str, name: str, description: str, course_ids: list[str]) -> str:
    unique_ids = list(dict.fromkeys(course_ids))
    max_sort = db.scalar(select(CourseBasket.sort_order).where(CourseBasket.version_id == version_id).order_by(CourseBasket.sort_order.desc()).limit(1))
    basket = CourseBasket(version_id=version_id, name=name, description=description, sort_order=(max_sort + 1) if max_sort is not None else 0)
    db.add(basket)
    db.flush()
    for idx, cid in enumerate(unique_ids):
        db.add(CourseBasketItem(basket_id=basket.id, course_id=cid, sort_order=idx))
    return basket.id


def attach(db, requirement_id: str, basket_id: str, min_count: int = 1) -> None:
    db.add(RequirementBasketLink(requirement_id=requirement_id, basket_id=basket_id, min_count=min_count, max_count=None, sort_order=0))


def populate_philosophy_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Philosophy"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Philosophy", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    hist = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - History and Topics: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="History and Topics")
    b_hist = mk_basket(db, version_id=version_id, name=f"{name} Minor - History", description="COI history and topics", course_ids=opt_ids(cmap, ["Philos 391", "Philos 392", "Philos 393", "Philos 394"]))
    attach(db, hist.id, b_hist, min_count=1)
    logic = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Logic Course: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Logic Course")
    b_logic = mk_basket(db, version_id=version_id, name=f"{name} Minor - Logic", description="COI logic course", course_ids=opt_ids(cmap, ["Philos 200", "Philos 370"]))
    attach(db, logic.id, b_logic, min_count=1)
    p_opts = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Philosophy Options: Pick N", logic_type="PICK_N", pick_n=2, sort_order=2, category="MINOR", major_mode="TRACK", track_name="Philosophy Options")
    # all philosophy courses provide flexible mapping
    all_philos = [c.id for c in db.scalars(select(Course).where(Course.version_id == version_id)).all() if str(c.course_number or "").startswith("Philos ")]
    b_opts = mk_basket(db, version_id=version_id, name=f"{name} Minor - Philosophy Options", description="Any two philosophy courses", course_ids=all_philos)
    attach(db, p_opts.id, b_opts, min_count=2)
    final = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Final Option: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=3, category="MINOR", major_mode="TRACK", track_name="Final Option")
    fin_ids = list(dict.fromkeys(all_philos + opt_ids(cmap, ["Econ 440", "Law 463", "Mgt 411", "Pol Sci 301", "Pol Sci 423", "Pol Sci 451"])))
    b_fin = mk_basket(db, version_id=version_id, name=f"{name} Minor - Final Option", description="Philosophy or approved non-philosophy option", course_ids=fin_ids)
    attach(db, final.id, b_fin, min_count=1)


def populate_religion_studies_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Religion Studies"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Religion Studies", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    fixed = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Minor Requirement: Core Courses", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="REQUIREMENT", track_name=None)
    link_all_required(db, fixed.id, opt_ids(cmap, ["Philos 401", "Philos 402", "Philos 420"]))
    elective = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Religion Studies Electives: Pick N", logic_type="PICK_N", pick_n=2, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Religion Studies Electives")
    elective_list = ["History 220", "History 230", "History 240", "History 250", "History 260", "History 270", "History 280", "History 290", "History 360", "History 361", "History 368", "Hum 430", "Hum 461", "Hum 463", "Law 351", "Law 360", "Mgt 440", "Philos 350", "Philos 365", "Philos 382", "Philos 392", "Pol Sci 477"]
    b_el = mk_basket(db, version_id=version_id, name=f"{name} Minor - Electives", description="COI Religion Studies electives", course_ids=opt_ids(cmap, elective_list))
    attach(db, elective.id, b_el, min_count=2)


def populate_robotics_autonomous_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Robotics And Autonomous Systems"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Robotics And Autonomous Systems", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    fixed = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Minor Requirement: Foundational Courses", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="REQUIREMENT", track_name=None)
    link_all_required(db, fixed.id, opt_ids(cmap, ["ECE 387", "Mech Engr 396", "Pol Sci 466"]))
    depth = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Depth Options: Pick N", logic_type="PICK_N", pick_n=2, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Depth Options")
    depth_list = ["Astr Engr 331", "Mech Engr 320", "Mech Engr 325", "Comp Sci 471", "Comp Sci 472", "ECE 487", "Econ 367", "Engr 341", "Engr 342", "History 321", "Law 440", "Math 344", "Math 360", "Mgt 400", "Philos 320"]
    b_depth = mk_basket(db, version_id=version_id, name=f"{name} Minor - Depth", description="COI robotics depth options", course_ids=opt_ids(cmap, depth_list))
    attach(db, depth.id, b_depth, min_count=2)


def populate_space_warfighting_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Space Warfighting"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Space Warfighting", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    wrapper = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Space Warfighting Track: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Space Warfighting Track")

    # Operator track
    op = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Operator: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Operator")
    link_all_required(db, op.id, opt_ids(cmap, ["Astr Engr 321", "Astr Engr 431", "Law 419", "Pol Sci 465"]))
    op_opt = mk_req(db, version_id=version_id, parent_requirement_id=op.id, program_id=p.id, name="Track - Operator Additional Course: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Operator Additional Course")
    op_list = ["Astr Engr 331", "Astr Engr 332", "Astr Engr 423", "Chem 325", "Econ 374", "Geo 340", "Geo 382", "Geo 482", "History 240", "History 270", "History 332", "MSS 490", "MSS 493", "Physics 375", "Physics 291", "Pol Sci 469", "Pol Sci 473"]
    b_op = mk_basket(db, version_id=version_id, name=f"{name} - Operator Additional", description="Operator additional choice", course_ids=opt_ids(cmap, op_list))
    attach(db, op_opt.id, b_op, min_count=1)

    # Intel track
    intel = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Intel: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Intel")
    link_all_required(db, intel.id, opt_ids(cmap, ["Astr Engr 431", "Geo 340", "Pol Sci 465"]))
    intel_opt = mk_req(db, version_id=version_id, parent_requirement_id=intel.id, program_id=p.id, name="Track - Intel Additional Courses: Pick N", logic_type="PICK_N", pick_n=2, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Intel Additional Courses")
    intel_list = ["Astr Engr 332", "Econ 374", "Geo 382", "Geo 482", "History 240", "History 270", "History 322", "MSS 490", "MSS 493", "Physics 375", "Physics 291", "Pol Sci 462", "Pol Sci 469", "Pol Sci 473"]
    b_in = mk_basket(db, version_id=version_id, name=f"{name} - Intel Additional", description="Intel additional choices", course_ids=opt_ids(cmap, intel_list))
    attach(db, intel_opt.id, b_in, min_count=2)

    # Digital track
    dig = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Digital: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=2, category="MINOR", major_mode="TRACK", track_name="Digital")
    link_all_required(db, dig.id, opt_ids(cmap, ["Astr Engr 431"]))
    dig_law = mk_req(db, version_id=version_id, parent_requirement_id=dig.id, program_id=p.id, name="Track - Digital Legal Choice: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Digital Legal Choice")
    b_dl = mk_basket(db, version_id=version_id, name=f"{name} - Digital Legal", description="Law 440 or Law 419", course_ids=opt_ids(cmap, ["Law 440", "Law 419"]))
    attach(db, dig_law.id, b_dl, min_count=1)
    dig_pol = mk_req(db, version_id=version_id, parent_requirement_id=dig.id, program_id=p.id, name="Track - Digital Policy Choice: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Digital Policy Choice")
    b_dp = mk_basket(db, version_id=version_id, name=f"{name} - Digital Policy", description="Pol Sci 466 or Pol Sci 465", course_ids=opt_ids(cmap, ["Pol Sci 466", "Pol Sci 465"]))
    attach(db, dig_pol.id, b_dp, min_count=1)
    dig_opt1 = mk_req(db, version_id=version_id, parent_requirement_id=dig.id, program_id=p.id, name="Track - Digital Technical Choice: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=2, category="MINOR", major_mode="TRACK", track_name="Digital Technical Choice")
    d1 = ["Astr Engr 332", "Comp Sci 364", "Comp Sci 380", "Comp Sci 467", "Cyber Sci 333", "Cyber Sci 334", "ECE 281", "ECE 348", "ECE 382", "Math 378", "Ops Rsch 312"]
    b_d1 = mk_basket(db, version_id=version_id, name=f"{name} - Digital Technical", description="Digital technical choices", course_ids=opt_ids(cmap, d1))
    attach(db, dig_opt1.id, b_d1, min_count=1)
    dig_opt2 = mk_req(db, version_id=version_id, parent_requirement_id=dig.id, program_id=p.id, name="Track - Digital Regional Choice: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=3, category="MINOR", major_mode="TRACK", track_name="Digital Regional Choice")
    d2 = ["History 240", "History 270", "History 322", "MSS 490", "MSS 493", "Pol Sci 469", "Pol Sci 473"]
    b_d2 = mk_basket(db, version_id=version_id, name=f"{name} - Digital Regional", description="Digital regional choices", course_ids=opt_ids(cmap, d2))
    attach(db, dig_opt2.id, b_d2, min_count=1)

    # Acquisition track
    acq = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Acquisition: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=3, category="MINOR", major_mode="TRACK", track_name="Acquisition")
    link_all_required(db, acq.id, opt_ids(cmap, ["Astr Engr 331", "Astr Engr 431", "Pol Sci 465", "Sys Engr 310", "Mgt 477"]))
    acq_opt = mk_req(db, version_id=version_id, parent_requirement_id=acq.id, program_id=p.id, name="Track - Acquisition Additional Course: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Acquisition Additional Course")
    a1 = ["Astr Engr 332", "Astr Engr 423", "Chem 325", "ECE 343", "ECE 348", "ECE 434", "ECE 446", "Geo 340", "Geo 382", "History 240", "History 270", "History 322", "Law 419", "MSS 490", "MSS 493", "Physics 375", "Physics 291", "Pol Sci 469", "Pol Sci 473"]
    b_a1 = mk_basket(db, version_id=version_id, name=f"{name} - Acquisition Additional", description="Acquisition additional choice", course_ids=opt_ids(cmap, a1))
    attach(db, acq_opt.id, b_a1, min_count=1)


def populate_sustainability_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Sustainability"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Sustainability", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    fixed = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Minor Requirement: Required Courses", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="REQUIREMENT", track_name=None)
    link_all_required(db, fixed.id, opt_ids(cmap, ["Civ Engr 356", "Civ Engr 456"]))
    wrapper = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Sustainability Concentration: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Sustainability Concentration")

    # Sociocultural concentration: breadth 1 + depth two-course track
    soc = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Sociocultural Concentration: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Sociocultural Concentration")
    soc_b = mk_req(db, version_id=version_id, parent_requirement_id=soc.id, program_id=p.id, name="Track - Sociocultural Breadth: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Sociocultural Breadth")
    sb = ["Beh Sci 366B", "English 375", "History 369", "Mgt 478", "Philos 320"]
    b_sb = mk_basket(db, version_id=version_id, name=f"{name} - Sociocultural Breadth", description="Sociocultural breadth", course_ids=opt_ids(cmap, sb))
    attach(db, soc_b.id, b_sb, min_count=1)
    soc_d = mk_req(db, version_id=version_id, parent_requirement_id=soc.id, program_id=p.id, name="Track - Sociocultural Depth Track: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Sociocultural Depth Track")
    for idx, (nm, arr) in enumerate({
        "Track - Geopolitics: All Required": ["Geo 375", "Pol Sci 445"],
        "Track - Sociocultural Patterns: All Required": ["Geo 250", "Geo 412"],
    }.items()):
        rr = mk_req(db, version_id=version_id, parent_requirement_id=soc_d.id, program_id=p.id, name=nm, logic_type="ALL_REQUIRED", pick_n=None, sort_order=idx, category="MINOR", major_mode="TRACK", track_name=nm.replace("Track - ", "").replace(": All Required", ""))
        link_all_required(db, rr.id, opt_ids(cmap, arr))

    # Environmental concentration
    env = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Environmental Concentration: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Environmental Concentration")
    env_b = mk_req(db, version_id=version_id, parent_requirement_id=env.id, program_id=p.id, name="Track - Environmental Breadth: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Environmental Breadth")
    eb = ["Beh Sci 366B", "Chem 381C", "Civ Engr 362C", "Civ Engr 363", "Geo 365", "Geo 375", "Philos 320"]
    b_eb = mk_basket(db, version_id=version_id, name=f"{name} - Environmental Breadth", description="Environmental breadth", course_ids=opt_ids(cmap, eb))
    attach(db, env_b.id, b_eb, min_count=1)
    env_d = mk_req(db, version_id=version_id, parent_requirement_id=env.id, program_id=p.id, name="Track - Environmental Depth Track: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Environmental Depth Track")
    env_tracks = {
        "Track - Ecology: All Required": ["Biology 380", "Biology 481"],
        "Track - Climate: All Required": ["Meteor 320", "Meteor 352"],
        "Track - Environmental Geography: All Required": ["Geo 382P", "Geo 366"],
        "Track - Geomorphology: All Required": ["Geo 351", "Geo 353"],
        "Track - Energy: All Required": ["Mech Engr 312", "Mech Engr 468"],
    }
    idx = 0
    for nm, arr in env_tracks.items():
        rr = mk_req(db, version_id=version_id, parent_requirement_id=env_d.id, program_id=p.id, name=nm, logic_type="ALL_REQUIRED", pick_n=None, sort_order=idx, category="MINOR", major_mode="TRACK", track_name=nm.replace("Track - ", "").replace(": All Required", ""))
        link_all_required(db, rr.id, opt_ids(cmap, arr))
        idx += 1

    # Global economy and data management concentration
    ge = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Global Economy and Data Management Concentration: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=2, category="MINOR", major_mode="TRACK", track_name="Global Economy and Data Management Concentration")
    ge_b = mk_req(db, version_id=version_id, parent_requirement_id=ge.id, program_id=p.id, name="Track - Global Economy/Data Breadth: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Global Economy/Data Breadth")
    gb = ["Econ 374", "Geo 375", "Mgt 478", "Philos 320", "Pol Sci 445"]
    b_gb = mk_basket(db, version_id=version_id, name=f"{name} - Global Econ Breadth", description="Global economy/data breadth", course_ids=opt_ids(cmap, gb))
    attach(db, ge_b.id, b_gb, min_count=1)
    ge_d = mk_req(db, version_id=version_id, parent_requirement_id=ge.id, program_id=p.id, name="Track - Global Economy/Data Depth Track: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Global Economy/Data Depth Track")
    ge_tracks = {
        "Track - Geospatial Economy: All Required": ["Geo 250", "Geo 360"],
        "Track - Database Management: All Required": ["Comp Sci 211", "Data 364"],
        "Track - Geographic Data Management: All Required": ["Geo 310", "Geo 340"],
        "Track - Economics and Statistics: All Required": ["Math 377", "Math 378"],
    }
    idx = 0
    for nm, arr in ge_tracks.items():
        rr = mk_req(db, version_id=version_id, parent_requirement_id=ge_d.id, program_id=p.id, name=nm, logic_type="ALL_REQUIRED", pick_n=None, sort_order=idx, category="MINOR", major_mode="TRACK", track_name=nm.replace("Track - ", "").replace(": All Required", ""))
        link_all_required(db, rr.id, opt_ids(cmap, arr))
        idx += 1


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id
        cmap = find_map(db, version_id)
        populate_philosophy_minor(db, version_id, cmap)
        populate_religion_studies_minor(db, version_id, cmap)
        populate_robotics_autonomous_minor(db, version_id, cmap)
        populate_space_warfighting_minor(db, version_id, cmap)
        populate_sustainability_minor(db, version_id, cmap)
        db.commit()
        print(f"Populated minor batch 3 in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()
