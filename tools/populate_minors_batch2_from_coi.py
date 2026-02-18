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


def populate_global_logistics_management_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Global Logistics Management"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Global Logistics Management", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    fixed = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Minor Requirement: Foundational Courses", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="REQUIREMENT", track_name=None)
    link_all_required(db, fixed.id, opt_ids(cmap, ["Civ Engr 356", "Mgt 476", "Mgt 478"]))
    analytic = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Analytic Option: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Analytic Option")
    b_an = mk_basket(db, version_id=version_id, name=f"{name} - Analytic", description="COI analytic option", course_ids=opt_ids(cmap, ["Geo 440", "Math 378", "Mgt 391", "Ops Rsch 310"]))
    attach(db, analytic.id, b_an, min_count=1)
    breadth = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Breadth Option: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=2, category="MINOR", major_mode="TRACK", track_name="Breadth Option")
    breadth_list = ["Comp Sci 362", "Comp Sci 471", "Econ 374", "Econ 423", "Econ 480", "Geo 340", "Geo 360", "History 369", "Mgt 477", "Mgt 498", "MSS 421", "Pol Sci 445", "Soc Sci 444", "Soc Sci 483", "Sys Engr 336"]
    b_br = mk_basket(db, version_id=version_id, name=f"{name} - Breadth", description="COI breadth option", course_ids=opt_ids(cmap, breadth_list))
    attach(db, breadth.id, b_br, min_count=1)


def populate_hpc_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "High Performance Computing"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - High Performance Computing", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    wrapper = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - HPC Track: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="HPC Track")

    # CS track
    cs = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Computer Science HPC: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Computer Science HPC")
    link_all_required(db, cs.id, opt_ids(cmap, ["Math 340", "Comp Sci 220", "Comp Sci 362", "Comp Sci 380", "Comp Sci 471"]))

    # CE track
    ce = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Computer Engineering HPC: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Computer Engineering HPC")
    link_all_required(db, ce.id, opt_ids(cmap, ["ECE 281", "ECE 382", "ECE 383", "ECE 485"]))
    ce_opt = mk_req(db, version_id=version_id, parent_requirement_id=ce.id, program_id=p.id, name="Track - Computer Engineering HPC Option: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Computer Engineering HPC Option")
    b_ce = mk_basket(db, version_id=version_id, name=f"{name} - CE Option", description="CE track optional course", course_ids=opt_ids(cmap, ["Comp Sci 362", "ECE 386"]))
    attach(db, ce_opt.id, b_ce, min_count=1)

    # Computational aerodynamics
    aero = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Computational Aerodynamics HPC: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=2, category="MINOR", major_mode="TRACK", track_name="Computational Aerodynamics HPC")
    link_all_required(db, aero.id, opt_ids(cmap, ["Engr 346", "Math 346", "Aero Engr 241", "Aero Engr 341", "Aero Engr 342", "Aero Engr 472"]))

    # Data analytics track
    data = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - High Performance Data Analytics: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=3, category="MINOR", major_mode="TRACK", track_name="High Performance Data Analytics")
    link_all_required(db, data.id, opt_ids(cmap, ["Math 340", "Math 344", "Math 360", "Ops Rsch 311", "Comp Sci 471"]))
    cap = mk_req(db, version_id=version_id, parent_requirement_id=data.id, program_id=p.id, name="Track - Data Analytics Capstone Experience: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Data Analytics Capstone Experience")
    cap_ids = opt_ids(cmap, ["Ops Rsch 495", "Ops Rsch 499", "Comp Sci 495", "Comp Sci 499", "Data 495", "Data 499"])
    b_cap = mk_basket(db, version_id=version_id, name=f"{name} - Data Analytics Capstone", description="HPC data analytics capstone options", course_ids=cap_ids)
    attach(db, cap.id, b_cap, min_count=1)


def populate_nuclear_weapons_strategy_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Nuclear Weapons And Strategy"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Nuclear Weapons And Strategy", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    fixed = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Minor Requirement: Required Courses", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="REQUIREMENT", track_name=None)
    link_all_required(db, fixed.id, opt_ids(cmap, ["Physics 310", "Physics 354", "Physics 450", "Soc Sci 467"]))
    opt = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Option Course: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Option Course")
    b_opt = mk_basket(db, version_id=version_id, name=f"{name} - Option", description="COI option course", course_ids=opt_ids(cmap, ["Pol Sci 302", "MSS 423", "Chem 350", "History 320", "History 322"]))
    attach(db, opt.id, b_opt, min_count=1)


def populate_pre_health_professions_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Pre-Health Professions"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Pre-Health Professions", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    wrapper = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Pre-Health Track: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Pre-Health Track")
    tracks = {
        "Track - Pre-Medical / Pre-Dental: All Required": ["Biology 215", "Chem 233", "Chem 234", "Chem 243", "Biology 360", "Biology 363"],
        "Track - Pre-Nursing: All Required": ["Biology 332", "Biology 410", "Biology 431", "Biology 440", "Beh Sci 440"],
        "Track - Pre-Physician Assistant: All Required": ["Chem 230", "Biology 332", "Biology 410", "Biology 431", "Biology 440", "Chem 481"],
        "Track - Pre-Physical Therapy: All Required": ["Biology 215", "Biology 320", "Biology 410", "Biology 440", "Beh Sci 440"],
    }
    idx = 0
    for nm, arr in tracks.items():
        rr = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name=nm, logic_type="ALL_REQUIRED", pick_n=None, sort_order=idx, category="MINOR", major_mode="TRACK", track_name=nm.replace("Track - ", "").replace(": All Required", ""))
        link_all_required(db, rr.id, opt_ids(cmap, arr))
        idx += 1


def populate_quantum_technologies_minor(db, version_id: str, cmap: dict[str, str]) -> None:
    name = "Quantum Technologies"
    p = ensure_minor(db, version_id, name)
    cleanup_program_requirements(db, version_id, p.id)
    root = mk_req(db, version_id=version_id, parent_requirement_id=None, program_id=p.id, name="Minor - Quantum Technologies", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode=None, track_name=None)
    fixed = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Minor Requirement: Core Courses", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="REQUIREMENT", track_name=None)
    link_all_required(db, fixed.id, opt_ids(cmap, ["Physics 242", "Comp Sci 314", "Physics 314"]))

    wrapper = mk_req(db, version_id=version_id, parent_requirement_id=root.id, program_id=p.id, name="Track - Concentration: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Concentration")

    # Foundations of QM
    f = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Foundations of Quantum Mechanics: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Foundations of Quantum Mechanics")
    link_all_required(db, f.id, opt_ids(cmap, ["Physics 264", "Physics 465", "Philos 330"]))

    # Chemistry & Materials
    cm = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Chemistry and Materials: Pick N", logic_type="PICK_N", pick_n=2, sort_order=1, category="MINOR", major_mode="TRACK", track_name="Chemistry and Materials")
    b_cm = mk_basket(db, version_id=version_id, name=f"{name} - Chemistry and Materials", description="Concentration courses", course_ids=opt_ids(cmap, ["Chem 335", "Chem 336", "Mech Engr 340"]))
    attach(db, cm.id, b_cm, min_count=2)

    # Cyber
    cyber = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Cyber: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=2, category="MINOR", major_mode="TRACK", track_name="Cyber")
    link_all_required(db, cyber.id, opt_ids(cmap, ["Comp Sci 210", "Comp Sci 211", "Comp Sci 212"]))
    cyber_opt = mk_req(db, version_id=version_id, parent_requirement_id=cyber.id, program_id=p.id, name="Track - Cyber Advanced Choice: Any One", logic_type="ANY_ONE", pick_n=None, sort_order=0, category="MINOR", major_mode="TRACK", track_name="Cyber Advanced Choice")
    b_cy = mk_basket(db, version_id=version_id, name=f"{name} - Cyber Advanced", description="Cyber concentration advanced choice", course_ids=opt_ids(cmap, ["Comp Sci 471", "Cyber Sci 431"]))
    attach(db, cyber_opt.id, b_cy, min_count=1)

    # Engineering & Analysis
    ea = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Engineering and Analysis: Pick N", logic_type="PICK_N", pick_n=2, sort_order=3, category="MINOR", major_mode="TRACK", track_name="Engineering and Analysis")
    ea_list = ["Astr Engr 332", "ECE 281", "ECE 343", "Mech Engr 340", "Physics 291", "Physics 264", "Physics 465", "Math 344", "Math 360", "Math 346", "Engr 346", "Math 469", "Math 245", "ECE 245"]
    b_ea = mk_basket(db, version_id=version_id, name=f"{name} - Engineering and Analysis", description="Engineering and analysis concentration", course_ids=opt_ids(cmap, ea_list))
    attach(db, ea.id, b_ea, min_count=2)

    # Strategic impact
    si = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Strategic Impact: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=4, category="MINOR", major_mode="TRACK", track_name="Strategic Impact")
    link_all_required(db, si.id, opt_ids(cmap, ["MSS 302", "Pol Sci 466"]))

    # Ethical/legal
    el = mk_req(db, version_id=version_id, parent_requirement_id=wrapper.id, program_id=p.id, name="Track - Ethical and Legal Considerations: All Required", logic_type="ALL_REQUIRED", pick_n=None, sort_order=5, category="MINOR", major_mode="TRACK", track_name="Ethical and Legal Considerations")
    link_all_required(db, el.id, opt_ids(cmap, ["Philos 320", "Law 442"]))


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE version found")
        version_id = active.id
        cmap = find_map(db, version_id)

        populate_global_logistics_management_minor(db, version_id, cmap)
        populate_hpc_minor(db, version_id, cmap)
        populate_nuclear_weapons_strategy_minor(db, version_id, cmap)
        populate_pre_health_professions_minor(db, version_id, cmap)
        populate_quantum_technologies_minor(db, version_id, cmap)

        db.commit()
        print(f"Populated minor batch 2 in version {active.name} ({version_id})")


if __name__ == "__main__":
    main()

