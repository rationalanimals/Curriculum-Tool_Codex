import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
os.chdir(BACKEND)
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import CurriculumVersion, Requirement, SessionLocal, select  # noqa: E402


CORE_CHILDREN = [
    {"name": "Core Requirement: All Required", "logic_type": "ALL_REQUIRED", "pick_n": None, "major_mode": "REQUIREMENT"},
    {"name": "Track - Basic: All Required", "logic_type": "ALL_REQUIRED", "pick_n": None, "major_mode": "TRACK", "track_name": "Basic"},
    {"name": "Track - Intermediate Science: Pick N", "logic_type": "PICK_N", "pick_n": 2, "major_mode": "TRACK", "track_name": "Intermediate Science"},
    {"name": "Track - Intermediate Liberal Arts: Pick N", "logic_type": "PICK_N", "pick_n": 2, "major_mode": "TRACK", "track_name": "Intermediate Liberal Arts"},
    {"name": "Track - Advanced STEM: Any One", "logic_type": "ANY_ONE", "pick_n": None, "major_mode": "TRACK", "track_name": "Advanced STEM"},
    {"name": "Track - Advanced Liberal Arts: Any One", "logic_type": "ANY_ONE", "pick_n": None, "major_mode": "TRACK", "track_name": "Advanced Liberal Arts"},
    {"name": "Track - Advanced: Any One", "logic_type": "ANY_ONE", "pick_n": None, "major_mode": "TRACK", "track_name": "Advanced"},
]


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE curriculum version found")

        version_id = active.id
        roots = db.scalars(
            select(Requirement).where(
                Requirement.version_id == version_id,
                Requirement.parent_requirement_id.is_(None),
                Requirement.program_id.is_(None),
                Requirement.category == "CORE",
            )
        ).all()
        if roots:
            core_root = sorted(roots, key=lambda r: (r.sort_order if r.sort_order is not None else 99999, r.id))[0]
            if core_root.name != "Core":
                core_root.name = "Core"
        else:
            max_top_sort = db.scalar(select(Requirement.sort_order).where(Requirement.version_id == version_id).order_by(Requirement.sort_order.desc()).limit(1))
            core_root = Requirement(
                version_id=version_id,
                parent_requirement_id=None,
                program_id=None,
                name="Core",
                logic_type="ALL_REQUIRED",
                sort_order=(max_top_sort + 1) if max_top_sort is not None else 0,
                category="CORE",
                major_mode=None,
                track_name=None,
            )
            db.add(core_root)
            db.flush()

        children = db.scalars(
            select(Requirement).where(
                Requirement.version_id == version_id,
                Requirement.parent_requirement_id == core_root.id,
                Requirement.category == "CORE",
            )
        ).all()
        by_name = {c.name: c for c in children}
        next_sort = 0 if not children else max((c.sort_order or 0) for c in children) + 1

        created = 0
        updated = 0
        for spec in CORE_CHILDREN:
            row = by_name.get(spec["name"])
            if row is None:
                row = Requirement(
                    version_id=version_id,
                    parent_requirement_id=core_root.id,
                    program_id=None,
                    name=spec["name"],
                    logic_type=spec["logic_type"],
                    pick_n=spec["pick_n"],
                    sort_order=next_sort,
                    category="CORE",
                    major_mode=spec.get("major_mode"),
                    track_name=spec.get("track_name"),
                )
                next_sort += 1
                db.add(row)
                created += 1
                continue
            changed = False
            if row.logic_type != spec["logic_type"]:
                row.logic_type = spec["logic_type"]
                changed = True
            if (row.pick_n or None) != (spec["pick_n"] or None):
                row.pick_n = spec["pick_n"]
                changed = True
            if (row.major_mode or None) != (spec.get("major_mode") or None):
                row.major_mode = spec.get("major_mode")
                changed = True
            if (row.track_name or None) != (spec.get("track_name") or None):
                row.track_name = spec.get("track_name")
                changed = True
            if changed:
                updated += 1

        db.commit()
        print(f"Active version: {active.name}")
        print(f"Core root: {core_root.name} ({core_root.id})")
        print(f"Core child nodes created: {created}")
        print(f"Core child nodes updated: {updated}")


if __name__ == "__main__":
    main()
