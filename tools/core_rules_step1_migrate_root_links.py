import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
os.chdir(BACKEND)
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import CurriculumVersion, Requirement, RequirementFulfillment, SessionLocal, select  # noqa: E402


ROOT_NAME = "Core"
TARGET_NAME = "Core Requirement: All Required"


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE curriculum version found")

        reqs = db.scalars(select(Requirement).where(Requirement.version_id == active.id, Requirement.category == "CORE")).all()
        by_name = {r.name: r for r in reqs}
        root = by_name.get(ROOT_NAME)
        target = by_name.get(TARGET_NAME)
        if not root or not target:
            raise RuntimeError(f"Missing required nodes. root={bool(root)} target={bool(target)}")

        root_links = db.scalars(
            select(RequirementFulfillment)
            .where(RequirementFulfillment.requirement_id == root.id)
            .order_by(RequirementFulfillment.sort_order.asc())
        ).all()
        target_links = db.scalars(
            select(RequirementFulfillment)
            .where(RequirementFulfillment.requirement_id == target.id)
            .order_by(RequirementFulfillment.sort_order.asc())
        ).all()
        target_by_course = {x.course_id: x for x in target_links}
        next_sort = (max((x.sort_order or 0) for x in target_links) + 1) if target_links else 0

        moved = 0
        deduped = 0
        for link in root_links:
            existing = target_by_course.get(link.course_id)
            if existing:
                # Keep target entry; remove duplicate root link.
                db.delete(link)
                deduped += 1
                continue
            link.requirement_id = target.id
            link.sort_order = next_sort
            next_sort += 1
            moved += 1

        db.commit()
        print(f"Active version: {active.name}")
        print(f"Moved links from '{ROOT_NAME}' to '{TARGET_NAME}': {moved}")
        print(f"Deduped root links removed: {deduped}")


if __name__ == "__main__":
    main()

