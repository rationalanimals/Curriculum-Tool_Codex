import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
os.chdir(BACKEND)
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import (  # noqa: E402
    CurriculumVersion,
    Requirement,
    RequirementBasketLink,
    RequirementFulfillment,
    SessionLocal,
    CourseBasketItem,
    select,
)


CORE_REQ_NAME = "Core Requirement: All Required"
TRACK_NAMES = {
    "Track - Basic: All Required",
    "Track - Intermediate Science: Pick N",
    "Track - Intermediate Liberal Arts: Pick N",
    "Track - Advanced STEM: Any One",
    "Track - Advanced Liberal Arts: Any One",
    "Track - Advanced: Any One",
}


def main() -> None:
    with SessionLocal() as db:
        active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
        if not active:
            raise RuntimeError("No ACTIVE curriculum version found")
        version_id = active.id

        reqs = db.scalars(select(Requirement).where(Requirement.version_id == version_id, Requirement.category == "CORE")).all()
        by_name = {r.name: r for r in reqs}
        core_req = by_name.get(CORE_REQ_NAME)
        if not core_req:
            raise RuntimeError(f"Missing '{CORE_REQ_NAME}'")

        track_ids = [by_name[n].id for n in TRACK_NAMES if n in by_name]
        if not track_ids:
            raise RuntimeError("No core track nodes found")

        # Courses represented by track direct links.
        represented: set[str] = set(
            x.course_id
            for x in db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_(track_ids))).all()
        )

        # Courses represented by track baskets.
        basket_links = db.scalars(select(RequirementBasketLink).where(RequirementBasketLink.requirement_id.in_(track_ids))).all()
        basket_ids = [x.basket_id for x in basket_links]
        if basket_ids:
            represented |= set(
                x.course_id
                for x in db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id.in_(basket_ids))).all()
            )

        core_links = db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id == core_req.id)).all()
        removed = 0
        for link in core_links:
            if link.course_id in represented:
                db.delete(link)
                removed += 1

        db.commit()
        remaining = db.scalar(
            select(RequirementFulfillment.id).where(RequirementFulfillment.requirement_id == core_req.id).limit(1)
        )
        print(f"Active version: {active.name}")
        print(f"Core requirement overlaps removed: {removed}")
        print(f"Core requirement still has links: {bool(remaining)}")


if __name__ == "__main__":
    main()

