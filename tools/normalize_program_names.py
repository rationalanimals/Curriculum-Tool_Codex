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
    PlanItem,
    Requirement,
    SessionLocal,
    select,
)


ALIAS_MAP = {
    "Aero": "Aeronautical Engineering",
    "Astro": "Astronautical Engineering",
    "ECE": "Electrical And Computer Engineering",
}


def normalize_names(version_id: str | None = None) -> None:
    db = SessionLocal()
    try:
        rows = db.scalars(select(AcademicProgram).order_by(AcademicProgram.name.asc())).all()
        by_name = {}
        for p in rows:
            if version_id and p.version_id != version_id:
                continue
            by_name.setdefault((p.version_id, p.name), []).append(p)

        deleted = 0
        skipped = []
        for alias, canonical in ALIAS_MAP.items():
            keys = [k for k in by_name.keys() if k[1] == alias]
            for key in keys:
                v_id = key[0]
                alias_rows = by_name.get((v_id, alias), [])
                canon_rows = by_name.get((v_id, canonical), [])
                if not alias_rows:
                    continue
                if not canon_rows:
                    # Safe rename when canonical does not exist.
                    for row in alias_rows:
                        row.name = canonical
                    continue
                # Canonical exists; delete alias only if unused.
                for row in alias_rows:
                    req_count = len(db.execute(select(Requirement).where(Requirement.program_id == row.id)).all())
                    plan_count = len(db.execute(select(PlanItem).where(PlanItem.major_program_id == row.id)).all())
                    if req_count == 0 and plan_count == 0:
                        db.delete(row)
                        deleted += 1
                    else:
                        skipped.append(
                            {
                                "version_id": v_id,
                                "alias": alias,
                                "program_id": row.id,
                                "req_count": req_count,
                                "plan_count": plan_count,
                            }
                        )
        db.commit()
        print(f"Deleted alias programs: {deleted}")
        if skipped:
            print("Skipped aliases with dependencies:")
            for s in skipped:
                print(
                    f"- version={s['version_id']} alias={s['alias']} id={s['program_id']} "
                    f"(requirements={s['req_count']}, plan_items={s['plan_count']})"
                )
    finally:
        db.close()


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    normalize_names(arg)

