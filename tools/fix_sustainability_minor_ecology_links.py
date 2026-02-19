from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"


def norm(s: str) -> str:
    return " ".join(str(s or "").upper().split())


def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    active = cur.execute(
        "select id,name from curriculum_versions where status='ACTIVE' order by created_at desc limit 1"
    ).fetchone()
    if not active:
        raise SystemExit("No ACTIVE curriculum version found")
    version_id = active["id"]

    program = cur.execute(
        "select id from academic_programs where version_id=? and program_type='MINOR' and name='Sustainability'",
        (version_id,),
    ).fetchone()
    if not program:
        raise SystemExit("Sustainability minor not found")
    program_id = program["id"]

    ecology_req = cur.execute(
        """
        select id
        from requirements
        where version_id=? and program_id=? and name='Track - Ecology: All Required'
        limit 1
        """,
        (version_id, program_id),
    ).fetchone()
    if not ecology_req:
        raise SystemExit("Ecology requirement node not found")
    req_id = ecology_req["id"]

    courses = cur.execute(
        "select id, course_number from courses where version_id=?",
        (version_id,),
    ).fetchall()
    by_num = {norm(c["course_number"]): c["id"] for c in courses}
    needed = [by_num.get(norm("Biology 380")), by_num.get(norm("Biology 481"))]
    needed = [x for x in needed if x]
    if len(needed) < 2:
        raise SystemExit("Missing Biology 380 or Biology 481 in active catalog")

    existing = {
        row["course_id"]
        for row in cur.execute(
            "select course_id from requirement_fulfillment where requirement_id=?",
            (req_id,),
        ).fetchall()
    }
    added = 0
    for cid in needed:
        if cid in existing:
            continue
        cur.execute(
            "insert into requirement_fulfillment(id, requirement_id, course_id, is_primary, sort_order, required_semester, required_semester_min, required_semester_max) values(?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), req_id, cid, 1, 0, None, None, None),
        )
        added += 1

    con.commit()
    con.close()
    print(
        {
            "version_id": version_id,
            "version_name": active["name"],
            "requirement_id": req_id,
            "links_added": added,
        }
    )


if __name__ == "__main__":
    main()
