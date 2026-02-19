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

    prog = cur.execute(
        "select id from academic_programs where version_id=? and program_type='MINOR' and name='Sustainability'",
        (version_id,),
    ).fetchone()
    if not prog:
        raise SystemExit("Sustainability minor not found")
    program_id = prog["id"]

    course_rows = cur.execute(
        "select id,course_number from courses where version_id=?",
        (version_id,),
    ).fetchall()
    course_id_by_num = {norm(r["course_number"]): r["id"] for r in course_rows}

    def ensure_req_links(req_name: str, course_numbers: list[str]) -> int:
        req = cur.execute(
            "select id from requirements where version_id=? and program_id=? and name=? limit 1",
            (version_id, program_id, req_name),
        ).fetchone()
        if not req:
            return 0
        req_id = req["id"]
        existing = {
            row["course_id"]
            for row in cur.execute(
                "select course_id from requirement_fulfillment where requirement_id=?",
                (req_id,),
            ).fetchall()
        }
        added = 0
        for n in course_numbers:
            cid = course_id_by_num.get(norm(n))
            if not cid or cid in existing:
                continue
            cur.execute(
                "insert into requirement_fulfillment(id, requirement_id, course_id, is_primary, sort_order, required_semester, required_semester_min, required_semester_max) values(?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), req_id, cid, 1, 0, None, None, None),
            )
            added += 1
        return added

    def ensure_basket_items(basket_name: str, course_numbers: list[str]) -> int:
        basket = cur.execute(
            "select id from course_baskets where version_id=? and name=? limit 1",
            (version_id, basket_name),
        ).fetchone()
        if not basket:
            return 0
        basket_id = basket["id"]
        existing = {
            row["course_id"]
            for row in cur.execute(
                "select course_id from course_basket_items where basket_id=?",
                (basket_id,),
            ).fetchall()
        }
        max_sort = cur.execute(
            "select coalesce(max(sort_order), -1) from course_basket_items where basket_id=?",
            (basket_id,),
        ).fetchone()[0]
        added = 0
        for n in course_numbers:
            cid = course_id_by_num.get(norm(n))
            if not cid or cid in existing:
                continue
            max_sort += 1
            cur.execute(
                "insert into course_basket_items(id, basket_id, course_id, sort_order) values(?,?,?,?)",
                (str(uuid.uuid4()), basket_id, cid, int(max_sort)),
            )
            added += 1
        return added

    added_req_links = 0
    added_req_links += ensure_req_links("Track - Ecology: All Required", ["Biology 380", "Biology 481"])
    added_req_links += ensure_req_links("Track - Environmental Geography: All Required", ["Geo 382", "Geo 366"])

    added_basket_items = 0
    added_basket_items += ensure_basket_items("Sustainability - Sociocultural Breadth", ["Beh Sci 366"])
    added_basket_items += ensure_basket_items("Sustainability - Environmental Breadth", ["Beh Sci 366", "Chem 381", "Civ Engr 362"])

    con.commit()
    con.close()
    print(
        {
            "version_id": version_id,
            "version_name": active["name"],
            "added_requirement_links": added_req_links,
            "added_basket_items": added_basket_items,
        }
    )


if __name__ == "__main__":
    main()

