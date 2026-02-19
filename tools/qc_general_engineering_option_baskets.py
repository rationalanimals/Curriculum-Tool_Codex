from __future__ import annotations

import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"


TARGET_REQ_NAMES = {
    "Track - Engineering Options: Pick N",
    "Track - Engineering/Basic Science Options: Pick N",
}


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
        "select id from academic_programs where version_id=? and program_type='MAJOR' and name='General Engineering'",
        (version_id,),
    ).fetchone()
    if not prog:
        raise SystemExit("General Engineering major not found in active version")
    program_id = prog["id"]

    links = cur.execute(
        """
        select r.name as requirement_name, b.id as basket_id, b.name as basket_name
        from requirements r
        join requirement_basket_links bl on bl.requirement_id = r.id
        join course_baskets b on b.id = bl.basket_id
        where r.version_id=? and r.program_id=? and r.name in (?, ?)
        """,
        (version_id, program_id, *list(TARGET_REQ_NAMES)),
    ).fetchall()
    if not links:
        raise SystemExit("Target General Engineering option baskets not found")

    removed = 0
    touched = 0
    for row in links:
        basket_id = row["basket_id"]
        items = cur.execute(
            """
            select i.id as item_id, c.course_number, c.credit_hours
            from course_basket_items i
            join courses c on c.id=i.course_id
            where i.basket_id=?
            """,
            (basket_id,),
        ).fetchall()
        keep_ids = [it["item_id"] for it in items if float(it["credit_hours"] or 0.0) >= 3.0]
        if len(keep_ids) != len(items):
            touched += 1
            removed += len(items) - len(keep_ids)
            if keep_ids:
                cur.execute(
                    f"delete from course_basket_items where basket_id=? and id not in ({','.join('?' * len(keep_ids))})",
                    (basket_id, *keep_ids),
                )
            else:
                cur.execute("delete from course_basket_items where basket_id=?", (basket_id,))

    con.commit()
    con.close()
    print(
        {
            "version_id": version_id,
            "version_name": active["name"],
            "baskets_touched": touched,
            "items_removed": removed,
        }
    )


if __name__ == "__main__":
    main()

