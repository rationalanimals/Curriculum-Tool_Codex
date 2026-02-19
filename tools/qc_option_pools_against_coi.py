from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
REPORT_PATH = ROOT / "docs" / "academy_general_option_qc_report.csv"

NON_ACAD_PREFIXES = (
    "ARMNSHP ",
    "MIL TNG ",
    "LDRSHP ",
    "PHY ED ",
    "AX ",
    "AV ",
)


def course_level(course_number: str) -> int:
    m = re.search(r"(\d{3})", str(course_number or ""))
    return int(m.group(1)) if m else -1


def is_non_academic(course_number: str) -> bool:
    n = str(course_number or "").strip().upper()
    return any(n.startswith(prefix) for prefix in NON_ACAD_PREFIXES)


def should_keep(req_name: str, basket_desc: str, course_number: str, credit_hours: float) -> bool:
    req = str(req_name or "").lower()
    desc = str(basket_desc or "").lower()
    n = str(course_number or "")
    c = float(credit_hours or 0.0)
    lvl = course_level(n)
    if "academy option" in req:
        return c >= 3.0 and not is_non_academic(n)
    if "general option" in req:
        return c >= 3.0 and not is_non_academic(n)
    if "open option" in req:
        # COI wording in current General Engineering model: 200-level or higher course.
        return lvl >= 200 and c >= 3.0 and not is_non_academic(n)
    if "3+ semester-hour" in desc or "3.0+ semester" in desc:
        return c >= 3.0 and not is_non_academic(n)
    return True


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

    rows = cur.execute(
        """
        select
            r.id as requirement_id,
            r.name as requirement_name,
            r.logic_type,
            r.pick_n,
            p.name as program_name,
            p.program_type,
            b.id as basket_id,
            b.name as basket_name,
            b.description as basket_desc,
            bl.min_count
        from requirements r
        join academic_programs p on p.id = r.program_id
        join requirement_basket_links bl on bl.requirement_id = r.id
        join course_baskets b on b.id = bl.basket_id
        where r.version_id = ?
          and (
            lower(r.name) like '%academy option%'
            or lower(r.name) like '%general option%'
            or lower(r.name) like '%open option%'
          )
        order by p.program_type, p.name, r.name
        """,
        (version_id,),
    ).fetchall()

    report_rows: list[dict] = []
    removed_total = 0
    touched = 0
    for row in rows:
        basket_id = row["basket_id"]
        items = cur.execute(
            """
            select i.id as item_id, c.course_number, c.credit_hours
            from course_basket_items i
            join courses c on c.id = i.course_id
            where i.basket_id = ?
            order by i.sort_order, c.course_number
            """,
            (basket_id,),
        ).fetchall()
        kept_item_ids: list[str] = []
        removed_courses: list[str] = []
        for it in items:
            keep = should_keep(
                row["requirement_name"],
                row["basket_desc"],
                it["course_number"],
                float(it["credit_hours"] or 0.0),
            )
            if keep:
                kept_item_ids.append(it["item_id"])
            else:
                removed_courses.append(f"{it['course_number']} ({float(it['credit_hours'] or 0.0):g})")
        if removed_courses:
            touched += 1
            removed_total += len(removed_courses)
            # Delete removed rows.
            cur.execute(
                f"delete from course_basket_items where basket_id=? and id not in ({','.join('?' * len(kept_item_ids))})",
                (basket_id, *kept_item_ids),
            ) if kept_item_ids else cur.execute("delete from course_basket_items where basket_id=?", (basket_id,))

        # Re-read for report after cleanup.
        after_items = cur.execute(
            """
            select c.course_number, c.credit_hours
            from course_basket_items i
            join courses c on c.id = i.course_id
            where i.basket_id = ?
            order by i.sort_order, c.course_number
            """,
            (basket_id,),
        ).fetchall()
        credits = [float(x["credit_hours"] or 0.0) for x in after_items]
        min_count = max(1, int(row["min_count"] or 1))
        min_lb = sum(sorted(credits)[:min_count]) if len(credits) >= min_count else 0.0
        report_rows.append(
            {
                "program_type": row["program_type"],
                "program_name": row["program_name"],
                "requirement_name": row["requirement_name"],
                "logic_type": row["logic_type"],
                "pick_n": row["pick_n"] or "",
                "basket_name": row["basket_name"],
                "basket_desc": row["basket_desc"] or "",
                "basket_courses": len(after_items),
                "basket_min_count": min_count,
                "min_credit_lb": round(min_lb, 2),
                "removed_count": len(removed_courses),
                "removed_examples": "; ".join(removed_courses[:20]),
                "sample_courses": ", ".join([x["course_number"] for x in after_items[:15]]),
            }
        )

    if report_rows:
        with REPORT_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(report_rows[0].keys()))
            writer.writeheader()
            writer.writerows(report_rows)

    con.commit()
    con.close()
    print(
        {
            "version_id": version_id,
            "version_name": active["name"],
            "option_pools_found": len(rows),
            "option_pools_touched": touched,
            "basket_items_removed": removed_total,
            "report": str(REPORT_PATH),
        }
    )


if __name__ == "__main__":
    main()

