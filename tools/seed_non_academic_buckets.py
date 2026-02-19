from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"


def is_phy_ed(course_number: str) -> bool:
    return str(course_number or "").upper().startswith("PHY ED ")


def phy_ed_level(course_number: str) -> int:
    raw = str(course_number or "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) < 3:
        return -1
    try:
        return int(digits[:3])
    except Exception:
        return -1


def bucket_codes_for_course(course_number: str, title: str) -> set[str]:
    num = str(course_number or "").strip()
    title_lc = str(title or "").lower()
    out: set[str] = set()
    if is_phy_ed(num):
        lvl = phy_ed_level(num)
        if 100 <= lvl < 800:
            out.add("NONACAD_PE")
        if "intercollegiate" in title_lc:
            out.add("NONACAD_ATHLETICS")
    if num.upper().startswith("LDRSHP ") or num.upper().startswith("CL "):
        out.add("NONACAD_LEADERSHIP")
    if num.upper().startswith("MIL TNG "):
        out.add("NONACAD_MIL_TRAINING")
    if num.upper().startswith("ARMNSHP "):
        out.add("NONACAD_AIRMANSHIP")
    return out


def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    active = cur.execute(
        "select id, name from curriculum_versions where status='ACTIVE' order by created_at desc limit 1"
    ).fetchone()
    if not active:
        raise SystemExit("No ACTIVE curriculum version found")
    version_id = active["id"]
    courses = cur.execute(
        "select id, course_number, title from courses where version_id=?",
        (version_id,),
    ).fetchall()
    existing = {
        (row["course_id"], str(row["bucket_code"] or "").strip().upper())
        for row in cur.execute(
            "select id, course_id, bucket_code from course_bucket_tags where course_id in (select id from courses where version_id=?)",
            (version_id,),
        ).fetchall()
    }
    added = 0
    for c in courses:
        cid = c["id"]
        codes = bucket_codes_for_course(c["course_number"], c["title"])
        for code in sorted(codes):
            key = (cid, code)
            if key in existing:
                continue
            cur.execute(
                "insert into course_bucket_tags(id, course_id, bucket_code, credit_hours_override, sort_order) values(?,?,?,?,?)",
                (str(uuid.uuid4()), cid, code, None, 0),
            )
            existing.add(key)
            added += 1

    con.commit()
    print(
        {
            "version_id": version_id,
            "version_name": active["name"],
            "courses_scanned": len(courses),
            "bucket_tags_added": added,
        }
    )
    con.close()


if __name__ == "__main__":
    main()

