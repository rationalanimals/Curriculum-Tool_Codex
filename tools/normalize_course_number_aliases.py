from __future__ import annotations

import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"


def iter_tables(cur):
    rows = cur.execute(
        "select name from sqlite_master where type='table' and name not like 'sqlite_%'"
    ).fetchall()
    return [r[0] for r in rows]


def table_columns(cur, table: str):
    return [r[1] for r in cur.execute(f"pragma table_info({table})").fetchall()]


def remap_course_references(cur, old_id: str, new_id: str) -> int:
    changed = 0
    for table in iter_tables(cur):
        if table == "courses":
            continue
        cols = table_columns(cur, table)
        target_cols = [c for c in cols if "course_id" in c]
        for col in target_cols:
            cur.execute(f"update {table} set {col}=? where {col}=?", (new_id, old_id))
            changed += int(cur.rowcount or 0)
    return changed


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Missing DB: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # 1) Normalize "Ar Stu ###" -> "For Ar Stu ###"
    cur.execute(
        "update courses set course_number = replace(course_number, 'Ar Stu ', 'For Ar Stu ') "
        "where course_number like 'Ar Stu %'"
    )
    ar_stu_updated = int(cur.rowcount or 0)

    # 2) Resolve "Division Hum 200S" to "HUM 200S" by version
    versions = [r[0] for r in cur.execute("select distinct version_id from courses").fetchall()]
    merged = 0
    renamed = 0
    remapped_refs = 0
    deleted_dups = 0
    for vid in versions:
        hum = cur.execute(
            "select id from courses where version_id=? and course_number='HUM 200S' limit 1",
            (vid,),
        ).fetchone()
        div = cur.execute(
            "select id from courses where version_id=? and course_number='Division Hum 200S' limit 1",
            (vid,),
        ).fetchone()
        if not div:
            continue
        div_id = div[0]
        if hum:
            hum_id = hum[0]
            remapped_refs += remap_course_references(cur, div_id, hum_id)
            cur.execute("delete from courses where id=?", (div_id,))
            deleted_dups += int(cur.rowcount or 0)
            merged += 1
        else:
            cur.execute("update courses set course_number='HUM 200S' where id=?", (div_id,))
            renamed += int(cur.rowcount or 0)

    con.commit()
    con.close()

    print(f"Ar Stu -> For Ar Stu updated: {ar_stu_updated}")
    print(f"Division Hum merged into existing HUM 200S versions: {merged}")
    print(f"Division Hum renamed to HUM 200S (no existing HUM in version): {renamed}")
    print(f"References remapped from duplicate Division Hum rows: {remapped_refs}")
    print(f"Duplicate Division Hum rows deleted: {deleted_dups}")


if __name__ == "__main__":
    main()
