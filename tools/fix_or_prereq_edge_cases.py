from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"


EDGE_CASES = {
    # Math 243 AND (Aero Engr 210 OR Aero Engr 315); Coreq Physics 110
    "AERO ENGR 351": {
        "prereq_all": ["MATH 243"],
        "prereq_any_groups": [["AERO ENGR 210", "AERO ENGR 315"]],
        "coreq_all": ["PHYSICS 110"],
    },
    # Math 346 AND Math 356 AND (Engr 342 OR ECE 333 OR Mech Engr 325)
    "ASTR ENGR 499A": {
        "prereq_all": ["MATH 346", "MATH 356"],
        "prereq_any_groups": [["ENGR 342", "ECE 333", "MECH ENGR 325"]],
        "coreq_all": [],
    },
    # Comp Sci 220 AND (ECE 281 OR Comp Sci 351)
    "CREAT ART 499A": {
        "prereq_all": ["COMP SCI 220"],
        "prereq_any_groups": [["ECE 281", "COMP SCI 351"]],
        "coreq_all": [],
    },
    # (Econ 333 OR Ops Rsch 331) AND (Math 356 OR Math 377 OR Math 300)
    "ECON 377": {
        "prereq_all": [],
        "prereq_any_groups": [["ECON 333", "OPS RSCH 331"], ["MATH 356", "MATH 377", "MATH 300"]],
        "coreq_all": [],
    },
    # (Math 243 OR Math 253) AND Math 245
    "MATH 346": {
        "prereq_all": ["MATH 245"],
        "prereq_any_groups": [["MATH 243", "MATH 253"]],
        "coreq_all": [],
    },
    # (Math 344 OR Math 360) AND (Ops Rsch 310 OR Aero Engr 315); Coreq Math 243
    "OPS RSCH 311": {
        "prereq_all": [],
        "prereq_any_groups": [["MATH 344", "MATH 360"], ["OPS RSCH 310", "AERO ENGR 315"]],
        "coreq_all": ["MATH 243"],
    },
}


def norm(s: str) -> str:
    return " ".join((s or "").upper().split())


def insert_prereq(
    cur: sqlite3.Cursor,
    *,
    course_id: str,
    required_course_id: str,
    relationship_type: str,
    group_key: str | None = None,
    group_min_required: int | None = None,
    group_label: str | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO course_prerequisites(
            id, course_id, required_course_id, relationship_type, enforcement,
            prerequisite_group_key, group_min_required, group_label
        ) VALUES (?, ?, ?, ?, 'HARD', ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            course_id,
            required_course_id,
            relationship_type.upper(),
            group_key,
            group_min_required,
            group_label,
        ),
    )


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Missing DB: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    active = cur.execute(
        "select id,name from curriculum_versions where status='ACTIVE' order by created_at desc limit 1"
    ).fetchone()
    if not active:
        raise SystemExit("No ACTIVE curriculum version found")
    version_id = active["id"]

    rows = cur.execute("select id,course_number from courses where version_id=?", (version_id,)).fetchall()
    cid_by_num = {norm(r["course_number"]): r["id"] for r in rows}

    updated_courses = 0
    inserted_rows = 0
    skipped_missing_refs = 0

    for course_num, cfg in EDGE_CASES.items():
        course_id = cid_by_num.get(norm(course_num))
        if not course_id:
            continue

        # Reset prerequisites for this course, then reinsert curated logic.
        cur.execute("delete from course_prerequisites where course_id=?", (course_id,))
        updated_courses += 1

        for req_num in cfg.get("prereq_all", []):
            req_id = cid_by_num.get(norm(req_num))
            if not req_id:
                skipped_missing_refs += 1
                continue
            insert_prereq(
                cur,
                course_id=course_id,
                required_course_id=req_id,
                relationship_type="PREREQUISITE",
            )
            inserted_rows += 1

        for idx, group in enumerate(cfg.get("prereq_any_groups", [])):
            key = f"EDGE_{norm(course_num).replace(' ', '_')}_PRE_{idx+1}"
            label = f"Prerequisite disjunction {idx + 1}"
            for req_num in group:
                req_id = cid_by_num.get(norm(req_num))
                if not req_id:
                    skipped_missing_refs += 1
                    continue
                insert_prereq(
                    cur,
                    course_id=course_id,
                    required_course_id=req_id,
                    relationship_type="PREREQUISITE",
                    group_key=key,
                    group_min_required=1,
                    group_label=label,
                )
                inserted_rows += 1

        for req_num in cfg.get("coreq_all", []):
            req_id = cid_by_num.get(norm(req_num))
            if not req_id:
                skipped_missing_refs += 1
                continue
            insert_prereq(
                cur,
                course_id=course_id,
                required_course_id=req_id,
                relationship_type="COREQUISITE",
            )
            inserted_rows += 1

    con.commit()
    print(
        {
            "version_id": version_id,
            "version_name": active["name"],
            "updated_courses": updated_courses,
            "inserted_rows": inserted_rows,
            "skipped_missing_refs": skipped_missing_refs,
        }
    )


if __name__ == "__main__":
    main()
