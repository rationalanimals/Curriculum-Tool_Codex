from __future__ import annotations
import sqlite3, uuid
from pathlib import Path

ROOT = Path(r'c:\Users\infin\OneDrive\Documents\USAFA\Curriculum Tool\V1')
DB = ROOT / 'backend' / 'cmt.db'

# course -> list of OR groups (each group is list of course_numbers; rel defaults to PREREQUISITE)
OR_GROUPS = {
    'Aero Engr 436': [['Aero Engr 210S', 'Aero Engr 315']],
    'Aero Engr 446': [['Aero Engr 241', 'Mech Engr 341']],
    'Astr Engr 321': [['Astr Engr 201', 'Comp Sci 211']],
    'Biology 360': [['Chem 230', 'Chem 233']],
    'Biology 363': [['Chem 230', 'Chem 233']],
    'Comp Sci 362': [['Comp Sci 210', 'Comp Sci 211', 'Comp Sci 212'], ['Math 356', 'Math 377']],
    'Comp Sci 364': [['Comp Sci 210', 'Comp Sci 211', 'Comp Sci 212']],
    'Comp Sci 380': [['Math 340', 'Math 374']],
    'Comp Sci 471': [['Comp Sci 210', 'Comp Sci 212']],
    'Econ 351': [['Econ 350', 'Econ 374']],
    'Econ 377': [['Econ 333', 'Ops Rsch 331'], ['Math 300', 'Math 356', 'Math 377']],
    'Econ 411': [['Econ 333', 'Ops Rsch 331']],
    'Econ 454': [['Econ 350', 'Econ 374']],
    'Econ 473': [['Econ 333', 'Ops Rsch 331']],
    'Econ 480': [['Econ 333', 'Econ 423']],
    'ECE 446': [['Math 346', 'Math 356', 'Math 377']],
    'ECE 484': [['ECE 373', 'ECE 383']],
    'Math 359': [['Math 356', 'Math 377']],
    'Math 378': [['Math 377', 'Math 356']],
    'Math 443': [['Math 346', 'Math 469'], ['Math 342', 'Physics 356']],
    'Math 470': [['Math 346', 'Math 469']],
    'Mech Engr 421': [['Mech Engr 320', 'Physics 355'], ['Math 346', 'Engr 346']],
    'Mech Engr 460': [['Math 300', 'Math 356']],
    'Ops Rsch 311': [['Math 344', 'Math 360']],
    'Phy Ed 344': [['Phy Ed 112', 'Phy Ed 152', 'Phy Ed 252', 'Phy Ed 352', 'Phy Ed 452']],
    'Pol Sci 302': [['Pol Sci 211', 'Soc Sci 212']],
    'Pol Sci 390': [['Soc Sci 212', 'Soc Sci 311']],
    'Pol Sci 392': [['Soc Sci 212', 'Soc Sci 311']],
}

def norm(s: str) -> str:
    return ' '.join((s or '').upper().split())

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row
cur = con.cursor()
vid = cur.execute("select id from curriculum_versions where status='ACTIVE' order by created_at desc limit 1").fetchone()['id']

course_rows = cur.execute("select id, course_number from courses where version_id=?", (vid,)).fetchall()
course_id_by_num = {norm(r['course_number']): r['id'] for r in course_rows}
canon_by_norm = {norm(r['course_number']): r['course_number'] for r in course_rows}

added = 0
updated = 0
skipped = 0

for course_num, groups in OR_GROUPS.items():
    course_id = course_id_by_num.get(norm(course_num))
    if not course_id:
        skipped += 1
        continue
    for idx, members in enumerate(groups, start=1):
        key = f"OR{idx}"
        label = f"Any one ({idx})"
        member_ids = []
        for m in members:
            mid = course_id_by_num.get(norm(m))
            if mid:
                member_ids.append((m, mid))
        if len(member_ids) < 2:
            continue

        for mnum, mid in member_ids:
            row = cur.execute(
                """
                select id, relationship_type, enforcement, prerequisite_group_key, group_min_required, group_label
                from course_prerequisites
                where course_id=? and required_course_id=? and upper(relationship_type)='PREREQUISITE'
                limit 1
                """,
                (course_id, mid),
            ).fetchone()
            if row:
                cur.execute(
                    """
                    update course_prerequisites
                    set prerequisite_group_key=?, group_min_required=?, group_label=?
                    where id=?
                    """,
                    (key, 1, label, row['id']),
                )
                updated += 1
            else:
                cur.execute(
                    """
                    insert into course_prerequisites
                    (id, course_id, required_course_id, relationship_type, enforcement, prerequisite_group_key, group_min_required, group_label)
                    values (?,?,?,?,?,?,?,?)
                    """,
                    (str(uuid.uuid4()), course_id, mid, 'PREREQUISITE', 'HARD', key, 1, label),
                )
                added += 1

con.commit()
print({'added': added, 'updated': updated, 'skipped_courses': skipped})
