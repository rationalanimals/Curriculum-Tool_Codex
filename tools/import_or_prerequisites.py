from __future__ import annotations

import csv
import hashlib
import re
import sqlite3
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
INPUT_CSV = ROOT / "docs" / "course_curation_prereq_or_resolution_queue.csv"

COURSE_TOKEN_RE = re.compile(r"\b([A-Za-z][A-Za-z&]*(?: [A-Za-z&]+){0,5} \d{3}[A-Z]?)\b")
OR_RE = re.compile(r"\b(or|either|and/or)\b", re.IGNORECASE)
LEADING_CONNECTOR_RE = re.compile(r"^(AND|OR|EITHER|BOTH|WITH|PLUS)\s+", re.IGNORECASE)


def norm(s: str) -> str:
    return " ".join((s or "").upper().split())


def split_pre_coreq(raw: str) -> tuple[str, str]:
    parts = re.split(r"\bcoreq:\s*", raw or "", maxsplit=1, flags=re.IGNORECASE)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return (raw or "").strip(), ""


def extract_clause_course_tokens(clause: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in COURSE_TOKEN_RE.findall(clause or ""):
        cleaned = LEADING_CONNECTOR_RE.sub("", token).strip()
        n = norm(cleaned)
        if not n or n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def stable_group_key(course_id: str, relationship_type: str, clause_text: str, clause_idx: int) -> str:
    payload = f"{course_id}|{relationship_type}|{clause_idx}|{clause_text.strip().upper()}"
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
    return f"AUTO_OR_{digest}"


def ensure_group_columns(conn: sqlite3.Connection) -> None:
    cols = conn.execute("PRAGMA table_info(course_prerequisites)").fetchall()
    names = {c[1] for c in cols}
    if "prerequisite_group_key" not in names:
        conn.execute("ALTER TABLE course_prerequisites ADD COLUMN prerequisite_group_key TEXT")
    if "group_min_required" not in names:
        conn.execute("ALTER TABLE course_prerequisites ADD COLUMN group_min_required INTEGER DEFAULT 1")
    if "group_label" not in names:
        conn.execute("ALTER TABLE course_prerequisites ADD COLUMN group_label TEXT")


def insert_row(
    conn: sqlite3.Connection,
    *,
    course_id: str,
    required_course_id: str,
    relationship_type: str,
    prerequisite_group_key: str | None,
    group_min_required: int | None,
    group_label: str | None,
) -> bool:
    existing = conn.execute(
        """
        SELECT id FROM course_prerequisites
        WHERE course_id=? AND required_course_id=? AND upper(relationship_type)=upper(?)
          AND coalesce(prerequisite_group_key,'') = coalesce(?, '')
        LIMIT 1
        """,
        (course_id, required_course_id, relationship_type, prerequisite_group_key),
    ).fetchone()
    if existing:
        return False

    conn.execute(
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
            prerequisite_group_key,
            group_min_required,
            group_label,
        ),
    )
    return True


def apply_for_relation(
    conn: sqlite3.Connection,
    *,
    course_id: str,
    relation_text: str,
    relationship_type: str,
    course_id_by_number: dict[str, str],
) -> tuple[int, int]:
    inserted = 0
    skipped_unmapped = 0
    clauses = [c.strip() for c in re.split(r";", relation_text or "") if c.strip()]
    for idx, clause in enumerate(clauses):
        tokens = extract_clause_course_tokens(clause)
        mapped_ids: list[str] = []
        for t in tokens:
            cid = course_id_by_number.get(t)
            if not cid or cid == course_id:
                skipped_unmapped += 1
                continue
            mapped_ids.append(cid)
        if not mapped_ids:
            continue

        if OR_RE.search(clause) and len(mapped_ids) >= 2:
            gkey = stable_group_key(course_id, relationship_type, clause, idx)
            label = f"{relationship_type.upper()} OR clause {idx + 1}"
            for rid in mapped_ids:
                if insert_row(
                    conn,
                    course_id=course_id,
                    required_course_id=rid,
                    relationship_type=relationship_type,
                    prerequisite_group_key=gkey,
                    group_min_required=1,
                    group_label=label,
                ):
                    inserted += 1
        else:
            for rid in mapped_ids:
                if insert_row(
                    conn,
                    course_id=course_id,
                    required_course_id=rid,
                    relationship_type=relationship_type,
                    prerequisite_group_key=None,
                    group_min_required=None,
                    group_label=None,
                ):
                    inserted += 1
    return inserted, skipped_unmapped


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Missing DB: {DB_PATH}")
    if not INPUT_CSV.exists():
        raise SystemExit(f"Missing OR queue CSV: {INPUT_CSV}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_group_columns(conn)

    active = conn.execute(
        "select id, name from curriculum_versions where status='ACTIVE' order by created_at desc limit 1"
    ).fetchone()
    if not active:
        raise SystemExit("No ACTIVE curriculum version found")
    version_id = active["id"]

    rows = conn.execute("select id, course_number from courses where version_id=?", (version_id,)).fetchall()
    course_id_by_number = {norm(r["course_number"]): r["id"] for r in rows}
    valid_course_ids = {r["id"] for r in rows}

    inserted_total = 0
    skipped_unmapped_total = 0
    processed_rows = 0
    skipped_rows = 0

    with INPUT_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            course_id = str(row.get("course_id") or "").strip()
            if not course_id or course_id not in valid_course_ids:
                skipped_rows += 1
                continue
            raw = str(row.get("raw_prereq_text") or "").strip()
            if not raw:
                skipped_rows += 1
                continue
            processed_rows += 1
            pre_text, coreq_text = split_pre_coreq(raw)
            ins, sk = apply_for_relation(
                conn,
                course_id=course_id,
                relation_text=pre_text,
                relationship_type="PREREQUISITE",
                course_id_by_number=course_id_by_number,
            )
            inserted_total += ins
            skipped_unmapped_total += sk
            ins, sk = apply_for_relation(
                conn,
                course_id=course_id,
                relation_text=coreq_text,
                relationship_type="COREQUISITE",
                course_id_by_number=course_id_by_number,
            )
            inserted_total += ins
            skipped_unmapped_total += sk

    conn.commit()
    print(
        {
            "version_id": version_id,
            "version_name": active["name"],
            "processed_rows": processed_rows,
            "skipped_rows": skipped_rows,
            "inserted_prerequisite_rows": inserted_total,
            "skipped_unmapped_tokens": skipped_unmapped_total,
            "source_csv": str(INPUT_CSV),
        }
    )


if __name__ == "__main__":
    main()
