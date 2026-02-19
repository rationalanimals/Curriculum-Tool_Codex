from __future__ import annotations

import csv
import hashlib
import sqlite3
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "backend" / "cmt.db"
REPORT_PATH = ROOT / "docs" / "basket_normalize_dedupe_report.csv"

NON_ACAD_PREFIXES = ("ARMNSHP ", "MIL TNG ", "LDRSHP ", "PHY ED ", "AX ", "AV ")


def is_non_academic(course_number: str) -> bool:
    n = str(course_number or "").strip().upper()
    return any(n.startswith(prefix) for prefix in NON_ACAD_PREFIXES)


def in_academy_open_family(req_name: str) -> bool:
    n = str(req_name or "").lower()
    return ("academy option" in n) or ("open academic option" in n)


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

    report_rows: list[dict[str, str]] = []

    # Stage 1: normalize academy/open-academic option pools by criteria.
    # Criteria: >=3.0 credits and not non-academic prefixes.
    canonical_ids = {
        r["id"]
        for r in cur.execute(
            "select id,course_number,credit_hours from courses where version_id=?",
            (version_id,),
        ).fetchall()
        if float(r["credit_hours"] or 0.0) >= 3.0 and not is_non_academic(r["course_number"])
    }

    rows = cur.execute(
        """
        select b.id as basket_id, b.name as basket_name, r.id as req_id, r.name as req_name
        from requirement_basket_links l
        join course_baskets b on b.id = l.basket_id
        join requirements r on r.id = l.requirement_id
        where b.version_id=?
          and (lower(r.name) like '%academy option%' or lower(r.name) like '%open academic option%')
        """,
        (version_id,),
    ).fetchall()

    normalized_basket_ids = sorted({r["basket_id"] for r in rows})
    for basket_id in normalized_basket_ids:
        before_ids = {
            x["course_id"]
            for x in cur.execute("select course_id from course_basket_items where basket_id=?", (basket_id,)).fetchall()
        }
        to_add = canonical_ids - before_ids
        to_remove = before_ids - canonical_ids
        if to_remove:
            cur.execute(
                f"delete from course_basket_items where basket_id=? and course_id in ({','.join('?' * len(to_remove))})",
                (basket_id, *sorted(to_remove)),
            )
        if to_add:
            max_sort = cur.execute(
                "select coalesce(max(sort_order),-1) from course_basket_items where basket_id=?",
                (basket_id,),
            ).fetchone()[0]
            sort_val = int(max_sort) + 1
            for cid in sorted(to_add):
                cur.execute(
                    "insert into course_basket_items(id,basket_id,course_id,sort_order) values(hex(randomblob(16)),?,?,?)",
                    (basket_id, cid, sort_val),
                )
                sort_val += 1
        report_rows.append(
            {
                "stage": "normalize",
                "basket_id": basket_id,
                "basket_name": cur.execute("select name from course_baskets where id=?", (basket_id,)).fetchone()[0],
                "action": "sync_to_canonical_academy_open",
                "details": f"added={len(to_add)} removed={len(to_remove)} final={len(canonical_ids)}",
            }
        )

    # Stage 2: dedupe exact-equal baskets only when semantically safe:
    # - all linked requirements have the same name; OR
    # - all linked requirements are in academy/open-academic family.
    baskets = cur.execute("select id,name from course_baskets where version_id=?", (version_id,)).fetchall()
    by_sig: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for b in baskets:
        cids = [r["course_id"] for r in cur.execute(
            "select course_id from course_basket_items where basket_id=? order by course_id", (b["id"],)
        ).fetchall()]
        sig = hashlib.sha1("|".join(cids).encode()).hexdigest()
        by_sig[sig].append(b)

    for group in [g for g in by_sig.values() if len(g) > 1]:
        group_ids = [b["id"] for b in group]
        link_rows = cur.execute(
            f"""
            select l.basket_id, l.requirement_id, l.min_count, r.name as req_name
            from requirement_basket_links l
            join requirements r on r.id=l.requirement_id
            where l.basket_id in ({','.join('?' * len(group_ids))})
            """,
            tuple(group_ids),
        ).fetchall()
        req_names = {r["req_name"] for r in link_rows}
        safe = False
        reason = ""
        if len(req_names) == 1:
            safe = True
            reason = "same_requirement_name"
        elif req_names and all(in_academy_open_family(n) for n in req_names):
            safe = True
            reason = "academy_open_family"
        else:
            reason = "semantic_mismatch"

        if not safe:
            for b in group:
                report_rows.append(
                    {
                        "stage": "dedupe",
                        "basket_id": b["id"],
                        "basket_name": b["name"],
                        "action": "skipped",
                        "details": reason,
                    }
                )
            continue

        # Choose canonical basket: most links, then shortest name.
        links_by_basket = defaultdict(int)
        for lr in link_rows:
            links_by_basket[lr["basket_id"]] += 1
        canonical = sorted(group, key=lambda b: (-links_by_basket[b["id"]], len(b["name"]), b["name"]))[0]
        canonical_id = canonical["id"]

        for b in group:
            bid = b["id"]
            if bid == canonical_id:
                report_rows.append(
                    {
                        "stage": "dedupe",
                        "basket_id": bid,
                        "basket_name": b["name"],
                        "action": "canonical_kept",
                        "details": reason,
                    }
                )
                continue
            # Relink requirement_basket_links to canonical, avoiding duplicates.
            links = cur.execute(
                "select requirement_id,min_count,max_count,sort_order from requirement_basket_links where basket_id=?",
                (bid,),
            ).fetchall()
            moved = 0
            for lk in links:
                exists = cur.execute(
                    "select 1 from requirement_basket_links where basket_id=? and requirement_id=?",
                    (canonical_id, lk["requirement_id"]),
                ).fetchone()
                if not exists:
                    cur.execute(
                        """
                        insert into requirement_basket_links(
                            id,requirement_id,basket_id,min_count,max_count,sort_order
                        ) values(hex(randomblob(16)),?,?,?,?,?)
                        """,
                        (
                            lk["requirement_id"],
                            canonical_id,
                            lk["min_count"] or 1,
                            lk["max_count"],
                            lk["sort_order"] or 0,
                        ),
                    )
                    moved += 1
            cur.execute("delete from requirement_basket_links where basket_id=?", (bid,))
            cur.execute("delete from course_basket_items where basket_id=?", (bid,))
            cur.execute("delete from course_baskets where id=?", (bid,))
            report_rows.append(
                {
                    "stage": "dedupe",
                    "basket_id": bid,
                    "basket_name": b["name"],
                    "action": "merged_into_canonical",
                    "details": f"canonical={canonical['name']} moved_links={moved} reason={reason}",
                }
            )

    con.commit()
    con.close()

    if report_rows:
        with REPORT_PATH.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["stage", "basket_id", "basket_name", "action", "details"])
            w.writeheader()
            w.writerows(report_rows)

    print(
        {
            "version_id": version_id,
            "version_name": active["name"],
            "normalized_option_baskets": len(normalized_basket_ids),
            "report": str(REPORT_PATH),
        }
    )


if __name__ == "__main__":
    main()
