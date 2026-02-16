# Course Catalog QC Workflow (Manual COI Mode)

Use this workflow before additional major/minor pathway definition.

## 1) Generate QC report

With backend running:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/design/course-catalog-qc/<VERSION_ID>?session_token=<TOKEN>" | ConvertTo-Json -Depth 8 > docs/course_catalog_qc_report.json
```

Get `<VERSION_ID>` from the active version in Design Studio. Use your current login token for `<TOKEN>`.

Or run local DB-based QC generation (no running server required):

```powershell
python tools/generate_course_curation_queue.py
```

This writes:
- `docs/course_catalog_qc_report_pass2.json`
- `docs/course_curation_queue.csv`
- `docs/course_curation_updates_template.csv`

## 2) Triage anomalies in this order

1. `orphan_requirement_link`
- Requirement link points to missing course record.
- Fix by relinking requirement to correct course or deleting stale link.

2. `malformed_course_number`
- Course number format is not standard (`DEPT 123` / `DEPT 123A`).
- Normalize to official COI short-number format.

3. `missing_title`, `title_looks_like_credit_hours`, `title_looks_like_id`
- Replace with official short title from COI.
- Ensure title is short name, not hours or ID string.

4. `placeholder_course`
- Resolve `GENR xxx` placeholders into explicit real courses or explicit option-slot baskets.

5. `possible_missing_prereq`
- Validate against COI and add prereq rows where required.

## 3) Verify course coverage

From QC report:

- Check `prefix_counts` for expected departments.
- Confirm course prefixes span full COI set (not just A-H).

## 4) Re-run QC and close deltas

After each cleanup batch:

1. Re-run `/design/course-catalog-qc/{version_id}`.
2. Confirm anomaly count is reduced.
3. Confirm requirement tree course labels no longer show missing/ID-like values.

To apply curated updates from CSV:

1. Edit `docs/course_curation_updates_template.csv`:
- Set `action` to `UPDATE` or `DELETE`.
- Fill `new_title`/`new_credit_hours` and optional prereq/coreq numbers.

2. Apply:

```powershell
python tools/apply_course_curation_csv.py
```

## 5) Completion gate before pathway work

Proceed to major/minor pathway definition only when:

- no orphan requirement links remain,
- no malformed course numbers remain,
- no title-as-hours/ID anomalies remain,
- placeholders are either resolved or intentionally represented as option slots + baskets,
- prerequisite rows are populated per COI for required courses.
