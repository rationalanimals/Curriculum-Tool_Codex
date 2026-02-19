# Course Metadata Deep-Dive Workflow

Date: 2026-02-18

Goal: Methodically curate course metadata course-by-course, including:
- credit hours
- fixed/offered periods
- standing requirements (example: `C2C standing`)
- additional enrollment/approval constraints
- prereq/coreq corrections

## Data fields now supported

Course fields:
- `credit_hours`
- `designated_semester`
- `offered_periods_json`
- `standing_requirement`
- `additional_requirements_text`
- `min_section_size`

## Artifacts and tools

- Full queue generator:
  - `python tools/generate_course_metadata_deep_dive_queue.py`
  - Output: `docs/course_metadata_deep_dive_queue.csv`
- Apply updates:
  - `python tools/apply_course_curation_csv.py <csv_path>`
- Template:
  - `docs/course_curation_updates_template.csv`

## CSV columns (curation)

- `new_credit_hours`
- `new_designated_semester`
- `new_offered_periods_json`
- `new_standing_requirement`
- `new_additional_requirements_text`
- `prereq_numbers_semicolon`
- `coreq_numbers_semicolon`

Notes:
- Leave a `new_*` field blank to keep existing value.
- To clear text fields, include the column and set blank.
- Prereq/coreq semicolon columns rebuild structured prereq rows for that course.

## Recommended execution cadence

1. Generate fresh queue CSV.
2. Curate one batch (10-30 courses max) in a batch CSV.
3. Apply the batch.
4. Re-run checks in Design Studio:
   - Course Detail Editor (spot check fields)
   - Program Feasibility / Course of Study Feasibility (watch for shifts)
5. Commit batch before next batch.

## Starter batch already applied

`docs/course_curation_batch_starter_credits.csv` (6 rows):
- Chem 100 -> 4.0
- Physics 110 -> 4.0
- Ldrshp 100 -> 0.75
- Ldrshp 200 -> 0.75
- Ldrshp 300 -> 0.75
- Ldrshp 400 -> 0.75

