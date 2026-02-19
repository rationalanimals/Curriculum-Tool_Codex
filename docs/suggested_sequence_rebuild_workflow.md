# Suggested Sequence Rebuild Workflow (One-at-a-Time, COI-Checked)

Date: 2026-02-18

Purpose: Rebuild `Suggested Major Sequences` from scratch, one major at a time, with QC checks before each import.

## Current Reset State

- Program naming aliases removed: `Aero`, `Astro`, `ECE`.
- Suggested sequence registry reset and restarted.
- Imported so far:
  - `Aeronautical Engineering Rec Seq COI2526` (43/43 course-number match, 0 warnings).

## Why one-by-one

Prior bulk parse produced sequence errors. One-by-one import enforces:
- explicit per-major QC output,
- human review against COI section text before import,
- controlled handling of optional/open-choice course slots.

## Tooling

- Normalize program names:
  - `python tools/normalize_program_names.py 26645fdc-5a9f-4752-93bb-323b1434ec32`
- QC + import one sequence:
  - Clear existing suggested sequence registry:
    - `python tools/import_suggested_sequence_one_by_one.py --version "COI 2025-2026" --clear-only`
  - Dry-run QC:
    - `python tools/import_suggested_sequence_one_by_one.py --version "COI 2025-2026" --major "Astronautical Engineering"`
  - Apply import:
    - `python tools/import_suggested_sequence_one_by_one.py --version "COI 2025-2026" --major "Astronautical Engineering" --apply`
  - First import after reset:
    - `python tools/import_suggested_sequence_one_by_one.py --version "COI 2025-2026" --major "Aeronautical Engineering" --clear-all --apply`

## QC Gates (must pass before apply, unless consciously forced)

1. `match_rate_percent` should be high (target >= 95%).
2. `unmatched_count` should be 0 or explainable.
3. No duplicate `(semester_index, position)` warnings.
4. Sequence aligns with COI recommended/suggested order for that major.
5. Semester-credit totals align with expected profile in:
   - `docs/canvas-sequences/expected_semester_credits.json`

## How to handle options/open-choice in suggested sequences

Recommended policy:
- Program Design Rules remain the source of truth for option logic (baskets, pick-N, track rules).
- Suggested Sequences are a recommended path visualization, not full requirement logic.
- If COI sequence leaves an open slot, encode it in suggested sequence as a placeholder course (currently `GENR`).
- Any imported sequence containing `GENR` is tagged with:
  - `options_note = "Includes GENR placeholder option slots for COI open-choice requirements."`

Interpretation:
- `GENR` means “open choice slot to be resolved by requirement baskets/rules.”
- It should not replace basket/option-slot logic in Program Design Rules.

## Major-by-major queue (next)

1. Astronautical Engineering
2. Basic Sciences
3. Biology
4. Civil Engineering
5. Computer Science
6. Cyber Science
7. Data Science
8. Electrical And Computer Engineering
9. English
10. Foreign Area Studies
11. General Engineering
12. Geospatial Science
13. History
14. Humanities
15. Management
16. Mathematics
17. Meteorology
18. Military & Strategic Studies
19. Philosophy
20. Political Science
21. Social Sciences
22. Systems Engineering

Majors currently lacking parseable sequence file (must be built manually from COI):
- Behavioral Sciences
- Chemistry
- Economics
- Legal Studies
- Mechanical Engineering
- Operations Research
- Physics

## UI behavior

After each import, the sequence appears in Design Studio:
- Top controls: `Suggested Major Sequences` dropdown.
- Action: `Load Suggested Sequence` loads that sequence into canvas.

These sequences are included in full dataset export/import under module `CANVAS`.
