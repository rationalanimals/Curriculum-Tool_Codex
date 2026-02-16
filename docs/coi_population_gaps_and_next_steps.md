# COI Manual Population: Gaps and Next Data Structures

This project is now running in manual COI mode (auto-parsing endpoints disabled).

## Current state after manual population run

- Active version populated with full COI major/minor program inventory.
- Requirement tree rebuilt with:
  - Core requirement baseline.
  - Major and minor root nodes.
  - Major requirement links from each available recommended sequence bundle.
- Major sequence bundles imported one-by-one into Canvas for QC.
- QC report generated at `docs/coi_manual_population_report.json`.

## Remaining COI gaps surfaced by QC

1. Some majors are still under-specified in COI sequence data.
- No sequence bundle currently available for:
  - Behavioral Sciences
  - Chemistry
  - Economics
  - Legal Studies
  - Mechanical Engineering
  - Operations Research
  - Physics

2. Residency validation failures are expected for under-defined programs.
- Many program definitions currently total below 125 in-residence hours because COI leaves open option slots.

3. Minor definitions are placeholders.
- Minor pathway rules fail because minors need explicit course sets (>=5 courses, >=15 hours, >=3 upper-level).

## Data structures to add next

1. Requirement option slots (critical).
- Represent COI open choices explicitly:
  - `slot_count`
  - `slot_credit_hours_min/max`
  - `eligible_source` (Core basket, Major basket, Division basket, Academy option)
  - `allow_substitutions`

2. Course baskets / catalog sets.
- Explicit reusable sets for:
  - Core track baskets.
  - Major option lists.
  - Minor option lists.
  - Division-approved option pools.

3. Program tracks as first-class entities.
- Track metadata separate from requirement names:
  - Track ID/name
  - owning program
  - required courses
  - option baskets

4. Course descriptive metadata extension.
- Current `Course` model lacks COI-rich detail:
  - long description
  - prerequisites text source
  - notes/constraints
  - seasonal/offering restrictions

5. Rule provenance metadata.
- Each validation/program rule should track:
  - source section/table in COI
  - applicability scope
  - effective date/revision

## Rule additions to encode from COI next

1. Open-slot fulfillment rules.
- Validate that defined option slots are filled by eligible courses.

2. Minor completion rules from explicit minor baskets.
- Move minor checks from placeholder assumptions to COI-defined course sets.

3. Core track pathway rules for every major.
- Encode major-specific required picks inside core Pick-N groups.

4. Residency accounting model.
- Distinguish in-residence, transfer, validation, exchange contributions in rule evaluation.

5. Summer period-specific offering constraints.
- Add course availability by summer period and validate schedule feasibility accordingly.

