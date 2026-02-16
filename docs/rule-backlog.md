# Rules Backlog (COI + Program Governance)

Last updated: 2026-02-16

This backlog reflects current implementation state and is ordered for execution in Design Studio first. Advisor-only checks remain documented but deferred from Design Studio execution.

## Current Status Snapshot

Implemented (Design Studio active):
- Program Design Rules tree + checklist validation integration.
- Core/major/minor rule structures, substitutions, timing windows.
- Program Feasibility and Course of Study Feasibility sections.
- Validation rules engine with editable rules and categories.
- Category-based non-execution for `Cadet Performance` and `Definitional`.
- ABET bucket framework:
  - Course bucket tagging (`ABET_MATH_BASIC_SCI`, `ABET_ENGINEERING_TOPICS`).
  - ABET threshold validation rules (config-driven).
- Program/Major Pathway validations:
  - Minor min courses/hours/upper-level checks.
  - Double-major division separation and additional-hours checks.
  - Double-major checks run from both majors' perspectives.

Implemented (documentation-only / non-executing in design):
- Definitional rule: upper-level course number minimum.
- Cadet Performance category rules placeholders.

## Prioritized Backlog

### P1. Cross-Program Timing and Feasibility Hardening
1. Validation Rule: Cross-program timing clash (selected major/minor/core).
- Scope: Program Feasibility + Course of Study Feasibility.
- Output: pass/fail per clash with course and constraint IDs.

2. Validation Rule: Required timing vs offered-period compatibility.
- Scope: Program Feasibility + Course of Study Feasibility.
- Depends on richer course offering constraints.

3. Validation Rule: Substitution group integrity.
- Detect invalid substitution graph patterns (self, duplicate, impossible selection cardinality).

### P2. Program Definition Completeness and Traceability
4. Validation Rule: Program definition completeness.
- Major/minor has required top-level nodes and resolvable course mappings.

5. Validation Rule: Requirement graph consistency.
- Detect dead branches, orphaned nodes, and unsatisfiable rule combinations.

6. Rule traceability IDs and mapping.
- Ensure every Program Design rule and Validation rule can be traced in outputs.

### P3. Non-Academic Graduation Requirements (Program Design + Validation)
7. Non-Academic Graduation Requirements node set.
- Physical Education completion + PEA threshold.
- Military training completion requirements.
- Leadership practicum requirements.
- Airmanship/athletics participation per residency semester.

8. Validation Rules for non-academic completions.
- Program Feasibility: rule-definition completeness and structural consistency.
- Course of Study Feasibility: student plan satisfaction status.

### P4. Governance and Policy Rules from COI

9. Transfer/validation policy checks.
- AP/IB/transfer/validation interactions with residency accounting and degree applicability.

10. Exchange/residency caps.
- Exchange-semester/hour limits and residency interaction checks.

11. Waiver-aware validation integration.
- Add waiver records to override/annotate failing checks with authority and effective term.
- Include waiver provenance in validation messages.

### P5. Advisor-Mode Rule Activation (Deferred)
12. Cadet Performance rules (advisor mode only).
- Minimum cumulative GPA.
- Minimum core GPA.
- Attempts/repeats policies.
- Deficiency/probation rules.

13. Advisor what-if feasibility.
- Completed coursework, waivers, validation credits, transfer credits.

## Data Model Backlog (Revised)

Already added:
- `course_bucket_tags` (course-to-bucket mapping with optional credit override).

Recommended next additions:
1. `waiver_records`
- Fields: `rule_id`, `requirement_id?`, `program_id?`, `authority_type`, `authority_ref`, `status`, `effective_start`, `effective_end`, `notes`.

2. `program_metadata` (extend current programs)
- Add: `program_classification` (disciplinary/interdisciplinary/divisional), governance metadata.

3. `course_offering_constraints`
- Add structured fields for term/period availability, schedule blocks, special scheduling restrictions.

4. `residency_ledger`
- Explicit accounting by source: in-residence, transfer, validation, exchange.

5. `rule_bindings` (optional normalization)
- Explicit map of rules to programs/requirements/buckets for explainability and performance.

## Execution Order (Recommended)

In priority order

## Notes on Naming and Scope

- Avoid using "COI" in rule titles; use policy-specific names (e.g., "Residency minimum in-residence hours").
- External-source rules should encode source in title when needed (e.g., "ABET EAC: ...").
- Definitional rules are visible/editable but non-executing in Design Studio.
