# USAFA CMT 80% Delivery Plan (Design Studio + Resource/Instructor Ilities)

Date: 2026-02-18
Scope baseline: COI manual population state + current Phases 1/2 implementation
Primary objective: Deliver an 80% product that reliably evaluates alternative curricula and their feasibility/resource implications.

## 1) Product Goal and Success Criteria

### Goal
Provide a stable, explainable curriculum design environment where leadership can:
- Define core/major/minor pathways with explicit requirement logic.
- Build and compare alternative course-of-study canvases.
- Evaluate feasibility against program design rules and validation rules.
- Quantify resource and instructor-load impacts with actionable pass/fail findings.

### 80% Success Criteria
- At least 90% of major/minor pathway structures are explicitly modeled (minimal placeholders).
- Program Feasibility and Course of Study Feasibility both produce explainable, repeatable results.
- Resource/instructor checks are decision-useful (not placeholder estimates).
- Summer-period and timing constraints materially affect validation outcomes.
- Scenario save/export/import supports reproducible comparison and archival.

## 2) Current-State Summary (from existing artifacts)

### Confirmed strengths
- Design Studio rule editing, requirement tree, canvas, feasibility views, and validation rules are operational.
- Dataset import/export exists and supports modular bundles.
- Core/major/minor structures and timing/substitution primitives exist.

### High-impact gaps
- Under-specified majors remain (Behavioral Sciences, Chemistry, Economics, Legal Studies, Mechanical Engineering, Operations Research, Physics).
- Placeholder option courses still exist (GENR placeholders in course QC report).
- Option-slot semantics are not fully first-class across all validations.
- Resource/instructor feasibility is partially modeled but not yet robust enough for institutional decisions.

Sources:
- `docs/coi_population_gaps_and_next_steps.md`
- `docs/rule-backlog.md`
- `docs/coi_manual_population_report.json`
- `docs/course_catalog_qc_report_pass2.json`

## 3) Delivery Principles

- Prioritize model correctness over feature breadth.
- Convert implicit assumptions into visible/editable rules.
- Keep all feasibility outcomes explainable with rule IDs and plain-language messages.
- Avoid hard-coded policy text; encode policy through editable rule config.
- Preserve backward compatibility for existing datasets whenever practical.

## 4) Execution Plan (2-week sprints)

## Sprint 1: COI Completion and Placeholder Burn-Down

### Objectives
- Close major-definition gaps and reduce placeholder-driven failures.

### Work
- Complete program-design population for under-specified majors.
- Replace/retire placeholder `GENR` pathways with real baskets/option slots where COI leaves open choices.
- Run manual QC pass and regenerate population reports.

### Acceptance criteria
- All listed gap majors have explicit requirement trees and course mappings.
- Placeholder anomalies reduced to only intentionally retained open slots (documented).
- Program Feasibility shows no false fails caused by missing structure for those majors.

### Artifact outputs
- Updated `docs/coi_manual_population_report.json`
- Updated `docs/course_catalog_qc_report_pass2.json`
- Gap closure note appended to `docs/coi_population_gaps_and_next_steps.md`

## Sprint 2: Option Slots + Baskets as First-Class Model

### Objectives
- Represent COI open-choice logic natively and validate it consistently.

### Work
- Finalize option-slot data model fields:
  - slot count
  - eligibility source/pool
  - min/max credit bounds (if applicable)
  - substitution eligibility policy
- Ensure baskets can satisfy pick/choose logic cleanly.
- Ensure core-rules modal and feasibility engines consume basket/slot definitions uniformly.

### Acceptance criteria
- No need to create fake courses to represent open choices.
- Pick N / Any One behavior is consistent in Program Design, Program Feasibility, and Course of Study Feasibility.
- Output messages identify unmet slot constraints clearly.

### Artifact outputs
- Data-model note in `docs/` (schema + migration notes)
- Rule examples added to `docs/rule-backlog.md`

## Sprint 3: Program Design Consistency Hardening

### Objectives
- Catch broken requirement structures before downstream analysis.

### Work
- Implement/strengthen rules for:
  - program completeness
  - requirement graph consistency (orphan/dead/unsatisfiable branches)
  - substitution integrity (self, duplicate, impossible cardinality)
- Improve issue messaging and traceability IDs in both feasibility views.

### Acceptance criteria
- Deliberately malformed requirement structures are flagged deterministically.
- Program Design Rules consistency section reports specific actionable failures.
- No duplicate/ambiguous rule reporting for equivalent issues.

### Artifact outputs
- Updated validation rule catalog and examples
- Regression checklist for consistency rules

## Sprint 4: Resource and Instructor Ilities MVP

### Objectives
- Make resource feasibility materially useful for alternative-curriculum decisions.

### Work
- Upgrade checks for:
  - instructor qualification coverage by course/section
  - instructor load limits per academic period
  - section demand vs available staffing assumptions
  - classroom-type/capacity constraints
- Add rule-level pass/fail outputs in Program Feasibility and Course of Study Feasibility.

### Acceptance criteria
- Resource-related failures identify affected course/period/capacity constraint.
- At least one alternative curriculum comparison demonstrates different instructor-load outcomes.
- Results can be exported and re-imported without loss of interpretability.

### Artifact outputs
- Resource rule definitions in Validation Rules catalog
- Example comparison report (baseline vs alternative)

## Sprint 5: Timing/Offering Feasibility + Summer Integration

### Objectives
- Ensure plans are schedule-feasible, not just requirement-complete.

### Work
- Enforce offering-period compatibility (semester/summer-period availability).
- Enforce timing constraints (fixed, no earlier/later windows) consistently.
- Implement cross-program timing clash checks for selected major/minor combinations.
- Ensure summer periods are first-class in all timing calculations and messages.

### Acceptance criteria
- Timing violations are surfaced with precise conflict details.
- Summer-period constraints alter feasibility outcomes when relevant.
- Cross-program clashes are identified only where policy requires (core vs selected programs; cross-program scope configurable).

### Artifact outputs
- Timing policy matrix document
- Validation examples for semester + summer cases

## Sprint 6: Non-Academic Requirements + Scenario Operations

### Objectives
- Complete near-term decision workflow for leadership working groups.

### Work
- Add non-academic requirement structures and validations (PE, military training, leadership/airmanship) as editable rules/nodes.
- Standardize scenario operations:
  - save full scenario state
  - compare scenario A/B
  - export archival package with IDs/dependency checks
- Improve operator UX performance for large trees and feasibility panels.

### Acceptance criteria
- Non-academic checks appear in both feasibility contexts where applicable.
- A/B scenario comparison can be executed end-to-end with reproducible outputs.
- Design Studio remains responsive for typical working-group sessions.

### Artifact outputs
- Scenario operations runbook in `README.md` or `docs/`
- Final 80% readiness checklist

## 5) Backlog Ordering (Top Priority Next)

1. COI gap majors and placeholder reduction (Sprint 1)
2. Option-slot/basket first-class semantics (Sprint 2)
3. Program design consistency rules hardening (Sprint 3)
4. Resource/instructor feasibility MVP (Sprint 4)
5. Timing/offering + summer feasibility hardening (Sprint 5)
6. Non-academic requirements + scenario operations (Sprint 6)

## 6) Explicit Deferrals (post-80%)

- Advisor-mode cadet-performance execution (keep documented under `Cadet Performance` category, non-executing in Design Studio).
- Advanced analytics/optimization (Monte Carlo and predictive risk).
- External integration hardening and enterprise deployment concerns.

## 7) Risks and Mitigations

- Risk: COI ambiguity creates repeated rework.
  - Mitigation: encode assumptions as explicit definitional rules and keep them editable.
- Risk: placeholder logic leaks into production decisions.
  - Mitigation: report placeholders as first-class warnings until resolved.
- Risk: performance degradation with larger requirement trees.
  - Mitigation: preserve tree state, incremental recompute, and bounded rendering.

## 8) Definition of Done for 80% Milestone

- Core + major + minor rules are sufficiently complete to evaluate realistic alternatives.
- Feasibility outputs are stable, explainable, and reproducible across save/load cycles.
- Resource/instructor ilities are represented by concrete pass/fail checks with useful diagnostics.
- Leadership can run and compare multiple curriculum scenarios without engineering intervention.
