# USAFA Curriculum Management Tool (V1)

Local Phase 1 + Phase 2 scaffold for curriculum design workflows, including COI import and Design Studio tooling.

## One-Time Setup + Local Run (PowerShell)

Use two terminals.

### Terminal 1: Backend (FastAPI)

```powershell
cd "c:\Users\infin\OneDrive\Documents\USAFA\Curriculum Tool\V1\backend"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

### Terminal 2: Frontend (Vite + React)

```powershell
cd "c:\Users\infin\OneDrive\Documents\USAFA\Curriculum Tool\V1\frontend"
npm install
npm run dev
```

## URLs

- Backend health: `http://127.0.0.1:8000/health`
- Backend OpenAPI docs: `http://127.0.0.1:8000/docs`
- Frontend app: `http://127.0.0.1:5173`

## Default Login

- `design_admin / design_admin` (DESIGN role)
- `advisor_user / advisor_user` (ADVISOR role)

## Demo Data Behavior

- Startup now seeds an idempotent demo baseline automatically (versions, courses, programs, requirements, canvas, prereqs, substitutions, sections, cadets, transition data).
- Browse page includes a `Load Demo Data` button that calls `POST /demo/load-data` and refreshes UI queries.
- The endpoint only adds missing records; running it repeatedly is safe.

## QC Checklist (Phase 2, End-to-End)

1. Login
- Go to `/login`, sign in as `design_admin`.
- Expected: session token appears and all pages load.

2. Ensure visible data
- Go to `/browse`, click `Load Demo Data`.
- Expected: success summary appears; Versions/Courses/Programs/Requirements/Instructors/Classrooms/Sections/Cadets lists show non-empty rows.

3. COI import quality pipeline
- Go to `/import`.
- Upload `coi_extracted.txt` in COI Baseline Loader.
- Click `Analyze COI`.
- Expected: analyze JSON shows detected/threshold counts.
- Click `Start Review Session`.
- Expected: review table appears with include toggles and editable titles.
- Toggle one row include or edit a title, click `Save Decisions`.
- Expected: counts refresh.
- Click `Commit Reviewed Baseline`.
- Expected: commit result JSON includes inserted/updated/skipped counts.

4. Canvas + view modes
- Go to `/design`.
- Expected: 8 semester columns render with courses.
- Change `View Mode` among Generic Core, Major-Specific, Wing Aggregate, Cohort Transition, Comparison.
- Expected: view payload/summary updates without empty-page errors.
- Move a plan item to another semester.
- Expected: canvas reloads and impact refreshes.

5. Impact + validation + dashboard
- In `/design`, review impact and detailed impact cards.
- Expected: impacted programs/findings populate.
- Review validation findings and validation dashboard totals.
- Expected: severity/tier counts and finding rows are visible.

6. Validation rule lifecycle
- Create a rule in the rule editor.
- Expected: new rule appears in list.
- Toggle or edit it.
- Expected: row updates and validation refreshes.
- Delete it.
- Expected: row removed.

7. Course detail tabs + prerequisites/substitutions
- Select a course in Design Studio.
- Expected: tabbed detail (general/scheduling/prerequisites/requirements/resources/history) renders with data.
- Add a prerequisite link.
- Expected: prerequisite graph/detail refreshes.
- Add a substitution.
- Expected: substitutions list refreshes.

8. Requirement tree + linkage
- Reorder requirements (up/down) and drag/drop in tree.
- Expected: tree persists with updated order/parent.
- Link selected course to a requirement.
- Expected: fulfillment data appears for the course.

9. Cadet gap analysis
- Select a cadet and run gap analysis panel.
- Expected: missing requirement/course list is returned.

10. Collaboration workflow
- Add a comment.
- Expected: comment appears in comments list.
- Create a change request, then review/approve or reject.
- Expected: status updates and appears in change request list.

11. Transition planner
- Assign a cohort class year to active version.
- Expected: cohort list updates.
- Add an equivalency mapping between versions.
- Expected: equivalency list updates.
- Run transition impact.
- Expected: unmapped/impact results populate.

12. Versioning + audit
- Compare two versions in diff panel.
- Expected: added/removed/changed details appear.
- Check audit panel.
- Expected: recent actions (seed, import, edits) are listed.

