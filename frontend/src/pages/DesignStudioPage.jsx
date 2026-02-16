import { useEffect, useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AutoComplete, Button, Card, Col, Input, List, Modal, Row, Select, Space, Switch, Table, Tabs, Tag, Tooltip, Tree, Typography } from "antd";

const API = "http://127.0.0.1:8000";

async function authed(path, opts = {}) {
  const token = localStorage.getItem("session_token") || "";
  const withToken = `${path}${path.includes("?") ? "&" : "?"}session_token=${encodeURIComponent(token)}`;
  const r = await fetch(`${API}${withToken}`, opts);
  if (!r.ok) throw new Error("Request failed");
  return r.json();
}

export function DesignStudioPage() {
  const qc = useQueryClient();
  const [commentText, setCommentText] = useState("");
  const [changeTitle, setChangeTitle] = useState("");
  const [selectedVersionId, setSelectedVersionId] = useState();
  const [verboseCanvas, setVerboseCanvas] = useState(false);
  const [addModalOpen, setAddModalOpen] = useState(false);
  const [addSemester, setAddSemester] = useState();
  const [addCourseId, setAddCourseId] = useState();
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [editPlanItemId, setEditPlanItemId] = useState();
  const [editSemester, setEditSemester] = useState();
  const [editCourseId, setEditCourseId] = useState();
  const [editOriginalCourseId, setEditOriginalCourseId] = useState();
  const [checklistProgramIds, setChecklistProgramIds] = useState([]);
  const [rulesetName, setRulesetName] = useState("");
  const [rulesetLogic, setRulesetLogic] = useState("ALL_REQUIRED");
  const [rulesetPickN, setRulesetPickN] = useState("");
  const [rulesetParentId, setRulesetParentId] = useState();
  const [rulesetProgramId, setRulesetProgramId] = useState();
  const [rulesetCategory, setRulesetCategory] = useState("CORE");
  const [rulesetMajorMode, setRulesetMajorMode] = useState("REQUIREMENT");
  const [rulesetTrackName, setRulesetTrackName] = useState("");
  const [rulesetMajorName, setRulesetMajorName] = useState("");
  const [rulesetCoreTrack, setRulesetCoreTrack] = useState("");
  const [rulesetFilter, setRulesetFilter] = useState("ALL");
  const [canvasFilter, setCanvasFilter] = useState("ALL");
  const [selectedRuleNodeId, setSelectedRuleNodeId] = useState();
  const [editReqName, setEditReqName] = useState("");
  const [editReqLogic, setEditReqLogic] = useState("ALL_REQUIRED");
  const [editReqPickN, setEditReqPickN] = useState("");
  const [editReqProgramId, setEditReqProgramId] = useState();
  const [editReqParentId, setEditReqParentId] = useState();
  const [editReqCategory, setEditReqCategory] = useState("CORE");
  const [editReqMajorMode, setEditReqMajorMode] = useState("REQUIREMENT");
  const [editReqTrackName, setEditReqTrackName] = useState("");
  const [editReqMajorName, setEditReqMajorName] = useState("");
  const [editReqDivision, setEditReqDivision] = useState();
  const [editReqCoreTrack, setEditReqCoreTrack] = useState("");
  const [reqScopeLocked, setReqScopeLocked] = useState(false);
  const [reqLockedCategory, setReqLockedCategory] = useState();
  const [reqCourseModalOpen, setReqCourseModalOpen] = useState(false);
  const [reqCourseRequirementId, setReqCourseRequirementId] = useState();
  const [reqCourseId, setReqCourseId] = useState();
  const [reqCourseTimingRules, setReqCourseTimingRules] = useState([]);
  const [reqSubDraftRows, setReqSubDraftRows] = useState([]);
  const [treeExpandedKeys, setTreeExpandedKeys] = useState([]);
  const [reqEditorOpen, setReqEditorOpen] = useState(false);
  const [coreRulesModalOpen, setCoreRulesModalOpen] = useState(false);
  const [coreRulesProgramId, setCoreRulesProgramId] = useState();
  const [coreRulesProgramName, setCoreRulesProgramName] = useState("");
  const [coreRulesRows, setCoreRulesRows] = useState([]);
  const [compareFrom, setCompareFrom] = useState();
  const [compareTo, setCompareTo] = useState();
  const [selectedCadetId, setSelectedCadetId] = useState();
  const [selectedCourseId, setSelectedCourseId] = useState();
  const [newPrereqId, setNewPrereqId] = useState();
  const [newPrereqType, setNewPrereqType] = useState("PREREQUISITE");
  const [newPrereqEnforcement, setNewPrereqEnforcement] = useState("HARD");
  const [substituteCourseId, setSubstituteCourseId] = useState();
  const [subBidirectional, setSubBidirectional] = useState(false);
  const [subRequiresApproval, setSubRequiresApproval] = useState(false);
  const [linkRequirementId, setLinkRequirementId] = useState();
  const [transitionClassYear, setTransitionClassYear] = useState("2030");
  const [transitionFromVersion, setTransitionFromVersion] = useState();
  const [transitionToVersion, setTransitionToVersion] = useState();
  const [transitionFromCourse, setTransitionFromCourse] = useState();
  const [transitionToCourse, setTransitionToCourse] = useState();
  const [newRuleName, setNewRuleName] = useState("");
  const [newRuleTier, setNewRuleTier] = useState(1);
  const [newRuleSeverity, setNewRuleSeverity] = useState("WARNING");
  const [newRuleActive, setNewRuleActive] = useState("YES");
  const [newRuleConfig, setNewRuleConfig] = useState("{}");
  const [editRuleId, setEditRuleId] = useState();
  const [editRuleTier, setEditRuleTier] = useState(1);
  const [editRuleSeverity, setEditRuleSeverity] = useState("WARNING");
  const [editRuleActive, setEditRuleActive] = useState("YES");
  const [editRuleConfig, setEditRuleConfig] = useState("{}");
  const [validationDomainFilter, setValidationDomainFilter] = useState("ALL");
  const semesterOptions = [1, 2, 3, 4, 5, 6, 7, 8].map((s) => ({ value: s, label: `Semester ${s}` }));
  const timingTypeOptions = [
    { value: "FIXED", label: "Fixed semester" },
    { value: "MIN", label: "No earlier than" },
    { value: "MAX", label: "No later than" },
  ];
  function timingRulesFromFields(requiredSemester, requiredSemesterMin, requiredSemesterMax) {
    if (requiredSemester != null) return [{ type: "FIXED", semester: requiredSemester }];
    if (requiredSemesterMin != null && requiredSemesterMax != null && Number(requiredSemesterMin) === Number(requiredSemesterMax)) {
      return [{ type: "FIXED", semester: Number(requiredSemesterMin) }];
    }
    const out = [];
    if (requiredSemesterMin != null) out.push({ type: "MIN", semester: requiredSemesterMin });
    if (requiredSemesterMax != null) out.push({ type: "MAX", semester: requiredSemesterMax });
    return out;
  }
  function timingFieldsFromRules(rules) {
    const out = { required_semester: null, required_semester_min: null, required_semester_max: null };
    const rows = (rules || []).filter((r) => r?.type);
    const fixed = rows.find((r) => r.type === "FIXED" && r.semester != null);
    if (fixed) {
      out.required_semester = Number(fixed.semester);
      return out;
    }
    const minRow = rows.find((r) => r.type === "MIN" && r.semester != null);
    const maxRow = rows.find((r) => r.type === "MAX" && r.semester != null);
    if (minRow) out.required_semester_min = Number(minRow.semester);
    if (maxRow) out.required_semester_max = Number(maxRow.semester);
    if (
      out.required_semester_min != null
      && out.required_semester_max != null
      && Number(out.required_semester_min) === Number(out.required_semester_max)
    ) {
      out.required_semester = Number(out.required_semester_min);
      out.required_semester_min = null;
      out.required_semester_max = null;
    }
    return out;
  }
  function normalizeCourseNumber(value) {
    return String(value || "").toUpperCase().replace(/\s+/g, " ").trim();
  }
  const versionsQ = useQuery({ queryKey: ["versions"], queryFn: () => authed("/versions") });
  const selectedVersion = useMemo(() => {
    const versions = versionsQ.data || [];
    if (!versions.length) return null;
    return versions.find((v) => v.id === selectedVersionId) || versions.find((v) => v.status === "ACTIVE") || versions[0];
  }, [versionsQ.data, selectedVersionId]);

  useEffect(() => {
    const versions = versionsQ.data || [];
    if (!versions.length) return;
    if (!selectedVersionId || !versions.some((v) => v.id === selectedVersionId)) {
      const fallback = versions.find((v) => v.status === "ACTIVE") || versions[0];
      setSelectedVersionId(fallback?.id);
    }
  }, [versionsQ.data, selectedVersionId]);

  const canvasQ = useQuery({
    queryKey: ["canvas", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/canvas/${selectedVersion.id}`)
  });
  const programsQ = useQuery({
    queryKey: ["programs", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/programs?version_id=${selectedVersion.id}`)
  });
  const validationQ = useQuery({
    queryKey: ["validation", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/validation/${selectedVersion.id}`)
  });
  const validationDashboardQ = useQuery({
    queryKey: ["validation-dashboard", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/validation-dashboard/${selectedVersion.id}`)
  });
  const validationRulesQ = useQuery({
    queryKey: ["validation-rules"],
    queryFn: () => authed("/design/validation-rules")
  });
  const requirementsTreeQ = useQuery({
    queryKey: ["requirements-tree", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/requirements/tree/${selectedVersion.id}`)
  });
  const commentsQ = useQuery({
    queryKey: ["comments", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/comments/${selectedVersion.id}`)
  });
  const changesQ = useQuery({
    queryKey: ["change-requests", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/change-requests/${selectedVersion.id}`)
  });
  const prereqGraphQ = useQuery({
    queryKey: ["prereq-graph", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/prerequisite-graph/${selectedVersion.id}`)
  });
  const versionDiffQ = useQuery({
    queryKey: ["version-diff", compareFrom, compareTo],
    enabled: !!compareFrom && !!compareTo,
    queryFn: () => authed(`/design/versioning/diff/${compareFrom}/${compareTo}`)
  });
  const coursesQ = useQuery({
    queryKey: ["courses", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/courses?version_id=${selectedVersion.id}&limit=500`)
  });
  const cadetsQ = useQuery({
    queryKey: ["cadets"],
    queryFn: () => authed("/cadets")
  });
  const gapQ = useQuery({
    queryKey: ["gap-analysis", selectedCadetId],
    enabled: !!selectedCadetId,
    queryFn: () => authed(`/design/requirements/gap-analysis/${selectedCadetId}`)
  });
  const courseDetailQ = useQuery({
    queryKey: ["course-detail", selectedCourseId],
    enabled: !!selectedCourseId,
    queryFn: () => authed(`/design/course-detail/${selectedCourseId}`)
  });
  const courseFulfillmentQ = useQuery({
    queryKey: ["course-fulfillment", selectedCourseId],
    enabled: !!selectedCourseId,
    queryFn: () => authed(`/requirements/fulfillment/course/${selectedCourseId}`)
  });
  const substitutionsQ = useQuery({
    queryKey: ["substitutions", selectedCourseId],
    enabled: !!selectedCourseId,
    queryFn: () => authed(`/substitutions/${selectedCourseId}`)
  });
  const auditQ = useQuery({
    queryKey: ["audit"],
    queryFn: () => authed("/audit?limit=500")
  });
  const cohortQ = useQuery({
    queryKey: ["cohorts"],
    queryFn: () => authed("/design/transition/cohort")
  });
  const equivalencyQ = useQuery({
    queryKey: ["equivalencies", transitionFromVersion, transitionToVersion],
    queryFn: () =>
      authed(
        `/design/transition/equivalency?from_version_id=${encodeURIComponent(transitionFromVersion || "")}&to_version_id=${encodeURIComponent(
          transitionToVersion || ""
        )}`
      )
  });
  const transitionImpactQ = useQuery({
    queryKey: ["transition-impact", transitionFromVersion, transitionToVersion],
    enabled: !!transitionFromVersion && !!transitionToVersion,
    queryFn: () =>
      authed(
        `/design/transition/impact?from_version_id=${encodeURIComponent(transitionFromVersion)}&to_version_id=${encodeURIComponent(transitionToVersion)}`
      )
  });
  const checklistQ = useQuery({
    queryKey: ["design-checklist", selectedVersion?.id, checklistProgramIds],
    enabled: !!selectedVersion?.id,
    queryFn: () =>
      authed(
        `/design/checklist/${selectedVersion.id}?program_ids=${encodeURIComponent((checklistProgramIds || []).join(","))}&include_core=true`
      )
  });
  const feasibilityQ = useQuery({
    queryKey: ["design-feasibility", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/feasibility/${selectedVersion.id}`)
  });
  const requirementsListQ = useQuery({
    queryKey: ["requirements-list", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/requirements?version_id=${selectedVersion.id}`),
  });
  const requirementFulfillmentVersionQ = useQuery({
    queryKey: ["requirement-fulfillment-version", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/requirements/fulfillment/version/${selectedVersion.id}`),
  });
  const requirementSubstitutionsQ = useQuery({
    queryKey: ["requirement-substitutions", reqCourseRequirementId],
    enabled: !!reqCourseRequirementId,
    queryFn: () => authed(`/requirements/substitutions/${reqCourseRequirementId}`),
  });
  const requirementSubstitutionsVersionQ = useQuery({
    queryKey: ["requirement-substitutions-version", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/requirements/substitutions/version/${selectedVersion.id}`),
  });

  async function move(planItemId, targetSemester, targetPosition = 0) {
    await authed("/design/canvas/move", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ plan_item_id: planItemId, target_semester: targetSemester, target_position: targetPosition })
    });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["canvas"] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
    ]);
  }

  async function addToCanvas(courseId, semesterIndex, config = {}) {
    if (!selectedVersion?.id || !courseId) return;
    const params = new URLSearchParams({
      version_id: selectedVersion.id,
      course_id: courseId,
      semester_index: String(semesterIndex),
    });
    if (config.category) params.set("category", config.category);
    if (config.major_mode) params.set("major_mode", config.major_mode);
    if (config.major_program_id) params.set("major_program_id", config.major_program_id);
    if (config.track_name) params.set("track_name", config.track_name);
    await authed(
      `/design/canvas/add?${params.toString()}`,
      { method: "POST" }
    );
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["canvas", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
    ]);
  }

  async function removeFromCanvas(planItemId) {
    await authed(`/design/canvas/${encodeURIComponent(planItemId)}`, { method: "DELETE" });
    if (!selectedVersion?.id) return;
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["canvas", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
    ]);
  }

  async function updateCanvasItem() {
    if (!editPlanItemId || !editSemester || !editCourseId) return;
    if (editCourseId !== editOriginalCourseId) {
      await removeFromCanvas(editPlanItemId);
      await addToCanvas(editCourseId, editSemester, {});
    }
    setEditModalOpen(false);
  }

  function openEditCourseModal(course, semester) {
    setEditPlanItemId(course.plan_item_id);
    setEditSemester(semester);
    setEditCourseId(course.course_id);
    setEditOriginalCourseId(course.course_id);
    setEditModalOpen(true);
  }

  async function createRulesetRequirement() {
    if (!selectedVersion?.id) return;
    if (rulesetCategory === "MAJOR" && !(rulesetMajorName || "").trim()) return;
    async function resolveProgramId(category, programName, division) {
      if (!["MAJOR", "MINOR"].includes(category)) return null;
      const name = (programName || "").trim();
      if (!name) return null;
      const existing = (programsQ.data || []).find(
        (p) => p.name.toLowerCase() === name.toLowerCase() && p.program_type === category
      );
      if (existing) {
        if ((division || null) !== (existing.division || null)) {
          await authed(`/programs/${existing.id}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              version_id: existing.version_id,
              name: existing.name,
              program_type: existing.program_type,
              division: division || null,
            }),
          });
          await qc.invalidateQueries({ queryKey: ["programs", selectedVersion.id] });
        }
        return existing.id;
      }
      const created = await authed("/programs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ version_id: selectedVersion.id, name, program_type: category, division: division || null }),
      });
      await qc.invalidateQueries({ queryKey: ["programs", selectedVersion.id] });
      return created.id;
    }
    function logicLabel(logic, pickN) {
      if (logic === "PICK_N") return `Pick ${pickN || 1}`;
      if (logic === "ANY_ONE") return "Any One";
      return "All Required";
    }
    function autoRequirementName() {
      const logicTxt = logicLabel(rulesetLogic, Number(rulesetPickN || 1));
      if (rulesetCategory === "CORE") return `Core${rulesetCoreTrack ? ` - ${rulesetCoreTrack.trim()}` : ""} - ${logicTxt}`;
      if (rulesetCategory === "PE") return `PE - ${logicTxt}`;
      if (rulesetCategory === "MINOR") {
        return `Minor - ${(rulesetMajorName || "Unspecified").trim()}${rulesetTrackName ? ` - ${rulesetTrackName.trim()}` : ""} - ${logicTxt}`;
      }
      return `Major - ${(rulesetMajorName || "Unspecified").trim()}${rulesetTrackName ? ` - ${rulesetTrackName.trim()}` : ""} - ${logicTxt}`;
    }
    const resolvedProgramId = await resolveProgramId(rulesetCategory, rulesetMajorName);
    const payload = {
      version_id: selectedVersion.id,
      name: autoRequirementName(),
      program_id: resolvedProgramId,
      parent_requirement_id: rulesetParentId || null,
      logic_type: rulesetLogic,
      pick_n: rulesetLogic === "PICK_N" ? Number(rulesetPickN || 1) : null,
      sort_order: null,
      category: rulesetCategory,
      major_mode: rulesetCategory === "MAJOR" ? rulesetMajorMode : null,
      track_name:
        rulesetCategory === "CORE"
          ? (rulesetCoreTrack || "").trim() || null
          : rulesetCategory === "MAJOR" && rulesetMajorMode === "TRACK"
            ? (rulesetTrackName || "").trim() || null
            : null,
    };
    await authed("/requirements", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setRulesetPickN("");
    setRulesetTrackName("");
    setRulesetMajorName("");
    setRulesetCoreTrack("");
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-list", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
    ]);
  }

  function resetRequirementEditor() {
    setSelectedRuleNodeId(undefined);
    setEditReqName("");
    setEditReqLogic("ALL_REQUIRED");
    setEditReqPickN("");
    setEditReqProgramId(undefined);
    setEditReqParentId(undefined);
    setEditReqCategory("CORE");
    setEditReqMajorMode("REQUIREMENT");
    setEditReqTrackName("");
    setEditReqMajorName("");
    setEditReqDivision(undefined);
    setEditReqCoreTrack("");
  }

  function openNewRequirementEditor() {
    resetRequirementEditor();
    setReqScopeLocked(false);
    setReqLockedCategory(undefined);
    setReqEditorOpen(true);
  }

  function openRequirementEditor(requirementId) {
    loadRequirementNodeToEditor(requirementId);
    setReqScopeLocked(false);
    setReqLockedCategory(undefined);
    setReqEditorOpen(true);
  }

  function openSubNodeEditor(parentRequirementId) {
    resetRequirementEditor();
    const parent = requirementNodeMap[parentRequirementId];
    if (parent && parent.parent_requirement_id) {
      window.alert("Subnodes are only allowed under top-level Core/Major/Minor/PE nodes.");
      return;
    }
    if (parent) {
      const parentCategory = String(parent.category || "CORE").toUpperCase();
      setReqLockedCategory(parentCategory);
      setEditReqCategory(parentCategory);
      if (parentCategory === "MAJOR" || parentCategory === "MINOR") {
        setEditReqMajorMode("REQUIREMENT");
        setEditReqTrackName("");
        const majorName =
          parent.program_name || (programsQ.data || []).find((p) => p.id === parent.program_id)?.name || "";
        setEditReqMajorName(majorName);
        const division =
          (programsQ.data || []).find((p) => p.id === parent.program_id)?.division || parent.program_division || undefined;
        setEditReqDivision(division);
      } else if (parentCategory === "CORE") {
        setEditReqMajorMode("REQUIREMENT");
        setEditReqCoreTrack("");
      }
    }
    setEditReqParentId(parentRequirementId);
    setReqScopeLocked(true);
    setReqEditorOpen(true);
  }

  function openRequirementCourseModal(requirementId, opts = {}) {
    setReqCourseRequirementId(requirementId);
    setReqCourseId(opts.primaryCourseId || undefined);
    setReqCourseTimingRules(
      timingRulesFromFields(opts.requiredSemester || null, opts.requiredSemesterMin || null, opts.requiredSemesterMax || null)
    );
    setReqSubDraftRows([]);
    setReqCourseModalOpen(true);
  }

  async function saveRequirementCourseModal() {
    if (!reqCourseRequirementId || !reqCourseId) return;
    if (reqCourseTimingRules.some((r) => r.type === "FIXED") && reqCourseTimingRules.length > 1) {
      window.alert("Fixed semester cannot be combined with no earlier/no later rules.");
      return;
    }
    const timing = timingFieldsFromRules(reqCourseTimingRules);
    if (
      timing.required_semester_min != null
      && timing.required_semester_max != null
      && Number(timing.required_semester_min) > Number(timing.required_semester_max)
    ) {
      window.alert("No earlier than semester cannot be later than no later than semester.");
      return;
    }
    const alreadyLinked = (requirementLinkedCoursesForModal || []).some((x) => x.course_id === reqCourseId);
    if (!alreadyLinked) {
      await authed("/requirements/fulfillment", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requirement_id: reqCourseRequirementId,
          course_id: reqCourseId,
          is_primary: true,
          required_semester: timing.required_semester,
          required_semester_min: timing.required_semester_min,
          required_semester_max: timing.required_semester_max,
        }),
      });
    } else {
      await authed("/requirements/fulfillment", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requirement_id: reqCourseRequirementId,
          course_id: reqCourseId,
          is_primary: true,
          required_semester: timing.required_semester,
          required_semester_min: timing.required_semester_min,
          required_semester_max: timing.required_semester_max,
        }),
      });
    }
    const existingPairs = new Set(
      (requirementSubstitutionsQ.data || [])
        .filter((s) => s.primary_course_id === reqCourseId)
        .map((s) => `${s.primary_course_id}|${s.substitute_course_id}`)
    );
    const draftIds = reqSubDraftRows.map((r) => r.course_id).filter(Boolean);
    const uniqueDraftIds = Array.from(new Set(draftIds)).filter((id) => id !== reqCourseId);
    for (const subCourseId of uniqueDraftIds) {
      const key = `${reqCourseId}|${subCourseId}`;
      if (existingPairs.has(key)) continue;
      await authed("/requirements/substitutions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requirement_id: reqCourseRequirementId,
          primary_course_id: reqCourseId,
          substitute_course_id: subCourseId,
          is_bidirectional: true,
        }),
      });
    }
    setReqSubDraftRows([]);
    setReqCourseTimingRules([]);
    setReqCourseModalOpen(false);
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["requirement-fulfillment-version", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["requirement-substitutions", reqCourseRequirementId] }),
      qc.invalidateQueries({ queryKey: ["requirement-substitutions-version", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
    ]);
  }

  async function updateRulesetRequirement() {
    if (!selectedRuleNodeId || !selectedVersion?.id) return;
    if (["MAJOR", "MINOR"].includes(editReqCategory) && !(editReqMajorName || "").trim()) return;
    async function resolveProgramId(category, programName, division) {
      if (!["MAJOR", "MINOR"].includes(category)) return null;
      const name = (programName || "").trim();
      if (!name) return null;
      const existing = (programsQ.data || []).find(
        (p) => p.name.toLowerCase() === name.toLowerCase() && p.program_type === category
      );
      if (existing) {
        if ((division || null) !== (existing.division || null)) {
          await authed(`/programs/${existing.id}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              version_id: existing.version_id,
              name: existing.name,
              program_type: existing.program_type,
              division: division || null,
            }),
          });
          await qc.invalidateQueries({ queryKey: ["programs", selectedVersion.id] });
        }
        return existing.id;
      }
      const created = await authed("/programs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ version_id: selectedVersion.id, name, program_type: category, division: division || null }),
      });
      await qc.invalidateQueries({ queryKey: ["programs", selectedVersion.id] });
      return created.id;
    }
    const effectiveParentId = editReqParentId || requirementNodeMap[selectedRuleNodeId]?.parent_requirement_id || null;
    const isSubNode = !!effectiveParentId;
    function autoRequirementName() {
      function logicLabel(logic, pickN) {
        if (logic === "PICK_N") return `Pick ${pickN || 1}`;
        if (logic === "ANY_ONE") return "Any One";
        return "All Required";
      }
      const logicTxt = logicLabel(editReqLogic, Number(editReqPickN || 1));
      if (!isSubNode) {
        if (editReqCategory === "CORE") return "Core";
        if (editReqCategory === "PE") return "PE";
        if (editReqCategory === "MINOR") return `Minor - ${(editReqMajorName || "Unspecified").trim()}`;
        return `Major - ${(editReqMajorName || "Unspecified").trim()}`;
      }
      if (editReqCategory === "MAJOR") {
        if (editReqMajorMode === "TRACK") return `Track - ${(editReqTrackName || "").trim()}: ${logicTxt}`;
        return `Major Requirement: ${logicTxt}`;
      }
      if (editReqCategory === "MINOR") {
        if (editReqMajorMode === "TRACK") return `Track - ${(editReqTrackName || "").trim()}: ${logicTxt}`;
        return `Minor Requirement: ${logicTxt}`;
      }
      if (editReqCategory === "CORE") {
        if (editReqMajorMode === "TRACK") return `Track - ${(editReqCoreTrack || "").trim()}: ${logicTxt}`;
        return `Core Requirement: ${logicTxt}`;
      }
      return `PE Requirement: ${logicTxt}`;
    }
    const resolvedProgramId = await resolveProgramId(editReqCategory, editReqMajorName, editReqDivision);
    const payload = {
      version_id: selectedVersion.id,
      name: autoRequirementName(),
      program_id: resolvedProgramId,
      parent_requirement_id: effectiveParentId,
      logic_type: editReqLogic,
      pick_n: editReqLogic === "PICK_N" ? Number(editReqPickN || 1) : null,
      sort_order: requirementNodeMap[selectedRuleNodeId]?.sort_order ?? 0,
      category: editReqCategory,
      major_mode:
        editReqCategory === "MAJOR" || editReqCategory === "MINOR" || (editReqCategory === "CORE" && isSubNode)
          ? editReqMajorMode
          : null,
      track_name:
        editReqCategory === "CORE"
          ? editReqMajorMode === "TRACK"
            ? (editReqCoreTrack || "").trim() || null
            : null
          : (editReqCategory === "MAJOR" || editReqCategory === "MINOR") && editReqMajorMode === "TRACK"
            ? (editReqTrackName || "").trim() || null
            : null,
    };
    await authed(`/requirements/${selectedRuleNodeId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
    ]);
  }

  async function saveRequirementNode() {
    const isSubNodeCreate = !selectedRuleNodeId && reqScopeLocked && !!editReqParentId;
    if (selectedRuleNodeId) {
      await updateRulesetRequirement();
    } else {
      // Mirror "create" path with editor values.
      if (!selectedVersion?.id) return;
      if (["MAJOR", "MINOR"].includes(editReqCategory) && !(editReqMajorName || "").trim()) return;
      if (
        isSubNodeCreate
        && ["MAJOR", "MINOR"].includes(editReqCategory)
        && editReqMajorMode === "TRACK"
        && !(editReqTrackName || "").trim()
      ) return;
      if (isSubNodeCreate && editReqCategory === "CORE" && editReqMajorMode === "TRACK" && !(editReqCoreTrack || "").trim()) return;
      async function resolveProgramId(category, programName, division) {
        if (!["MAJOR", "MINOR"].includes(category)) return null;
        const name = (programName || "").trim();
        if (!name) return null;
        const existing = (programsQ.data || []).find(
          (p) => p.name.toLowerCase() === name.toLowerCase() && p.program_type === category
        );
        if (existing) {
          if ((division || null) !== (existing.division || null)) {
            await authed(`/programs/${existing.id}`, {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                version_id: existing.version_id,
                name: existing.name,
                program_type: existing.program_type,
                division: division || null,
              }),
            });
            await qc.invalidateQueries({ queryKey: ["programs", selectedVersion.id] });
          }
          return existing.id;
        }
        const created = await authed("/programs", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ version_id: selectedVersion.id, name, program_type: category, division: division || null }),
        });
        await qc.invalidateQueries({ queryKey: ["programs", selectedVersion.id] });
        return created.id;
      }
      function autoRequirementName() {
        function logicLabel(logic, pickN) {
          if (logic === "PICK_N") return `Pick ${pickN || 1}`;
          if (logic === "ANY_ONE") return "Any One";
          return "All Required";
        }
        const logicTxt = logicLabel(editReqLogic, Number(editReqPickN || 1));
        if (!isSubNodeCreate) {
          if (editReqCategory === "CORE") return "Core";
          if (editReqCategory === "PE") return "PE";
          if (editReqCategory === "MINOR") return `Minor - ${editReqMajorName.trim()}`;
          return `Major - ${editReqMajorName.trim()}`;
        }
        if (editReqCategory === "MAJOR") {
          if (editReqMajorMode === "TRACK") return `Track - ${(editReqTrackName || "").trim()}: ${logicTxt}`;
          return `Major Requirement: ${logicTxt}`;
        }
        if (editReqCategory === "MINOR") {
          if (editReqMajorMode === "TRACK") return `Track - ${(editReqTrackName || "").trim()}: ${logicTxt}`;
          return `Minor Requirement: ${logicTxt}`;
        }
        if (editReqCategory === "CORE") {
          if (editReqMajorMode === "TRACK") return `Track - ${(editReqCoreTrack || "").trim()}: ${logicTxt}`;
          return `Core Requirement: ${logicTxt}`;
        }
        return `PE Requirement: ${logicTxt}`;
      }
      const resolvedProgramId = await resolveProgramId(editReqCategory, editReqMajorName, editReqDivision);
      const existingRequirements = Object.values(requirementNodeMap || {});
      const normalize = (v) => String(v || "").trim().toLowerCase();
      const hasDuplicate = existingRequirements.some((r) => {
        if (r.version_id !== selectedVersion.id) return false;
        const sameParent = (r.parent_requirement_id || null) === (editReqParentId || null);
        if (!sameParent) return false;
        const sameCategory = String(r.category || "CORE").toUpperCase() === editReqCategory;
        if (!sameCategory) return false;

        if (!isSubNodeCreate) {
          if (editReqCategory === "MAJOR" || editReqCategory === "MINOR") {
            if (resolvedProgramId && r.program_id === resolvedProgramId) return true;
            const rProgramName = (programsQ.data || []).find((p) => p.id === r.program_id)?.name || "";
            return normalize(rProgramName) === normalize(editReqMajorName);
          }
          return true;
        }

        if (editReqCategory === "MAJOR" || editReqCategory === "MINOR") {
          const mode = String(editReqMajorMode || "REQUIREMENT").toUpperCase();
          const rMode = String(r.major_mode || "REQUIREMENT").toUpperCase();
          if (mode !== rMode) return false;
          if (mode === "TRACK" && normalize(r.track_name) !== normalize(editReqTrackName)) return false;
          const rLogic = String(r.logic_type || "ALL_REQUIRED").toUpperCase();
          const cLogic = String(editReqLogic || "ALL_REQUIRED").toUpperCase();
          if (rLogic !== cLogic) return false;
          if (cLogic === "PICK_N") return Number(r.pick_n || 0) === Number(editReqPickN || 1);
          return true;
        }
        if (editReqCategory === "CORE") {
          const mode = String(editReqMajorMode || "REQUIREMENT").toUpperCase();
          const rMode = String(r.major_mode || (r.track_name ? "TRACK" : "REQUIREMENT")).toUpperCase();
          if (mode !== rMode) return false;
          if (mode === "TRACK" && normalize(r.track_name) !== normalize(editReqCoreTrack)) return false;
          const rLogic = String(r.logic_type || "ALL_REQUIRED").toUpperCase();
          const cLogic = String(editReqLogic || "ALL_REQUIRED").toUpperCase();
          if (rLogic !== cLogic) return false;
          if (cLogic === "PICK_N") return Number(r.pick_n || 0) === Number(editReqPickN || 1);
          return true;
        }
        const rLogic = String(r.logic_type || "ALL_REQUIRED").toUpperCase();
        const cLogic = String(editReqLogic || "ALL_REQUIRED").toUpperCase();
        if (rLogic !== cLogic) return false;
        if (cLogic === "PICK_N") return Number(r.pick_n || 0) === Number(editReqPickN || 1);
        return normalize(r.name) === normalize(autoRequirementName());
      });
      if (hasDuplicate) {
        window.alert("Duplicate node exists for this scope.");
        return;
      }
      const payload = {
        version_id: selectedVersion.id,
        name: autoRequirementName(),
        program_id: resolvedProgramId,
        parent_requirement_id: editReqParentId || null,
        logic_type: editReqLogic,
        pick_n: editReqLogic === "PICK_N" ? Number(editReqPickN || 1) : null,
        sort_order: null,
        category: editReqCategory,
        major_mode:
          editReqCategory === "MAJOR" || editReqCategory === "MINOR" || (editReqCategory === "CORE" && isSubNodeCreate)
            ? editReqMajorMode
            : null,
        track_name:
          editReqCategory === "CORE"
            ? editReqMajorMode === "TRACK"
              ? (editReqCoreTrack || "").trim() || null
              : null
            : (editReqCategory === "MAJOR" || editReqCategory === "MINOR") && editReqMajorMode === "TRACK"
              ? (editReqTrackName || "").trim() || null
              : null,
      };
      await authed("/requirements", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion.id] }),
        qc.invalidateQueries({ queryKey: ["requirements-list", selectedVersion.id] }),
        qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
      ]);
    }
    setReqEditorOpen(false);
  }

  async function deleteRulesetRequirement() {
    if (!selectedRuleNodeId || !selectedVersion?.id) return;
    await authed(`/requirements/${selectedRuleNodeId}`, { method: "DELETE" });
    setSelectedRuleNodeId(undefined);
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-list", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
    ]);
    setReqEditorOpen(false);
  }

  async function deleteRequirementNodeById(requirementId) {
    if (!requirementId || !selectedVersion?.id) return;
    await authed(`/requirements/${requirementId}`, { method: "DELETE" });
    if (selectedRuleNodeId === requirementId) {
      resetRequirementEditor();
      setReqEditorOpen(false);
    }
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-list", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
    ]);
  }

  async function removeCourseFromRequirement(fulfillmentId, requirementId) {
    await authed(`/requirements/fulfillment/${fulfillmentId}`, { method: "DELETE" });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["requirement-fulfillment", requirementId] }),
      qc.invalidateQueries({ queryKey: ["requirement-fulfillment-version", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
    ]);
  }

  async function updateRequirementSubstitutionCourse(substitutionId, newSubstituteCourseId) {
    const row = (requirementSubstitutionsQ.data || []).find((s) => s.id === substitutionId);
    if (!row || !newSubstituteCourseId) return;
    if (newSubstituteCourseId === row.primary_course_id) return;
    await authed(`/requirements/substitutions/${substitutionId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        requirement_id: row.requirement_id,
        primary_course_id: row.primary_course_id,
        substitute_course_id: newSubstituteCourseId,
        is_bidirectional: true,
      }),
    });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["requirement-substitutions", reqCourseRequirementId] }),
      qc.invalidateQueries({ queryKey: ["requirement-substitutions-version", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
    ]);
  }

  async function deleteRequirementSubstitution(substitutionId) {
    await authed(`/requirements/substitutions/${substitutionId}`, { method: "DELETE" });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["requirement-substitutions", reqCourseRequirementId] }),
      qc.invalidateQueries({ queryKey: ["requirement-substitutions-version", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
    ]);
  }

  function openAddCourseModal(semester) {
    setAddSemester(semester);
    setAddCourseId(undefined);
    setAddModalOpen(true);
  }

  function closeAddCourseModal() {
    setAddModalOpen(false);
  }

  async function submitAddCourse() {
    if (!addSemester || !addCourseId) return;
    await addToCanvas(addCourseId, addSemester, {});
    closeAddCourseModal();
  }

  async function dropToSemester(semester, e) {
    e.preventDefault();
    const planItemId = e.dataTransfer.getData("text/plain");
    if (!planItemId) return;
    const semesterItems = (canvasQ.data?.[String(semester)] || []).map((c) => c.plan_item_id);
    const sourceInSame = semesterItems.includes(planItemId);
    const targetPosition = sourceInSame ? Math.max(0, semesterItems.length - 1) : semesterItems.length;
    await move(planItemId, semester, targetPosition);
  }

  async function dropToCourse(semester, targetPlanItemId, e) {
    e.preventDefault();
    e.stopPropagation();
    const planItemId = e.dataTransfer.getData("text/plain");
    if (!planItemId) return;
    const semesterItems = (canvasQ.data?.[String(semester)] || []).map((c) => c.plan_item_id);
    const sameSemester = semesterItems.includes(planItemId);
    const reordered = sameSemester ? semesterItems.filter((id) => id !== planItemId) : semesterItems;
    const idx = reordered.findIndex((id) => id === targetPlanItemId);
    const targetPosition = idx >= 0 ? idx : reordered.length;
    await move(planItemId, semester, targetPosition);
  }

  async function submitComment() {
    if (!commentText.trim() || !selectedVersion?.id) return;
    await authed("/design/comments", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        version_id: selectedVersion.id,
        entity_type: "CurriculumVersion",
        entity_id: selectedVersion.id,
        comment: commentText
      })
    });
    setCommentText("");
    await qc.invalidateQueries({ queryKey: ["comments"] });
  }

  async function submitChange() {
    if (!changeTitle.trim() || !selectedVersion?.id) return;
    await authed("/design/change-requests", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        version_id: selectedVersion.id,
        title: changeTitle
      })
    });
    setChangeTitle("");
    await qc.invalidateQueries({ queryKey: ["change-requests"] });
  }

  async function setVersionStatus(status) {
    if (!selectedVersion?.id) return;
    await authed(`/versions/${selectedVersion.id}/status?status=${encodeURIComponent(status)}`, { method: "POST" });
    await qc.invalidateQueries({ queryKey: ["versions"] });
  }

  async function addPrerequisite() {
    if (!selectedCourseId || !newPrereqId) return;
    await authed("/prerequisites", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        course_id: selectedCourseId,
        required_course_id: newPrereqId,
        relationship_type: newPrereqType,
        enforcement: newPrereqEnforcement
      })
    });
    setNewPrereqId(undefined);
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["course-detail", selectedCourseId] }),
      qc.invalidateQueries({ queryKey: ["prereq-graph"] }),
      qc.invalidateQueries({ queryKey: ["validation"] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
    ]);
  }

  async function addSubstitution() {
    if (!selectedCourseId || !substituteCourseId) return;
    await authed("/substitutions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        original_course_id: selectedCourseId,
        substitute_course_id: substituteCourseId,
        is_bidirectional: subBidirectional,
        requires_approval: subRequiresApproval,
        conditions: {}
      })
    });
    setSubstituteCourseId(undefined);
    await qc.invalidateQueries({ queryKey: ["substitutions", selectedCourseId] });
  }

  async function linkRequirementToCourse() {
    if (!selectedCourseId || !linkRequirementId) return;
    await authed("/requirements/fulfillment", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ requirement_id: linkRequirementId, course_id: selectedCourseId, is_primary: true })
    });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["course-detail", selectedCourseId] }),
      qc.invalidateQueries({ queryKey: ["course-fulfillment", selectedCourseId] }),
    ]);
  }

  async function assignCohort() {
    if (!selectedVersion?.id) return;
    await authed(`/design/transition/cohort?class_year=${encodeURIComponent(transitionClassYear)}&version_id=${encodeURIComponent(selectedVersion.id)}`, {
      method: "POST"
    });
    await qc.invalidateQueries({ queryKey: ["cohorts"] });
  }

  async function addEquivalency() {
    if (!transitionFromVersion || !transitionToVersion || !transitionFromCourse || !transitionToCourse) return;
    await authed(
      `/design/transition/equivalency?from_version_id=${encodeURIComponent(transitionFromVersion)}&to_version_id=${encodeURIComponent(
        transitionToVersion
      )}&from_course_id=${encodeURIComponent(transitionFromCourse)}&to_course_id=${encodeURIComponent(transitionToCourse)}`,
      { method: "POST" }
    );
    await Promise.all([qc.invalidateQueries({ queryKey: ["equivalencies"] }), qc.invalidateQueries({ queryKey: ["transition-impact"] })]);
  }

  async function createValidationRule() {
    if (!newRuleName.trim()) return;
    let cfg = {};
    try {
      cfg = JSON.parse(newRuleConfig || "{}");
    } catch {
      return;
    }
    await authed("/design/validation-rules", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: newRuleName,
        tier: Number(newRuleTier),
        severity: newRuleSeverity,
        active: newRuleActive === "YES",
        config: cfg
      })
    });
    setNewRuleName("");
    setNewRuleConfig("{}");
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["validation-rules"] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
    ]);
  }

  async function loadRuleForEdit(ruleId) {
    setEditRuleId(ruleId);
    const r = (validationRulesQ.data || []).find((x) => x.id === ruleId);
    if (!r) return;
    setEditRuleTier(r.tier || 1);
    setEditRuleSeverity(r.severity || "WARNING");
    setEditRuleActive(r.active ? "YES" : "NO");
    setEditRuleConfig(r.config_json || "{}");
  }

  async function updateValidationRule() {
    if (!editRuleId) return;
    let cfg = {};
    try {
      cfg = JSON.parse(editRuleConfig || "{}");
    } catch {
      return;
    }
    await authed(`/design/validation-rules/${editRuleId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        tier: Number(editRuleTier),
        severity: editRuleSeverity,
        active: editRuleActive === "YES",
        config: cfg
      })
    });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["validation-rules"] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
    ]);
  }

  async function toggleValidationRule(ruleId, isActive) {
    await authed(`/design/validation-rules/${ruleId}/toggle?active=${isActive ? "false" : "true"}`, { method: "POST" });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["validation-rules"] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
    ]);
  }

  async function deleteValidationRule(ruleId) {
    await authed(`/design/validation-rules/${ruleId}`, { method: "DELETE" });
    if (editRuleId === ruleId) setEditRuleId(undefined);
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["validation-rules"] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
    ]);
  }

  function findExistingCoreRulesRule(programId, programName) {
    return (validationRulesQ.data || []).find((r) => {
      let cfg = {};
      try {
        cfg = JSON.parse(r.config_json || "{}");
      } catch {
        cfg = {};
      }
      const t = String(cfg.type || "").toUpperCase();
      if (!["MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"].includes(t)) return false;
      return (cfg.program_id && cfg.program_id === programId) || String(cfg.program_name || "").toLowerCase() === String(programName || "").toLowerCase();
    });
  }

  function openCoreRulesBuilder(programIdArg, programNameArg) {
    const majorProgramId =
      programIdArg ||
      editReqProgramId ||
      (programsQ.data || []).find(
        (p) =>
          p.name.toLowerCase() === String(editReqMajorName || "").trim().toLowerCase()
          && ["MAJOR", "MINOR"].includes(String(p.program_type || "").toUpperCase())
      )
        ?.id;
    const majorProgram =
      (programsQ.data || []).find((p) => p.id === majorProgramId) ||
      (programsQ.data || []).find(
        (p) =>
          p.name.toLowerCase() === String(programNameArg || editReqMajorName || "").trim().toLowerCase()
          && ["MAJOR", "MINOR"].includes(String(p.program_type || "").toUpperCase())
      );
    if (!majorProgram) {
      window.alert("Select or create a major/minor first.");
      return;
    }
    const optionalCoreReqs = (requirementsListQ.data || [])
      .filter(
        (r) =>
          !r.program_id &&
          String(r.category || "").toUpperCase() === "CORE" &&
          ["PICK_N", "ANY_ONE", "ONE_OF", "ANY_N"].includes(String(r.logic_type || "").toUpperCase())
      )
      .sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
    const rows = [];
    for (const req of optionalCoreReqs) {
      const logic = String(req.logic_type || "ALL_REQUIRED").toUpperCase();
      const slotCount = logic === "PICK_N" || logic === "ANY_N" ? Math.max(1, Number(req.pick_n || 1)) : 1;
      for (let i = 0; i < slotCount; i += 1) {
        rows.push({
          requirement_id: req.id,
          requirement_name: req.name,
          slot_index: i,
          slot_total: slotCount,
          primary_course_id: undefined,
          substitute_course_ids: [],
          required_semester: undefined,
          required_semester_min: undefined,
          required_semester_max: undefined,
        });
      }
    }
    const existingRule = findExistingCoreRulesRule(majorProgram.id, majorProgram.name);
    if (existingRule) {
      let cfg = {};
      try {
        cfg = JSON.parse(existingRule.config_json || "{}");
      } catch {
        cfg = {};
      }
      const groups = cfg.required_core_groups || [];
      for (const g of groups) {
        const reqId = g.source_requirement_id || g.requirement_id || null;
        const slotIndex = Number(g.slot_index || 0);
        const nums = (g.course_numbers || []).map((x) => String(x || "").trim()).filter(Boolean);
        if (!reqId || !nums.length) continue;
        const primaryRaw = courseIdByNumber[nums[0]];
        const representative = (coreRuleRepresentativeByReqCourse[reqId] || {})[primaryRaw] || primaryRaw;
        const selected = Array.from(new Set(nums.map((n) => courseIdByNumber[n]).filter(Boolean)));
        const targetIdx = rows.findIndex((r) => r.requirement_id === reqId && r.slot_index === slotIndex);
        if (targetIdx >= 0) {
          rows[targetIdx] = {
            ...rows[targetIdx],
            primary_course_id: representative,
            substitute_course_ids: selected,
            required_semester: g.required_semester || undefined,
            required_semester_min: g.required_semester_min || undefined,
            required_semester_max: g.required_semester_max || undefined,
          };
        }
      }
    }

    setCoreRulesProgramId(majorProgram.id);
    setCoreRulesProgramName(majorProgram.name);
    setCoreRulesRows(rows);
    setCoreRulesModalOpen(true);
  }

  async function saveCoreRulesBuilder() {
    if (!coreRulesProgramId || !coreRulesProgramName) return;
    const validationErrors = [];
    for (const r of coreRulesRows || []) {
      if (r.required_semester != null && (r.required_semester_min != null || r.required_semester_max != null)) {
        window.alert("Fixed semester cannot be combined with no earlier/no later rules.");
        return;
      }
      if (
        r.required_semester_min != null
        && r.required_semester_max != null
        && Number(r.required_semester_min) > Number(r.required_semester_max)
      ) {
        window.alert("No earlier than semester cannot be later than no later than semester.");
        return;
      }
      if (!r.primary_course_id) continue;
      const groupIds =
        (coreRuleChoiceOptionsByReq[r.requirement_id] || []).find((o) => o.value === r.primary_course_id)?.group_course_ids || [];
      const explicitSelected = Array.from(new Set((r.substitute_course_ids || []).filter(Boolean))).filter((id) => groupIds.includes(id));
      if (!explicitSelected.length && groupIds.length > 1) {
        validationErrors.push(`${r.requirement_name} - Choice ${Number(r.slot_index || 0) + 1}: choose at least one applicable course from the group.`);
      }
      const timingFields = timingFieldsFromRules(coreRuleTimingRules(r));
      const selectedForConflict = explicitSelected.length ? explicitSelected : groupIds;
      const conflictCid = hasTimingConflictWithExisting(selectedForConflict, timingFields);
      if (conflictCid) {
        const cnum = courseMapById[conflictCid]?.course_number || "course";
        validationErrors.push(`${r.requirement_name} - Choice ${Number(r.slot_index || 0) + 1}: timing conflicts with existing rule for ${cnum}.`);
      }
      if (timingFields.required_semester != null || timingFields.required_semester_min != null || timingFields.required_semester_max != null) {
        const candidateAllowed = allowedSemestersForTiming(timingFields);
        const selectedNums = selectedForConflict
          .map((cid) => normalizeCourseNumber(courseMapById[cid]?.course_number))
          .filter(Boolean);
        for (const num of selectedNums) {
          for (const ex of existingTimingByCourseNumber[num] || []) {
            const exAllowed = allowedSemestersForTiming(ex);
            const overlap = Array.from(candidateAllowed).some((s) => exAllowed.has(s));
            if (!overlap) {
              validationErrors.push(
                `${r.requirement_name} - Choice ${Number(r.slot_index || 0) + 1}: timing conflicts with existing rule for ${num}.`
              );
              break;
            }
          }
        }
      }
    }
    if (validationErrors.length) {
      window.alert(validationErrors[0]);
      return;
    }
    const groups = (coreRulesRows || [])
      .filter((r) => r.primary_course_id)
      .map((r) => {
        const groupIds =
          (coreRuleChoiceOptionsByReq[r.requirement_id] || []).find((o) => o.value === r.primary_course_id)?.group_course_ids || [];
        const explicitSelected = Array.from(new Set((r.substitute_course_ids || []).filter(Boolean))).filter((id) => groupIds.includes(id));
        const ids = explicitSelected.length ? explicitSelected : (groupIds.length === 1 ? [groupIds[0]] : []);
        const seen = new Set();
        const nums = ids
          .filter((id) => {
            if (!id || seen.has(id)) return false;
            seen.add(id);
            return true;
          })
          .map((id) => courseMapById[id]?.course_number)
          .filter(Boolean);
        return {
          name: `${r.requirement_name} - Choice ${Number(r.slot_index || 0) + 1}`,
          min_count: 1,
          course_numbers: nums,
          source_requirement_id: r.requirement_id,
          slot_index: r.slot_index,
          required_semester: r.required_semester || null,
          required_semester_min: r.required_semester_min || null,
          required_semester_max: r.required_semester_max || null,
        };
      })
      .filter((g) => (g.course_numbers || []).length);
    const payload = {
      name: `Program Pathway - ${coreRulesProgramName}`,
      tier: 2,
      severity: "FAIL",
      active: true,
      config: {
        type: "MAJOR_PATHWAY_CORE",
        program_id: coreRulesProgramId,
        program_name: coreRulesProgramName,
        required_core_groups: groups,
      },
    };
    const existingRule = findExistingCoreRulesRule(coreRulesProgramId, coreRulesProgramName);
    if (existingRule) {
      await authed(`/design/validation-rules/${existingRule.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tier: payload.tier,
          severity: payload.severity,
          active: payload.active,
          config: payload.config,
        }),
      });
    } else {
      await authed("/design/validation-rules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    }
    setCoreRulesModalOpen(false);
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["validation-rules"] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
    ]);
  }

  async function deleteCoreRulesGroup(programId, programName, groupIndex) {
    const existingRule = findExistingCoreRulesRule(programId, programName);
    if (!existingRule) return;
    let cfg = {};
    try {
      cfg = JSON.parse(existingRule.config_json || "{}");
    } catch {
      cfg = {};
    }
    const groups = [...(cfg.required_core_groups || [])];
    if (groupIndex < 0 || groupIndex >= groups.length) return;
    groups.splice(groupIndex, 1);
    if (!groups.length) {
      await authed(`/design/validation-rules/${existingRule.id}`, { method: "DELETE" });
    } else {
      await authed(`/design/validation-rules/${existingRule.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tier: existingRule.tier,
          severity: existingRule.severity,
          active: existingRule.active,
          config: { ...cfg, required_core_groups: groups },
        }),
      });
    }
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["validation-rules"] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
    ]);
  }

  function flattenTreeForPersist(nodes, parentId = null, out = []) {
    nodes.forEach((n, idx) => {
      out.push({ requirement_id: n.key, parent_requirement_id: parentId, sort_order: idx });
      if (n.children?.length) flattenTreeForPersist(n.children, n.key, out);
    });
    return out;
  }

  function moveTreeNode(nodes, dragKey, dropKey, dropToGap, relativeDropPos) {
    const data = JSON.parse(JSON.stringify(nodes));
    let dragObj = null;

    function loop(arr, key, cb) {
      for (let i = 0; i < arr.length; i += 1) {
        if (arr[i].key === key) return cb(arr, i);
        if (arr[i].children) {
          const res = loop(arr[i].children, key, cb);
          if (res) return res;
        }
      }
      return null;
    }

    loop(data, dragKey, (arr, idx) => {
      [dragObj] = arr.splice(idx, 1);
      return true;
    });
    if (!dragObj) return data;

    if (!dropToGap) {
      loop(data, dropKey, (arr, idx) => {
        arr[idx].children = arr[idx].children || [];
        arr[idx].children.unshift(dragObj);
        return true;
      });
    } else {
      loop(data, dropKey, (arr, idx) => {
        arr.splice(relativeDropPos < 0 ? idx : idx + 1, 0, dragObj);
        return true;
      });
    }
    return data;
  }

  async function handleTreeDrop(info) {
    const dragKey = String(info.dragNode?.key || "");
    const dropKey = String(info.node?.key || "");
    if (dropKey.startsWith("core-rules:") || dropKey.startsWith("core-rule-group:")) return;
    const dragIsCourse = dragKey.startsWith("course:");
    const dropIsCourse = dropKey.startsWith("course:");

    if (dragIsCourse) {
      const dragFulfillmentId = dragKey.replace("course:", "");
      const allLinks = (requirementFulfillmentVersionQ.data || []).map((r) => ({ ...r }));
      const byFulfillment = {};
      const byRequirement = {};
      for (const row of allLinks) {
        byFulfillment[row.id] = row;
        byRequirement[row.requirement_id] = byRequirement[row.requirement_id] || [];
        byRequirement[row.requirement_id].push(row);
      }
      const dragged = byFulfillment[dragFulfillmentId];
      if (!dragged) return;

      let targetRequirementId = null;
      if (dropIsCourse) {
        const dropFulfillmentId = dropKey.replace("course:", "");
        targetRequirementId = byFulfillment[dropFulfillmentId]?.requirement_id || null;
      } else {
        targetRequirementId = dropKey || null;
      }
      if (!targetRequirementId) return;

      const sourceRequirementId = dragged.requirement_id;
      const sourceRows = [...(byRequirement[sourceRequirementId] || [])];
      const targetRows = sourceRequirementId === targetRequirementId ? sourceRows : [...(byRequirement[targetRequirementId] || [])];
      const sourceWithoutDragged = sourceRows.filter((r) => r.id !== dragFulfillmentId);
      const targetWithoutDragged =
        sourceRequirementId === targetRequirementId ? sourceWithoutDragged : targetRows.filter((r) => r.id !== dragFulfillmentId);

      let insertIndex = targetWithoutDragged.length;
      if (dropIsCourse) {
        const dropFulfillmentId = dropKey.replace("course:", "");
        const idx = targetWithoutDragged.findIndex((r) => r.id === dropFulfillmentId);
        if (idx >= 0) {
          if (info.dropToGap) {
            const dropPos = info.node.pos.split("-");
            const relativeDropPos = info.dropPosition - Number(dropPos[dropPos.length - 1]);
            insertIndex = relativeDropPos < 0 ? idx : idx + 1;
          } else {
            insertIndex = idx;
          }
        }
      }
      if (insertIndex < 0) insertIndex = 0;
      if (insertIndex > targetWithoutDragged.length) insertIndex = targetWithoutDragged.length;

      const movedRow = { ...dragged, requirement_id: targetRequirementId };
      const nextTargetRows = [...targetWithoutDragged];
      nextTargetRows.splice(insertIndex, 0, movedRow);

      const payload = [];
      if (sourceRequirementId !== targetRequirementId) {
        sourceWithoutDragged.forEach((r, i) =>
          payload.push({ fulfillment_id: r.id, requirement_id: sourceRequirementId, sort_order: i })
        );
      }
      nextTargetRows.forEach((r, i) =>
        payload.push({ fulfillment_id: r.id, requirement_id: targetRequirementId, sort_order: i })
      );

      await authed("/requirements/fulfillment/reorder", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["requirement-fulfillment-version", selectedVersion?.id] }),
        qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion?.id] }),
        qc.invalidateQueries({ queryKey: ["requirement-fulfillment"] }),
        qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
      ]);
      return;
    }

    if (dropIsCourse) return;
    const treeData = (requirementsTreeQ.data?.tree || []).map(function mapNode(n) {
      return {
        key: n.id,
        title: `${n.name} (${n.logic_type})`,
        children: (n.children || []).map(mapNode)
      };
    });
    const dropPos = info.node.pos.split("-");
    const relativeDropPos = info.dropPosition - Number(dropPos[dropPos.length - 1]);
    const moved = moveTreeNode(treeData, info.dragNode.key, info.node.key, info.dropToGap, relativeDropPos);
    const payload = flattenTreeForPersist(moved);
    await authed("/requirements/restructure", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    await qc.invalidateQueries({ queryKey: ["requirements-tree"] });
  }

  function loadRequirementNodeToEditor(requirementId) {
    const n = requirementNodeMap[requirementId];
    const raw = requirementById[requirementId];
    if (!n && !raw) return;
    const logicType = raw?.logic_type || n?.logic_type || "ALL_REQUIRED";
    const pickN = raw?.pick_n ?? n?.pick_n;
    const fallbackProgramType = (programsQ.data || []).find((p) => p.id === (raw?.program_id || n?.program_id))?.program_type;
    const category = (raw?.category || n?.category || fallbackProgramType || (raw?.program_id || n?.program_id ? "MAJOR" : "CORE")).toUpperCase();
    const programId = raw?.program_id || n?.program_id || undefined;
    const parentId = raw?.parent_requirement_id || n?.parent_requirement_id || undefined;
    let majorMode = (raw?.major_mode || n?.major_mode || "REQUIREMENT").toUpperCase();
    const trackName = raw?.track_name || n?.track_name || "";
    const programName = programId ? (programsQ.data || []).find((p) => p.id === programId)?.name || n?.program_name || "" : "";
    const programDivision = programId ? (programsQ.data || []).find((p) => p.id === programId)?.division || n?.program_division || undefined : undefined;
    const reqName = raw?.name || n?.name || "";

    // Best-effort parse for legacy rows where category-specific fields were not populated.
    let inferredCoreTrack = "";
    let inferredMajorName = programName || "";
    if (!programName && reqName.startsWith("Major ")) {
      const m = reqName.replace(/^Major\s+/, "").split(":")[0].trim();
      inferredMajorName = m || "";
    } else if (!programName && reqName.startsWith("Minor ")) {
      const m = reqName.replace(/^Minor\s+/, "").split(":")[0].trim();
      inferredMajorName = m || "";
    }
    if (category === "CORE") {
      const m = reqName.match(/^Core\s+(.+?):/);
      inferredCoreTrack = trackName || (m ? m[1].trim() : "");
      if (trackName || /^Track\s*-\s*/i.test(reqName) || /^Core Track\s*-\s*/i.test(reqName)) {
        majorMode = "TRACK";
      }
    }

    setSelectedRuleNodeId(requirementId);
    setEditReqName(reqName);
    setEditReqLogic(logicType);
    setEditReqPickN(pickN != null ? String(pickN) : "");
    setEditReqProgramId(programId);
    setEditReqParentId(parentId);
    setEditReqCategory(category);
    setEditReqMajorMode(majorMode);
    setEditReqTrackName(trackName);
    setEditReqMajorName(inferredMajorName);
    setEditReqDivision(programDivision);
    setEditReqCoreTrack(inferredCoreTrack);
  }

  function handleRequirementTreeSelect(keys, info) {
    const key = info?.node?.key || keys?.[0];
    if (!key || String(key).startsWith("course:")) return;
    loadRequirementNodeToEditor(String(key));
  }

  const nodeLabel = useMemo(() => {
    const map = {};
    for (const n of prereqGraphQ.data?.nodes || []) map[n.id] = n.label;
    return map;
  }, [prereqGraphQ.data]);

  const plannedCourseNumbers = useMemo(() => {
    const set = new Set();
    for (const sem of Object.values(canvasQ.data || {})) {
      for (const c of sem || []) set.add(c.course_id);
    }
    return set;
  }, [canvasQ.data]);
  const unplannedCourses = useMemo(
    () => (coursesQ.data || []).filter((c) => !plannedCourseNumbers.has(c.id)),
    [coursesQ.data, plannedCourseNumbers]
  );
  const aspectLabel = (course) => {
    const a = String(course?.aspect || "CORE").toUpperCase();
    if (a === "PE") return "PE";
    if (a === "MAJOR_REQUIRED" || a === "MAJOR") return "Major Requirement";
    if (a === "MAJOR_TRACK" || a === "TRACK") return `Major Track${course?.track_name ? `: ${course.track_name}` : ""}`;
    return "Core";
  };
  const formatSemesterConstraint = (row) => {
    if (!row) return "";
    const exact = row.required_semester;
    const min = row.required_semester_min;
    const max = row.required_semester_max;
    if (exact != null) return `S${exact} fixed`;
    const parts = [];
    if (min) parts.push(`>= S${min}`);
    if (max) parts.push(`<= S${max}`);
    return parts.join(", ");
  };
  const coreRuleTimingRules = (row) =>
    timingRulesFromFields(row?.required_semester || null, row?.required_semester_min || null, row?.required_semester_max || null);
  const allowedSemestersForTiming = (timing) => {
    const allowed = new Set();
    for (let s = 1; s <= 8; s += 1) {
      if (timing.required_semester != null && Number(s) !== Number(timing.required_semester)) continue;
      if (timing.required_semester_min != null && Number(s) < Number(timing.required_semester_min)) continue;
      if (timing.required_semester_max != null && Number(s) > Number(timing.required_semester_max)) continue;
      allowed.add(s);
    }
    return allowed;
  };
  const setCoreRuleTimingRules = (idx, rules, courseIds = []) => {
    const fields = timingFieldsFromRules(rules);
    const hasFields = fields.required_semester != null || fields.required_semester_min != null || fields.required_semester_max != null;
    if (hasFields && (courseIds || []).length) {
      const candidateAllowed = allowedSemestersForTiming(fields);
      for (const cid of courseIds) {
        const existing = coreRequirementTimingByCourse[cid] || [];
        for (const ex of existing) {
          const exAllowed = allowedSemestersForTiming(ex);
          const overlap = Array.from(candidateAllowed).some((s) => exAllowed.has(s));
          if (!overlap) {
            const cnum = courseMapById[cid]?.course_number || "course";
            window.alert(`Timing rule conflicts with existing requirement timing for ${cnum}.`);
            return;
          }
        }
      }
    }
    setCoreRulesRows((prev) =>
      prev.map((r, i) =>
        i === idx
          ? {
              ...r,
              required_semester: fields.required_semester,
              required_semester_min: fields.required_semester_min,
              required_semester_max: fields.required_semester_max,
            }
          : r
      )
    );
  };
  const hasTimingConflictWithExisting = (courseIds, fields) => {
    const hasFields =
      fields?.required_semester != null || fields?.required_semester_min != null || fields?.required_semester_max != null;
    if (!hasFields || !(courseIds || []).length) return null;
    const candidateAllowed = allowedSemestersForTiming(fields);
    for (const cid of courseIds) {
      const existing = coreRequirementTimingByCourse[cid] || [];
      for (const ex of existing) {
        const exAllowed = allowedSemestersForTiming(ex);
        const overlap = Array.from(candidateAllowed).some((s) => exAllowed.has(s));
        if (!overlap) {
          return cid;
        }
      }
    }
    return null;
  };
  const semesterMatchesConstraint = (semester, row) => {
    if (!semester || !row) return false;
    if (row.required_semester != null && Number(semester) !== Number(row.required_semester)) return false;
    if (row.required_semester_min != null && Number(semester) < Number(row.required_semester_min)) return false;
    if (row.required_semester_max != null && Number(semester) > Number(row.required_semester_max)) return false;
    return row.required_semester != null || row.required_semester_min != null || row.required_semester_max != null;
  };
  const requirementNodeMap = useMemo(() => {
    const out = {};
    function walk(nodes) {
      for (const n of nodes || []) {
        out[n.id] = n;
        walk(n.children || []);
      }
    }
    walk(requirementsTreeQ.data?.tree || []);
    return out;
  }, [requirementsTreeQ.data]);
  const requirementFullPathMap = useMemo(() => {
    const out = {};
    const byId = {};
    for (const r of requirementsListQ.data || []) byId[r.id] = r;
    function pathFor(id) {
      const parts = [];
      let cur = byId[id];
      const seen = new Set();
      while (cur && !seen.has(cur.id)) {
        parts.unshift(cur.name);
        seen.add(cur.id);
        cur = cur.parent_requirement_id ? byId[cur.parent_requirement_id] : null;
      }
      return parts.join(": ");
    }
    Object.keys(byId).forEach((id) => {
      out[id] = pathFor(id);
    });
    return out;
  }, [requirementsListQ.data]);
  const requirementById = useMemo(() => {
    const out = {};
    for (const r of requirementsListQ.data || []) out[r.id] = r;
    return out;
  }, [requirementsListQ.data]);
  const courseMapById = useMemo(() => {
    const out = {};
    for (const c of coursesQ.data || []) out[c.id] = c;
    return out;
  }, [coursesQ.data]);
  const courseIdByNumber = useMemo(() => {
    const out = {};
    for (const c of coursesQ.data || []) out[String(c.course_number || "").trim()] = c.id;
    return out;
  }, [coursesQ.data]);
  const courseIdByNormalizedNumber = useMemo(() => {
    const out = {};
    for (const c of coursesQ.data || []) out[normalizeCourseNumber(c.course_number)] = c.id;
    return out;
  }, [coursesQ.data]);
  const requirementLinkedCoursesForModal = useMemo(() => {
    if (!reqCourseRequirementId) return [];
    return (requirementFulfillmentVersionQ.data || []).filter((x) => x.requirement_id === reqCourseRequirementId);
  }, [requirementFulfillmentVersionQ.data, reqCourseRequirementId]);
  const substitutionsForSelectedPrimary = useMemo(
    () => (requirementSubstitutionsQ.data || []).filter((s) => reqCourseId && s.primary_course_id === reqCourseId),
    [requirementSubstitutionsQ.data, reqCourseId]
  );
  const filteredRequirementOptions = useMemo(() => {
    const selectedFilter = rulesetFilter || "ALL";
    const nodes = Object.values(requirementNodeMap);
    return nodes
      .filter((n) => {
        if (selectedFilter === "ALL") return true;
        if (selectedFilter === "CORE_ALL") return !n.program_id && (n.category || "").toUpperCase() === "CORE";
        if (selectedFilter.startsWith("CORE_TRACK:")) return (n.category || "").toUpperCase() === "CORE" && (n.track_name || "") === selectedFilter.replace("CORE_TRACK:", "");
        if (selectedFilter === "MAJOR_ALL") return n.program_type === "MAJOR" || (n.category || "").toUpperCase() === "MAJOR";
        if (selectedFilter.startsWith("MAJOR:")) return n.program_id === selectedFilter.replace("MAJOR:", "");
        if (selectedFilter.startsWith("DIVISION:")) return (n.program_division || "") === selectedFilter.replace("DIVISION:", "");
        if (selectedFilter === "MINOR_ALL") return n.program_type === "MINOR";
        if (selectedFilter.startsWith("MINOR:")) return n.program_id === selectedFilter.replace("MINOR:", "");
        if (selectedFilter === "PE") return (n.category || "").toUpperCase() === "PE";
        return true;
      })
      .sort((a, b) => `${a.name}`.localeCompare(`${b.name}`))
      .map((n) => ({
        value: n.id,
        label: n.name,
      }));
  }, [requirementNodeMap, rulesetFilter]);
  const filteredTree = useMemo(() => {
    const selectedFilter = rulesetFilter || "ALL";
    function includeNode(n) {
      if (selectedFilter === "ALL") return true;
      if (selectedFilter === "CORE_ALL") return !n.program_id && (n.category || "").toUpperCase() === "CORE";
      if (selectedFilter.startsWith("CORE_TRACK:")) return (n.category || "").toUpperCase() === "CORE" && (n.track_name || "") === selectedFilter.replace("CORE_TRACK:", "");
      if (selectedFilter === "MAJOR_ALL") return n.program_type === "MAJOR" || (n.category || "").toUpperCase() === "MAJOR";
      if (selectedFilter.startsWith("MAJOR:")) return n.program_id === selectedFilter.replace("MAJOR:", "");
      if (selectedFilter.startsWith("DIVISION:")) return (n.program_division || "") === selectedFilter.replace("DIVISION:", "");
      if (selectedFilter === "MINOR_ALL") return n.program_type === "MINOR";
      if (selectedFilter.startsWith("MINOR:")) return n.program_id === selectedFilter.replace("MINOR:", "");
      if (selectedFilter === "PE") return (n.category || "").toUpperCase() === "PE";
      return true;
    }
    function filterNode(n) {
      const filteredChildren = (n.children || []).map(filterNode).filter(Boolean);
      const matchesScope = includeNode(n);
      if (!matchesScope && filteredChildren.length === 0) return null;
      return { ...n, children: filteredChildren };
    }
    return (requirementsTreeQ.data?.tree || []).map(filterNode).filter(Boolean);
  }, [requirementsTreeQ.data, rulesetFilter]);

  const integratedTreeData = useMemo(() => {
    const mappedByReq = {};
    for (const m of requirementFulfillmentVersionQ.data || []) {
      mappedByReq[m.requirement_id] = mappedByReq[m.requirement_id] || [];
      mappedByReq[m.requirement_id].push({
        id: m.id,
        course_id: m.course_id,
        course_number: m.course_number,
        course_title: m.course_title,
        required_semester: m.required_semester,
        required_semester_min: m.required_semester_min,
        required_semester_max: m.required_semester_max,
      });
    }
    const substitutionMap = {};
    for (const s of requirementSubstitutionsVersionQ.data || []) {
      const req = s.requirement_id;
      if (!substitutionMap[req]) substitutionMap[req] = {};
      if (!substitutionMap[req][s.primary_course_id]) substitutionMap[req][s.primary_course_id] = new Set();
      substitutionMap[req][s.primary_course_id].add(s.substitute_course_id);
      if (s.is_bidirectional) {
        if (!substitutionMap[req][s.substitute_course_id]) substitutionMap[req][s.substitute_course_id] = new Set();
        substitutionMap[req][s.substitute_course_id].add(s.primary_course_id);
      }
    }
    const coreRulesByProgram = {};
    for (const r of validationRulesQ.data || []) {
      let cfg = {};
      try {
        cfg = JSON.parse(r.config_json || "{}");
      } catch {
        cfg = {};
      }
      const t = String(cfg.type || "").toUpperCase();
      if (!["MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"].includes(t)) continue;
      const pid = cfg.program_id || null;
      const pname = String(cfg.program_name || "");
      if (pid) coreRulesByProgram[`id:${pid}`] = { rule: r, groups: cfg.required_core_groups || [] };
      if (pname) coreRulesByProgram[`name:${pname.toLowerCase()}`] = { rule: r, groups: cfg.required_core_groups || [] };
    }
    function mapNode(n) {
      const canAddSubNode = !n.parent_requirement_id;
      const isProgramNode = ["MAJOR", "MINOR"].includes(String(n.category || "").toUpperCase());
      const reqNode = {
        key: n.id,
        title: (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, width: "100%" }}>
            <span>{n.name}</span>
            <Space size={4} style={{ marginLeft: "auto", justifyContent: "flex-end" }}>
              {canAddSubNode ? (
                <Button
                  size="small"
                  type="primary"
                  onClick={(e) => {
                    e.stopPropagation();
                    openSubNodeEditor(n.id);
                  }}
                >
                  Add Sub Node
                </Button>
              ) : null}
              <Button
                size="small"
                type="primary"
                onClick={(e) => {
                  e.stopPropagation();
                  openRequirementCourseModal(n.id);
                }}
              >
                Add Course
              </Button>
              {isProgramNode && !n.parent_requirement_id ? (
                <Button
                  size="small"
                  type="primary"
                  onClick={(e) => {
                    e.stopPropagation();
                    openCoreRulesBuilder(n.program_id, n.program_name);
                  }}
                >
                  Core Rules
                </Button>
              ) : null}
              <Button
                size="small"
                onClick={(e) => {
                  e.stopPropagation();
                  openRequirementEditor(n.id);
                }}
              >
                Edit
              </Button>
              <Button
                size="small"
                danger
                onClick={(e) => {
                  e.stopPropagation();
                  deleteRequirementNodeById(n.id);
                }}
              >
                Delete
              </Button>
            </Space>
          </div>
        ),
        children: [],
      };
      const childReqs = (n.children || []).map(mapNode);
      const sourceCourses =
        (mappedByReq[n.id] && mappedByReq[n.id].length ? mappedByReq[n.id] : null) ||
        (n.courses && n.courses.length ? n.courses : []);
      const courseLeaves = sourceCourses.map((c) => ({
        key: `course:${c.id}`,
        title: (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, width: "100%" }}>
            <span>
              {(() => {
                const primaryCourseId =
                  c.course_id ||
                  c.courseId ||
                  (c.course_number ? courseIdByNumber[String(c.course_number || "").trim()] : undefined);
                if (!primaryCourseId) return c.course_number || "Course";
                const substituteIds = Array.from(substitutionMap[n.id]?.[primaryCourseId] || []).sort((a, b) => {
                  const aNum = courseMapById[a]?.course_number || a;
                  const bNum = courseMapById[b]?.course_number || b;
                  return String(aNum).localeCompare(String(bNum));
                });
                const orderedIds = [primaryCourseId, ...substituteIds];
                const seen = new Set();
                return orderedIds
                  .filter((id) => {
                    if (!id || seen.has(id)) return false;
                    seen.add(id);
                    return true;
                  })
                  .map((id) => courseMapById[id]?.course_number || id)
                  .join(" / ");
              })()}
            </span>
            {formatSemesterConstraint(c) ? <Tag>{formatSemesterConstraint(c)}</Tag> : null}
            <Space style={{ marginLeft: "auto" }}>
              <Button
                size="small"
                onClick={(e) => {
                  e.stopPropagation();
                  openRequirementCourseModal(n.id, {
                    primaryCourseId:
                      c.course_id ||
                      c.courseId ||
                      (c.course_number ? courseIdByNumber[String(c.course_number || "").trim()] : undefined),
                    requiredSemester: c.required_semester || undefined,
                    requiredSemesterMin: c.required_semester_min || undefined,
                    requiredSemesterMax: c.required_semester_max || undefined,
                  });
                }}
              >
                Edit
              </Button>
              <Button
                size="small"
                danger
                onClick={(e) => {
                  e.stopPropagation();
                  removeCourseFromRequirement(c.id, n.id);
                }}
              >
                Unlink
              </Button>
            </Space>
          </div>
        ),
        isLeaf: true,
        disableCheckbox: true,
        selectable: false,
      }));
      const coreRulesKey = n.program_id
        ? `id:${n.program_id}`
        : n.program_name
          ? `name:${String(n.program_name).toLowerCase()}`
          : null;
      const coreRuleEntry = coreRulesKey ? coreRulesByProgram[coreRulesKey] : null;
      let coreRuleNode = null;
      if (isProgramNode && !n.parent_requirement_id && coreRuleEntry) {
        const groupLeaves = (coreRuleEntry.groups || []).map((g, idx) => ({
          key: `core-rule-group:${n.id}:${idx}`,
          title: (
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, width: "100%" }}>
              <Space>
                <span>{`${g.name || `Core Rule ${idx + 1}`}: ${(g.course_numbers || []).join(" / ")}`}</span>
                {formatSemesterConstraint(g) ? <Tag>{formatSemesterConstraint(g)}</Tag> : null}
              </Space>
              <Button
                size="small"
                danger
                onClick={(e) => {
                  e.stopPropagation();
                  deleteCoreRulesGroup(n.program_id, n.program_name, idx);
                }}
              >
                Delete
              </Button>
            </div>
          ),
          isLeaf: true,
          disableCheckbox: true,
          selectable: false,
          draggable: false,
        }));
        coreRuleNode = {
          key: `core-rules:${n.id}`,
          title: (
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, width: "100%" }}>
              <span>Core Rules</span>
              <Button
                size="small"
                onClick={(e) => {
                  e.stopPropagation();
                  openCoreRulesBuilder(n.program_id, n.program_name);
                }}
              >
                Edit
              </Button>
            </div>
          ),
          children: groupLeaves,
          disableCheckbox: true,
          selectable: false,
          draggable: false,
        };
      }
      reqNode.children = coreRuleNode ? [coreRuleNode, ...courseLeaves, ...childReqs] : [...courseLeaves, ...childReqs];
      return reqNode;
    }
    return filteredTree.map(mapNode);
  }, [filteredTree, requirementFulfillmentVersionQ.data, requirementSubstitutionsVersionQ.data, courseMapById, courseIdByNumber, validationRulesQ.data]);
  const coreRequirementTimingByCourse = useMemo(() => {
    const out = {};
    for (const row of requirementFulfillmentVersionQ.data || []) {
      const req = requirementById[row.requirement_id];
      const isCore = String(req?.category || "").toUpperCase() === "CORE" && !req?.program_id;
      if (!isCore) continue;
      if (!formatSemesterConstraint(row)) continue;
      out[row.course_id] = out[row.course_id] || [];
      const key = `${row.required_semester || ""}|${row.required_semester_min || ""}|${row.required_semester_max || ""}`;
      if (out[row.course_id].some((x) => `${x.required_semester || ""}|${x.required_semester_min || ""}|${x.required_semester_max || ""}` === key)) {
        continue;
      }
      out[row.course_id].push({
        required_semester: row.required_semester,
        required_semester_min: row.required_semester_min,
        required_semester_max: row.required_semester_max,
      });
    }
    return out;
  }, [requirementFulfillmentVersionQ.data, requirementById]);
  const canvasTimingByCourse = useMemo(() => {
    const out = {};
    const addConstraint = (cid, constraint) => {
      if (!cid || !constraint || !formatSemesterConstraint(constraint)) return;
      out[cid] = out[cid] || [];
      const key = `${constraint.required_semester || ""}|${constraint.required_semester_min || ""}|${constraint.required_semester_max || ""}`;
      if (out[cid].some((x) => `${x.required_semester || ""}|${x.required_semester_min || ""}|${x.required_semester_max || ""}` === key)) return;
      out[cid].push({
        required_semester: constraint.required_semester,
        required_semester_min: constraint.required_semester_min,
        required_semester_max: constraint.required_semester_max,
      });
    };
    for (const [cid, rows] of Object.entries(coreRequirementTimingByCourse || {})) {
      for (const row of rows || []) addConstraint(cid, row);
    }
    const selectedIds = new Set(checklistProgramIds || []);
    const selectedNames = new Set(
      (programsQ.data || [])
        .filter((p) => selectedIds.has(p.id))
        .map((p) => String(p.name || "").trim().toLowerCase())
    );
    for (const vr of validationRulesQ.data || []) {
      let cfg = {};
      try {
        cfg = JSON.parse(vr.config_json || "{}");
      } catch {
        cfg = {};
      }
      const t = String(cfg.type || "").toUpperCase();
      if (!["MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"].includes(t)) continue;
      const pid = cfg.program_id || null;
      const pname = String(cfg.program_name || "").trim().toLowerCase();
      if (!(pid && selectedIds.has(pid)) && !(pname && selectedNames.has(pname))) continue;
      for (const g of cfg.required_core_groups || []) {
        const constraint = {
          required_semester: g.required_semester ?? null,
          required_semester_min: g.required_semester_min ?? null,
          required_semester_max: g.required_semester_max ?? null,
        };
        if (!formatSemesterConstraint(constraint)) continue;
        for (const num of g.course_numbers || []) {
          const cid = courseIdByNormalizedNumber[normalizeCourseNumber(num)];
          if (!cid) continue;
          addConstraint(cid, constraint);
        }
      }
    }
    return out;
  }, [coreRequirementTimingByCourse, checklistProgramIds, programsQ.data, validationRulesQ.data, courseIdByNormalizedNumber]);
  const checklistRequirementsTree = useMemo(() => {
    const rows = (checklistQ.data?.requirements || []).map((r) => ({ ...r, children: [...(r.children || [])] }));
    const byProgram = {};
    const top = [];
    for (const r of rows) {
      if (String(r.requirement_id || "").startsWith("core-rule:")) continue;
      top.push(r);
      if (r.program_id) byProgram[r.program_id] = r;
    }
    for (const r of rows) {
      if (!String(r.requirement_id || "").startsWith("core-rule:")) continue;
      const parent = byProgram[r.program_id];
      if (parent) {
        parent.children = [...(parent.children || []), r];
      } else {
        top.push(r);
      }
    }
    return top;
  }, [checklistQ.data]);
  const existingTimingByCourseNumber = useMemo(() => {
    const out = {};
    for (const [cid, rows] of Object.entries(coreRequirementTimingByCourse || {})) {
      const num = normalizeCourseNumber(courseMapById[cid]?.course_number);
      if (!num) continue;
      out[num] = out[num] || [];
      for (const row of rows || []) {
        out[num].push({
          required_semester: row.required_semester,
          required_semester_min: row.required_semester_min,
          required_semester_max: row.required_semester_max,
        });
      }
    }
    return out;
  }, [coreRequirementTimingByCourse, courseMapById]);
  const coreRuleChoiceOptionsByReq = useMemo(() => {
    const byReq = {};
    const coursesByReq = {};
    for (const f of requirementFulfillmentVersionQ.data || []) {
      coursesByReq[f.requirement_id] = coursesByReq[f.requirement_id] || [];
      coursesByReq[f.requirement_id].push(f.course_id);
    }
    const subsByReq = {};
    for (const s of requirementSubstitutionsVersionQ.data || []) {
      subsByReq[s.requirement_id] = subsByReq[s.requirement_id] || [];
      subsByReq[s.requirement_id].push(s);
    }
    for (const [reqId, rawIds] of Object.entries(coursesByReq)) {
      const linkedIds = Array.from(new Set(rawIds.filter(Boolean)));
      const setIds = new Set(linkedIds);
      const adj = {};
      linkedIds.forEach((id) => {
        adj[id] = new Set();
      });
      for (const s of subsByReq[reqId] || []) {
        const primaryLinked = setIds.has(s.primary_course_id);
        const substituteLinked = setIds.has(s.substitute_course_id);
        if (!primaryLinked && !substituteLinked) continue;
        if (!adj[s.primary_course_id]) adj[s.primary_course_id] = new Set();
        if (!adj[s.substitute_course_id]) adj[s.substitute_course_id] = new Set();
        adj[s.primary_course_id].add(s.substitute_course_id);
        adj[s.substitute_course_id].add(s.primary_course_id);
      }
      const seen = new Set();
      const groups = [];
      for (const id of linkedIds) {
        if (seen.has(id)) continue;
        const stack = [id];
        const group = [];
        while (stack.length) {
          const cur = stack.pop();
          if (!cur || seen.has(cur)) continue;
          seen.add(cur);
          group.push(cur);
          for (const nxt of adj[cur] || []) {
            if (!seen.has(nxt)) stack.push(nxt);
          }
        }
        const ordered = group.sort((a, b) => String(courseMapById[a]?.course_number || a).localeCompare(String(courseMapById[b]?.course_number || b)));
        const label = ordered.map((cid) => courseMapById[cid]?.course_number || cid).join(" / ");
        groups.push({ value: ordered[0], label, group_course_ids: ordered });
      }
      byReq[reqId] = groups.sort((a, b) => a.label.localeCompare(b.label));
    }
    return byReq;
  }, [requirementFulfillmentVersionQ.data, requirementSubstitutionsVersionQ.data, courseMapById]);
  const coreRuleRepresentativeByReqCourse = useMemo(() => {
    const out = {};
    for (const [reqId, options] of Object.entries(coreRuleChoiceOptionsByReq || {})) {
      out[reqId] = out[reqId] || {};
      for (const o of options || []) {
        for (const cid of o.group_course_ids || []) out[reqId][cid] = o.value;
      }
    }
    return out;
  }, [coreRuleChoiceOptionsByReq]);
  const coreRulesGroupedRows = useMemo(() => {
    const out = [];
    const map = {};
    for (const r of coreRulesRows || []) {
      const key = r.requirement_id;
      if (!map[key]) {
        map[key] = { requirement_id: r.requirement_id, requirement_name: r.requirement_name, slot_total: r.slot_total, rows: [] };
        out.push(map[key]);
      }
      map[key].rows.push(r);
    }
    out.forEach((g) => {
      g.rows.sort((a, b) => Number(a.slot_index || 0) - Number(b.slot_index || 0));
    });
    return out;
  }, [coreRulesRows]);
  const validationRulesWithDomain = useMemo(() => {
    function inferDomain(rule) {
      const name = String(rule?.name || "").toLowerCase();
      let cfg = {};
      try {
        cfg = JSON.parse(rule?.config_json || "{}");
      } catch {
        cfg = {};
      }
      if (
        name.includes("abet") ||
        name.includes("major") ||
        cfg.program ||
        cfg.program_id ||
        cfg.major ||
        cfg.pathway
      ) {
        return "PROGRAM_PATHWAY";
      }
      if (
        name.includes("prerequisite") ||
        name.includes("ordering") ||
        name.includes("co-requisite") ||
        name.includes("integrity")
      ) {
        return "CURRICULUM_INTEGRITY";
      }
      if (
        name.includes("section") ||
        name.includes("instructor") ||
        name.includes("classroom") ||
        name.includes("capacity") ||
        name.includes("resource")
      ) {
        return "RESOURCES";
      }
      return "GENERAL";
    }
    return (validationRulesQ.data || []).map((r) => ({ ...r, domain: inferDomain(r) }));
  }, [validationRulesQ.data]);
  const filteredValidationRules = useMemo(
    () =>
      validationDomainFilter === "ALL"
        ? validationRulesWithDomain
        : validationRulesWithDomain.filter((r) => r.domain === validationDomainFilter),
    [validationDomainFilter, validationRulesWithDomain]
  );
  const canvasFilteredCourseIds = useMemo(() => {
    const selectedFilter = canvasFilter || "ALL";
    if (selectedFilter === "ALL") return null;
    const linksByReq = {};
    for (const m of requirementFulfillmentVersionQ.data || []) {
      linksByReq[m.requirement_id] = linksByReq[m.requirement_id] || [];
      linksByReq[m.requirement_id].push(m.course_id);
    }
    function nodeMatches(n) {
      if (selectedFilter === "CORE_ALL") return !n.program_id && (n.category || "").toUpperCase() === "CORE";
      if (selectedFilter.startsWith("CORE_TRACK:")) return (n.category || "").toUpperCase() === "CORE" && (n.track_name || "") === selectedFilter.replace("CORE_TRACK:", "");
      if (selectedFilter === "MAJOR_ALL") return n.program_type === "MAJOR" || (n.category || "").toUpperCase() === "MAJOR";
      if (selectedFilter.startsWith("MAJOR:")) return n.program_id === selectedFilter.replace("MAJOR:", "");
      if (selectedFilter === "MINOR_ALL") return n.program_type === "MINOR";
      if (selectedFilter.startsWith("MINOR:")) return n.program_id === selectedFilter.replace("MINOR:", "");
      if (selectedFilter.startsWith("DIVISION:")) return (n.program_division || "") === selectedFilter.replace("DIVISION:", "");
      if (selectedFilter === "PE") return (n.category || "").toUpperCase() === "PE";
      return true;
    }
    const out = new Set();
    function walk(nodes, inheritedMatch = false) {
      for (const n of nodes || []) {
        const includeBranch = inheritedMatch || nodeMatches(n);
        if (includeBranch) {
          for (const c of n.courses || []) out.add(c.course_id);
          for (const cid of linksByReq[n.id] || []) out.add(cid);
        }
        walk(n.children || [], includeBranch);
      }
    }
    walk(requirementsTreeQ.data?.tree || [], false);
    return out;
  }, [canvasFilter, requirementsTreeQ.data, requirementFulfillmentVersionQ.data]);
  const allRequirementKeys = useMemo(() => {
    const keys = [];
    function walk(nodes) {
      for (const n of nodes || []) {
        keys.push(n.id);
        walk(n.children || []);
      }
    }
    walk(filteredTree || []);
    return keys;
  }, [filteredTree]);
  const majorNameOptions = useMemo(
    () =>
      (programsQ.data || [])
        .filter((p) => p.program_type === "MAJOR")
        .map((p) => ({ value: p.name })),
    [programsQ.data]
  );
  const minorNameOptions = useMemo(
    () =>
      (programsQ.data || [])
        .filter((p) => p.program_type === "MINOR")
        .map((p) => ({ value: p.name })),
    [programsQ.data]
  );
  const divisionOptions = useMemo(
    () => [
      { value: "HUMANITIES", label: "Humanities" },
      { value: "SOCIAL_SCIENCES", label: "Social Sciences" },
      { value: "BASIC_SCIENCES_AND_MATH", label: "Basic Sciences and Math" },
      { value: "ENGINEERING_SCIENCES", label: "Engineering Sciences" },
    ],
    []
  );
  const coreTrackOptions = useMemo(() => {
    const set = new Set();
    Object.values(requirementNodeMap).forEach((n) => {
      if ((n.category || "").toUpperCase() === "CORE" && n.track_name) set.add(n.track_name);
    });
    return Array.from(set).sort().map((x) => ({ value: x }));
  }, [requirementNodeMap]);
  const majorTrackOptions = useMemo(() => {
    const set = new Set();
    Object.values(requirementNodeMap).forEach((n) => {
      if ((n.category || "").toUpperCase() === "MAJOR" && n.track_name) set.add(n.track_name);
    });
    return Array.from(set).sort().map((x) => ({ value: x }));
  }, [requirementNodeMap]);
  const minorTrackOptions = useMemo(() => {
    const set = new Set();
    Object.values(requirementNodeMap).forEach((n) => {
      if ((n.category || "").toUpperCase() === "MINOR" && n.track_name) set.add(n.track_name);
    });
    return Array.from(set).sort().map((x) => ({ value: x }));
  }, [requirementNodeMap]);
  const rulesetFilterOptions = useMemo(() => {
    const coreTracks = Array.from(
      new Set(
        Object.values(requirementNodeMap)
          .filter((n) => (n.category || "").toUpperCase() === "CORE" && n.track_name)
          .map((n) => n.track_name)
      )
    ).sort();
    const majors = (programsQ.data || [])
      .filter((p) => p.program_type === "MAJOR")
      .sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
    const minors = (programsQ.data || [])
      .filter((p) => p.program_type === "MINOR")
      .sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
    const divisionList = [
      { value: "DIVISION:BASIC_SCIENCES_AND_MATH", label: "Division - Basic Sciences and Math" },
      { value: "DIVISION:ENGINEERING_SCIENCES", label: "Division - Engineering Sciences" },
      { value: "DIVISION:HUMANITIES", label: "Division - Humanities" },
      { value: "DIVISION:SOCIAL_SCIENCES", label: "Division - Social Sciences" },
    ];
    const ordered = [
      { value: "CORE_ALL", label: "Core - All" },
      ...coreTracks.map((t) => ({ value: `CORE_TRACK:${t}`, label: `Core - ${t}` })),
      ...divisionList,
      { value: "MAJOR_ALL", label: "Majors - All" },
      ...majors.map((p) => ({ value: `MAJOR:${p.id}`, label: `Major - ${p.name}` })),
      { value: "MINOR_ALL", label: "Minors - All" },
      ...minors.map((p) => ({ value: `MINOR:${p.id}`, label: `Minor - ${p.name}` })),
      { value: "PE", label: "PE" },
    ];
    return [{ value: "ALL", label: "No Filter" }, ...ordered];
  }, [programsQ.data, requirementNodeMap]);

  useEffect(() => {
    if (!selectedRuleNodeId) return;
    loadRequirementNodeToEditor(selectedRuleNodeId);
    setTreeExpandedKeys((prev) => (prev.includes(selectedRuleNodeId) ? prev : [...prev, selectedRuleNodeId]));
  }, [selectedRuleNodeId, requirementNodeMap]);

  useEffect(() => {
    // Default to expanded requirements so linked course leaves are visible under each node.
    setTreeExpandedKeys(allRequirementKeys);
  }, [allRequirementKeys]);

  useEffect(() => {
    if (!selectedVersion?.id) return;
    qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] });
  }, [requirementsTreeQ.dataUpdatedAt, selectedVersion?.id]);
  useEffect(() => {
    if (!selectedVersion?.id) return;
    qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion.id] });
  }, [requirementsTreeQ.dataUpdatedAt, validationRulesQ.dataUpdatedAt, selectedVersion?.id]);

  const isReqEditMode = !!selectedRuleNodeId;
  const isSubNodeCreate = !isReqEditMode && reqScopeLocked && !!editReqParentId;
  const isEditingSubNode = isReqEditMode && !!editReqParentId;

  function renderChecklist(nodes, level = 0) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {(nodes || []).map((r) => (
          <div key={`${r.requirement_id}-${level}`} style={{ marginLeft: level * 14 }}>
            <Space>
              <Tag color={r.is_satisfied ? "green" : "red"}>{r.is_satisfied ? "Met" : "Open"}</Tag>
              {String(r.requirement_id || "").startsWith("core-rule:")
                ? <Typography.Text>{`${r.name} ${r.satisfied_units}/${r.required_units}`}</Typography.Text>
                : (
                  <>
                    <Typography.Text strong={level === 0}>{r.name}</Typography.Text>
                    <Typography.Text type="secondary">
                      {r.satisfied_units}/{r.required_units}
                    </Typography.Text>
                  </>
                )}
            </Space>
            {(r.fixed_semester_violations || []).map((v) => (
              <div key={`${r.requirement_id}-${v}`}>
                <Typography.Text type="danger">{v}</Typography.Text>
              </div>
            ))}
            {r.children?.length ? <div style={{ marginTop: 6 }}>{renderChecklist(r.children, level + 1)}</div> : null}
          </div>
        ))}
      </div>
    );
  }
  const feasibilityColumns = [
    {
      title: "Type",
      dataIndex: "kind",
      key: "kind",
      width: 130,
      render: (v) => String(v || "").replaceAll("_", " "),
    },
    {
      title: "Combination",
      dataIndex: "label",
      key: "label",
      width: 340,
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      width: 110,
      render: (v) => <Tag color={v === "FAIL" ? "red" : v === "WARNING" ? "orange" : "green"}>{v}</Tag>,
    },
    {
      title: "Issues",
      dataIndex: "issue_count",
      key: "issue_count",
      width: 90,
    },
    {
      title: "Constraints",
      dataIndex: "constraint_count",
      key: "constraint_count",
      width: 110,
    },
    {
      title: "Min Credits",
      dataIndex: "min_required_credits",
      key: "min_required_credits",
      width: 110,
    },
    {
      title: "Mandatory Courses",
      dataIndex: "mandatory_course_count",
      key: "mandatory_course_count",
      width: 140,
    },
  ];
  const selectedChecklistFeasibility = useMemo(() => {
    const selected = checklistProgramIds || [];
    if (!selected.length) return null;
    const rows = feasibilityQ.data?.rows || [];
    const selectedSet = new Set(selected);
    const sameIds = (rowIds) => {
      const s = new Set(rowIds || []);
      if (s.size !== selectedSet.size) return false;
      for (const id of selectedSet) if (!s.has(id)) return false;
      return true;
    };
    if (selected.length === 1) {
      return rows.find((r) => sameIds(r.program_ids)) || null;
    }
    if (selected.length === 2) {
      return rows.find((r) => sameIds(r.program_ids)) || null;
    }
    return null;
  }, [checklistProgramIds, feasibilityQ.data]);

  return (
    <Space direction="vertical" style={{ width: "100%" }}>
      <Card title="Phase 2: Dean's Design Studio">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text>
            Selected version: <b>{selectedVersion?.name || "None"}</b>
          </Typography.Text>
          <Space>
            <Select
              style={{ width: 320 }}
              placeholder="Curriculum version"
              value={selectedVersionId}
              onChange={setSelectedVersionId}
              options={(versionsQ.data || []).map((v) => ({ value: v.id, label: `${v.name} (${v.status})` }))}
            />
            <Select
              showSearch
              optionFilterProp="label"
              style={{ width: 320 }}
              value={canvasFilter}
              onChange={setCanvasFilter}
              options={rulesetFilterOptions}
            />
            <Typography.Text type="secondary">Verbose</Typography.Text>
            <Switch checked={verboseCanvas} onChange={setVerboseCanvas} />
          </Space>
          <Space>
            <Typography.Text type="secondary">Version Lifecycle</Typography.Text>
            <Button size="small" onClick={() => setVersionStatus("DRAFT")}>DRAFT</Button>
            <Button size="small" onClick={() => setVersionStatus("UNDER_REVIEW")}>UNDER_REVIEW</Button>
            <Button size="small" onClick={() => setVersionStatus("APPROVED")}>APPROVED</Button>
            <Button size="small" onClick={() => setVersionStatus("ACTIVE")}>ACTIVE</Button>
            <Button size="small" onClick={() => setVersionStatus("ARCHIVED")}>ARCHIVED</Button>
          </Space>
        </Space>
      </Card>
      <Row gutter={12}>
        {[1, 2, 3, 4, 5, 6, 7, 8].map((semester) => (
          <Col key={semester} xs={24} sm={12} md={8} lg={6} xl={6}>
            <Card
              size="small"
              title={
                <Space>
                  <span>{`Semester ${semester}`}</span>
                  <Tag>
                    {(canvasQ.data?.[String(semester)] || [])
                      .filter((course) => !canvasFilteredCourseIds || canvasFilteredCourseIds.has(course.course_id))
                      .reduce((sum, c) => sum + Number(c.credits || 0), 0)} cr
                  </Tag>
                </Space>
              }
              extra={
                <Button size="small" type="primary" onClick={() => openAddCourseModal(semester)}>
                  Add Course
                </Button>
              }
              className="semester-card"
              onDragOver={(e) => e.preventDefault()}
              onDrop={(e) => dropToSemester(semester, e)}
            >
              {(canvasQ.data?.[String(semester)] || [])
                .filter((course) => !canvasFilteredCourseIds || canvasFilteredCourseIds.has(course.course_id))
                .map((course) => (
                <Tooltip
                  key={course.plan_item_id}
                  title={`${course.course_number} | ${course.title} | ${aspectLabel(course)}${course.major_program_name ? ` | ${course.major_program_name}` : ""}`}
                >
                  <Card
                    size="small"
                    draggable
                    onDragStart={(e) => e.dataTransfer.setData("text/plain", course.plan_item_id)}
                    onDragOver={(e) => e.preventDefault()}
                    onDrop={(e) => dropToCourse(semester, course.plan_item_id, e)}
                    style={{ marginBottom: 8, cursor: "grab" }}
                  >
	                    <Space direction="vertical" style={{ width: "100%" }}>
	                      <Typography.Text strong>{course.course_number}</Typography.Text>
	                      <Space style={{ width: "100%", justifyContent: "flex-end" }} wrap>
	                        {canvasTimingByCourse[course.course_id] ? (
	                          (() => {
	                            const constraints = canvasTimingByCourse[course.course_id] || [];
	                            const inRequired = constraints.some((c) => semesterMatchesConstraint(semester, c));
	                            const labels = constraints.map((c) => formatSemesterConstraint(c)).filter(Boolean);
	                            const single = labels.length === 1 ? labels[0] : `${labels.length} constraints`;
	                            return (
	                              <Tooltip title={labels.join(" | ")}>
	                                <Tag color={inRequired ? "green" : "orange"}>{single}</Tag>
	                              </Tooltip>
	                            );
	                          })()
	                        ) : null}
	                        <Space>
	                          <Tag>{course.credits} cr</Tag>
	                          <Button size="small" onClick={() => openEditCourseModal(course, semester)}>
	                            Edit
	                          </Button>
	                          <Button size="small" danger onClick={() => removeFromCanvas(course.plan_item_id)}>
	                            Delete
	                          </Button>
	                        </Space>
	                      </Space>
                      {verboseCanvas && (
                        <Space>
                          <Typography.Text type="secondary">{course.title}</Typography.Text>
                          <Tag color="blue">{aspectLabel(course)}</Tag>
                          {course.major_program_name && <Tag color="geekblue">{course.major_program_name}</Tag>}
                        </Space>
                      )}
                    </Space>
                  </Card>
                </Tooltip>
              ))}
            </Card>
          </Col>
        ))}
      </Row>
      <Card title="Dynamic Checklist (Core + Selected Programs)">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Select
            mode="multiple"
            allowClear
            style={{ width: "100%" }}
            placeholder="Select major/minor programs to evaluate with core requirements"
            value={checklistProgramIds}
            onChange={setChecklistProgramIds}
            options={(programsQ.data || []).map((p) => ({
              value: p.id,
              label: `${String(p.program_type || "").toUpperCase() === "MINOR" ? "Minor" : "Major"} - ${p.name}`,
            }))}
          />
          <Typography.Text type="secondary">
            Requirement and Core Rules checks green up as the canvas satisfies them (including substitution credit).
          </Typography.Text>
          {selectedChecklistFeasibility?.status === "FAIL" ? (
            <Space>
              <Tag color="orange">Warning</Tag>
              <Typography.Text type="danger">
                Selected checklist programs are not a valid combination due to feasibility check failures.
              </Typography.Text>
            </Space>
          ) : null}
          <Space>
            <Tag color="blue">Completion {checklistQ.data?.summary?.completion_percent ?? 0}%</Tag>
            <Typography.Text type="secondary">
              {checklistQ.data?.summary?.top_level_satisfied ?? 0}/{checklistQ.data?.summary?.top_level_total ?? 0} top-level requirements met
            </Typography.Text>
          </Space>
          <div>{renderChecklist(checklistRequirementsTree || [])}</div>
        </Space>
      </Card>
      <Card title="Feasibility Analysis">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Space>
            <Tag color="green">{`Pass ${feasibilityQ.data?.summary?.pass ?? 0}`}</Tag>
            <Tag color="orange">{`Warning ${feasibilityQ.data?.summary?.warning ?? 0}`}</Tag>
            <Tag color="red">{`Fail ${feasibilityQ.data?.summary?.fail ?? 0}`}</Tag>
            <Typography.Text type="secondary">
              {`${feasibilityQ.data?.row_count ?? 0} combinations evaluated`}
            </Typography.Text>
          </Space>
          <Table
            size="small"
            rowKey={(r) => `${r.kind}:${r.label}`}
            loading={feasibilityQ.isLoading}
            columns={feasibilityColumns}
            dataSource={feasibilityQ.data?.rows || []}
            pagination={{ pageSize: 25, showSizeChanger: false }}
            scroll={{ x: 980, y: 420 }}
            expandable={{
              expandedRowRender: (row) => (
                <Space direction="vertical" style={{ width: "100%" }}>
                  <Typography.Text strong>Issues</Typography.Text>
                  {row.issues?.length ? (
                    <List
                      size="small"
                      dataSource={row.issues}
                      renderItem={(it) => (
                        <List.Item>
                          <Typography.Text type="danger">{it}</Typography.Text>
                        </List.Item>
                      )}
                    />
                  ) : (
                    <Typography.Text type="secondary">None</Typography.Text>
                  )}
                  <Typography.Text strong>Constraints</Typography.Text>
                  {row.constraints?.length ? (
                    <List
                      size="small"
                      dataSource={row.constraints}
                      renderItem={(it) => (
                        <List.Item>
                          <Typography.Text>{it}</Typography.Text>
                        </List.Item>
                      )}
                    />
                  ) : (
                    <Typography.Text type="secondary">None</Typography.Text>
                  )}
                </Space>
              ),
            }}
          />
        </Space>
      </Card>
      <Modal
        title={`Add Course to Semester ${addSemester || ""}`}
        open={addModalOpen}
        onCancel={closeAddCourseModal}
        onOk={submitAddCourse}
        okText="Add Course"
        okButtonProps={{
          disabled: !addCourseId || !addSemester,
        }}
      >
        <Space direction="vertical" style={{ width: "100%" }}>
          <Select
            showSearch
            allowClear
            style={{ width: "100%" }}
            placeholder="Course"
            value={addCourseId}
            onChange={setAddCourseId}
            filterOption={(input, option) => (option?.label || "").toLowerCase().includes(input.toLowerCase())}
            options={unplannedCourses.map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
          />
        </Space>
      </Modal>
      <Modal
        title="Edit Canvas Course"
        open={editModalOpen}
        onCancel={() => setEditModalOpen(false)}
        onOk={updateCanvasItem}
        okText="Save"
      >
        <Space direction="vertical" style={{ width: "100%" }}>
          <Select
            showSearch
            style={{ width: "100%" }}
            placeholder="Course"
            value={editCourseId}
            onChange={setEditCourseId}
            filterOption={(input, option) => (option?.label || "").toLowerCase().includes(input.toLowerCase())}
            options={(coursesQ.data || []).map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
          />
        </Space>
      </Modal>
      <Modal
        title={isReqEditMode ? "Edit Requirement Node" : isSubNodeCreate ? "Add Sub Node" : "Add Requirement Node"}
        open={reqEditorOpen}
        onCancel={() => {
          setReqEditorOpen(false);
          setReqScopeLocked(false);
          setReqLockedCategory(undefined);
        }}
        footer={null}
      >
        <Space direction="vertical" style={{ width: "100%" }}>
          <Select
            style={{ width: "100%" }}
            value={editReqCategory}
            onChange={setEditReqCategory}
            disabled={reqScopeLocked}
            options={[
              { value: "CORE", label: "Core" },
              { value: "MAJOR", label: "Major" },
              { value: "MINOR", label: "Minor" },
              { value: "PE", label: "PE" },
            ]}
          />
          {(editReqCategory === "MAJOR" || editReqCategory === "MINOR") && (
            <AutoComplete
              style={{ width: "100%" }}
              options={editReqCategory === "MINOR" ? minorNameOptions : majorNameOptions}
              value={editReqMajorName}
              onChange={setEditReqMajorName}
              disabled={reqScopeLocked && (reqLockedCategory === "MAJOR" || reqLockedCategory === "MINOR")}
              placeholder={`${editReqCategory === "MINOR" ? "Minor" : "Major"} (search or type new)`}
              filterOption={(inputValue, option) => (option?.value || "").toLowerCase().includes(inputValue.toLowerCase())}
            />
          )}
          {(editReqCategory === "MAJOR" || editReqCategory === "MINOR") && !isSubNodeCreate && !isEditingSubNode && (
            <Select
              allowClear
              style={{ width: "100%" }}
              placeholder="Division"
              value={editReqDivision}
              onChange={setEditReqDivision}
              options={divisionOptions}
            />
          )}
          {editReqCategory === "CORE" && (isSubNodeCreate || isReqEditMode) && (
            <Select
              style={{ width: "100%" }}
              value={editReqMajorMode}
              onChange={setEditReqMajorMode}
              options={[
                { value: "REQUIREMENT", label: "Core Requirement" },
                { value: "TRACK", label: "Track" },
              ]}
            />
          )}
          {editReqCategory === "CORE" && (isSubNodeCreate || isReqEditMode) && editReqMajorMode === "TRACK" && (
            <AutoComplete
              style={{ width: "100%" }}
              options={coreTrackOptions}
              value={editReqCoreTrack}
              onChange={setEditReqCoreTrack}
              placeholder="Core track (search or type new)"
              filterOption={(inputValue, option) => (option?.value || "").toLowerCase().includes(inputValue.toLowerCase())}
            />
          )}
          {(editReqCategory === "MAJOR" || editReqCategory === "MINOR") && (isSubNodeCreate || isReqEditMode) && (
            <Select
              style={{ width: "100%" }}
              value={editReqMajorMode}
              onChange={setEditReqMajorMode}
              options={[
                { value: "REQUIREMENT", label: editReqCategory === "MINOR" ? "Minor Requirement" : "Major Requirement" },
                { value: "TRACK", label: "Track" },
              ]}
            />
          )}
          {(editReqCategory === "MAJOR" || editReqCategory === "MINOR") && (isSubNodeCreate || isReqEditMode) && editReqMajorMode === "TRACK" && (
            <AutoComplete
              style={{ width: "100%" }}
              options={editReqCategory === "MINOR" ? minorTrackOptions : majorTrackOptions}
              value={editReqTrackName}
              onChange={setEditReqTrackName}
              placeholder="Track (search or type new)"
              filterOption={(inputValue, option) => (option?.value || "").toLowerCase().includes(inputValue.toLowerCase())}
            />
          )}
          {(isSubNodeCreate || isReqEditMode) && (
            <Select
              style={{ width: "100%" }}
              value={editReqLogic}
              onChange={setEditReqLogic}
              options={[
                { value: "ALL_REQUIRED", label: "ALL_REQUIRED" },
                { value: "PICK_N", label: "PICK_N" },
                { value: "ANY_ONE", label: "ANY_ONE" },
              ]}
            />
          )}
          {(isSubNodeCreate || isReqEditMode) && editReqLogic === "PICK_N" && (
            <Input
              style={{ width: "100%" }}
              placeholder="Pick N"
              value={editReqPickN}
              onChange={(e) => setEditReqPickN(e.target.value)}
            />
          )}
          <Button type="primary" onClick={saveRequirementNode}>
            Save Node
          </Button>
        </Space>
      </Modal>
      <Modal
        title="Add Course To Requirement"
        open={reqCourseModalOpen}
        onCancel={() => {
          setReqCourseModalOpen(false);
          setReqSubDraftRows([]);
          setReqCourseTimingRules([]);
        }}
        onOk={saveRequirementCourseModal}
        okText="Save Course"
        okButtonProps={{ disabled: !reqCourseId }}
      >
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text type="secondary">
            {requirementFullPathMap[reqCourseRequirementId || ""] || requirementNodeMap[reqCourseRequirementId || ""]?.name || "None"}
          </Typography.Text>
          <Select
            allowClear
            showSearch
            optionFilterProp="label"
            style={{ width: "calc(100% - 84px)" }}
            placeholder="Select course"
            value={reqCourseId}
            onChange={(v) => {
              setReqCourseId(v);
              const existing = (requirementLinkedCoursesForModal || []).find((x) => x.course_id === v);
              setReqCourseTimingRules(
                timingRulesFromFields(
                  existing?.required_semester || null,
                  existing?.required_semester_min || null,
                  existing?.required_semester_max || null
                )
              );
            }}
            options={(coursesQ.data || []).map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
          />
          <Typography.Text strong>Timing Rules</Typography.Text>
          {reqCourseTimingRules.map((rule, idx) => {
            const usedOther = new Set(reqCourseTimingRules.filter((_, i) => i !== idx).map((r) => r.type));
            const typeOptions = timingTypeOptions.filter((o) => !usedOther.has(o.value) || o.value === rule.type);
            return (
              <div key={`timing-rule-${idx}`} style={{ width: "100%", display: "flex", alignItems: "center", gap: 8 }}>
                <Select
                  style={{ width: 210 }}
                  value={rule.type}
                  onChange={(v) =>
                    setReqCourseTimingRules((prev) => {
                      const next = prev.map((r, i) => (i === idx ? { ...r, type: v } : r));
                      if (v === "FIXED") return [{ type: "FIXED", semester: next[idx]?.semester }];
                      return next.filter((r) => r.type !== "FIXED");
                    })
                  }
                  options={typeOptions}
                />
                <Select
                  allowClear
                  style={{ width: "calc(100% - 294px)" }}
                  placeholder="Semester"
                  value={rule.semester}
                  onChange={(v) =>
                    setReqCourseTimingRules((prev) =>
                      prev.map((r, i) => (i === idx ? { ...r, semester: v } : r))
                    )
                  }
                  options={semesterOptions}
                />
                <Button
                  danger
                  style={{ width: 76 }}
                  onClick={() =>
                    setReqCourseTimingRules((prev) => prev.filter((_, i) => i !== idx))
                  }
                >
                  Delete
                </Button>
              </div>
            );
          })}
          <Button
            onClick={() =>
              setReqCourseTimingRules((prev) => {
                if (prev.some((r) => r.type === "FIXED")) return prev;
                if (prev.length === 0) return [{ type: "FIXED", semester: undefined }];
                if (!prev.some((r) => r.type === "MIN")) return [...prev, { type: "MIN", semester: undefined }];
                if (!prev.some((r) => r.type === "MAX")) return [...prev, { type: "MAX", semester: undefined }];
                return prev;
              })
            }
            disabled={reqCourseTimingRules.some((r) => r.type === "FIXED") || reqCourseTimingRules.length >= 2}
          >
            Add Timing Rule
          </Button>
          <Typography.Text strong>Substitutions</Typography.Text>
          {reqSubDraftRows.map((row, idx) => (
            <div key={`new-sub-${idx}`} style={{ width: "100%", display: "flex", alignItems: "center", gap: 8 }}>
              <Select
                allowClear
                showSearch
                optionFilterProp="label"
                style={{ width: "calc(100% - 84px)" }}
                placeholder="Substitute course"
                value={row.course_id}
                onChange={(v) =>
                  setReqSubDraftRows((prev) =>
                    prev.map((r, i) => (i === idx ? { ...r, course_id: v === reqCourseId ? undefined : v } : r))
                  )
                }
                options={(coursesQ.data || [])
                  .filter((c) => c.id !== reqCourseId)
                  .map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
              />
              <Button
                danger
                style={{ width: 76 }}
                onClick={() => setReqSubDraftRows((prev) => prev.filter((_, i) => i !== idx))}
              >
                Delete
              </Button>
            </div>
          ))}
          <Button
            onClick={() => setReqSubDraftRows((prev) => [...prev, { course_id: undefined }])}
            disabled={!reqCourseId}
          >
            Add Substitution
          </Button>
          <List
            size="small"
            dataSource={substitutionsForSelectedPrimary}
            renderItem={(s) => (
              <List.Item
                actions={[
                  <Button key="delete" size="small" danger onClick={() => deleteRequirementSubstitution(s.id)}>
                    Delete
                  </Button>,
                ]}
              >
                <Select
                  allowClear
                  showSearch
                  optionFilterProp="label"
                  style={{ width: "calc(100% - 84px)" }}
                  value={s.substitute_course_id}
                  onChange={(v) => updateRequirementSubstitutionCourse(s.id, v)}
                  options={(coursesQ.data || [])
                    .filter((c) => c.id !== s.primary_course_id)
                    .map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
                />
              </List.Item>
            )}
          />
        </Space>
      </Modal>
      <Modal
        title={`Core Rules: ${coreRulesProgramName || "Major"}`}
        open={coreRulesModalOpen}
        onCancel={() => setCoreRulesModalOpen(false)}
        onOk={saveCoreRulesBuilder}
        okText="Save Core Rules"
      >
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text type="secondary">
            Define major-specific choices from Core Any One/Pick N nodes.
          </Typography.Text>
          {coreRulesGroupedRows.map((group) => (
            <Card
              key={`core-rules-group:${group.requirement_id}`}
              size="small"
              style={{ borderColor: group.slot_total > 1 ? "#b7ccff" : undefined, background: group.slot_total > 1 ? "#fafcff" : undefined }}
              title={group.requirement_name}
            >
              <Space direction="vertical" style={{ width: "100%" }}>
                {group.rows.map((row) => {
                  const rowKey = `${row.requirement_id}:${row.slot_index}`;
                  const idx = (coreRulesRows || []).findIndex((x) => x.requirement_id === row.requirement_id && x.slot_index === row.slot_index);
                  const siblingSelected = new Set(
                    (group.rows || [])
                      .filter((r) => r.slot_index !== row.slot_index)
                      .map((r) => r.primary_course_id)
                      .filter(Boolean)
                  );
                  const choiceOptions = (coreRuleChoiceOptionsByReq[row.requirement_id] || []).filter(
                    (o) => !siblingSelected.has(o.value) || o.value === row.primary_course_id
                  );
                  const selectedChoice = choiceOptions.find((o) => o.value === row.primary_course_id);
                  const groupCourseIds = selectedChoice?.group_course_ids || [];
                  const maxSubChoices = Math.max(0, groupCourseIds.length);
                  const existingTimingRows = groupCourseIds.flatMap((cid) => coreRequirementTimingByCourse[cid] || []);
                  const existingTimingLabels = Array.from(new Set(existingTimingRows.map((x) => formatSemesterConstraint(x)).filter(Boolean)));
                  const hasExistingGroupTiming = existingTimingLabels.length > 0;
                  return (
                    <Card key={`core-rule-row:${rowKey}`} size="small" title={row.slot_total > 1 ? `Pick ${row.slot_index + 1}/${row.slot_total}` : "Any One"}>
                      <Space direction="vertical" style={{ width: "100%" }}>
                        <Select
                          allowClear
                          showSearch
                          optionFilterProp="label"
                          style={{ width: "100%" }}
                          placeholder="Required core choice (course group)"
                          value={row.primary_course_id}
                          onChange={(v) => {
                            const selected = (coreRuleChoiceOptionsByReq[row.requirement_id] || []).find((o) => o.value === v);
                            const nextGroupIds = selected?.group_course_ids || [];
                            const timingFields = timingFieldsFromRules(coreRuleTimingRules(row));
                            const conflictCid = hasTimingConflictWithExisting(nextGroupIds, timingFields);
                            if (conflictCid) {
                              const cnum = courseMapById[conflictCid]?.course_number || "course";
                              window.alert(`Timing rule conflicts with existing requirement timing for ${cnum}.`);
                              return;
                            }
                            setCoreRulesRows((prev) =>
                              prev.map((r, i) => (i === idx ? { ...r, primary_course_id: v, substitute_course_ids: [] } : r))
                            );
                          }}
                          options={choiceOptions}
                        />
                        {(row.substitute_course_ids || []).map((subId, subIdx) => (
                          <div key={`core-sub-choice-${idx}-${subIdx}`} style={{ width: "100%", display: "flex", alignItems: "center", gap: 8 }}>
                            {(() => {
                              const selectedElsewhere = new Set(
                                (row.substitute_course_ids || []).filter((_, j) => j !== subIdx).filter(Boolean)
                              );
                              const optionsForThisSub = groupCourseIds
                                .filter((cid) => !selectedElsewhere.has(cid) || cid === subId)
                                .map((cid) => ({ value: cid, label: courseMapById[cid]?.course_number || cid }));
                              return (
                            <Select
                              allowClear
                              showSearch
                              optionFilterProp="label"
                              style={{ width: "calc(100% - 84px)" }}
                              placeholder="Applicable course choice"
                              value={subId}
                              onChange={(v) =>
                                setCoreRulesRows((prev) =>
                                  prev.map((r, i) => {
                                    if (i !== idx) return r;
                                    const next = [...(r.substitute_course_ids || [])];
                                    next[subIdx] = v;
                                    const unique = [];
                                    const seen = new Set();
                                    for (const x of next) {
                                      if (!x || seen.has(x)) continue;
                                      seen.add(x);
                                      unique.push(x);
                                    }
                                    return { ...r, substitute_course_ids: unique };
                                  })
                                )
                              }
                              options={optionsForThisSub}
                            />
                              );
                            })()}
                            <Button
                              danger
                              style={{ width: 76 }}
                              onClick={() =>
                                setCoreRulesRows((prev) =>
                                  prev.map((r, i) =>
                                    i === idx
                                      ? { ...r, substitute_course_ids: (r.substitute_course_ids || []).filter((_, j) => j !== subIdx) }
                                      : r
                                  )
                                )
                              }
                            >
                              Delete
                            </Button>
                          </div>
                        ))}
                        {maxSubChoices > 0 ? (
                          <Button
                            onClick={() =>
                              setCoreRulesRows((prev) =>
                                prev.map((r, i) =>
                                  i === idx ? { ...r, substitute_course_ids: [...(r.substitute_course_ids || []), undefined] } : r
                                )
                              )
                            }
                            disabled={!row.primary_course_id || (row.substitute_course_ids || []).length >= maxSubChoices}
                          >
                            Choose Applicable Course
                          </Button>
                        ) : null}
                        {hasExistingGroupTiming ? (
                          <Typography.Text type="secondary">
                            Timing rule already exists for this course/course group: {existingTimingLabels.join(" | ")}
                          </Typography.Text>
                        ) : null}
                        <Typography.Text strong>Timing Rules</Typography.Text>
                        {coreRuleTimingRules(row).map((rule, ruleIdx) => {
                          const rowRules = coreRuleTimingRules(row);
                          const usedOther = new Set(rowRules.filter((_, i) => i !== ruleIdx).map((r) => r.type));
                          const typeOptions = timingTypeOptions.filter((o) => !usedOther.has(o.value) || o.value === rule.type);
                          return (
                            <div
                              key={`core-rule-timing-${idx}-${ruleIdx}`}
                              style={{ width: "100%", display: "flex", alignItems: "center", gap: 8 }}
                            >
                              <Select
                                style={{ width: 210 }}
                                value={rule.type}
                                disabled={hasExistingGroupTiming}
                                onChange={(v) => {
                                  const next = rowRules.map((r, i) => (i === ruleIdx ? { ...r, type: v } : r));
                                  if (v === "FIXED") {
                                    setCoreRuleTimingRules(idx, [{ type: "FIXED", semester: next[ruleIdx]?.semester ?? 1 }], groupCourseIds);
                                    return;
                                  }
                                  setCoreRuleTimingRules(
                                    idx,
                                    next
                                      .filter((r) => r.type !== "FIXED")
                                      .map((r) => ({ ...r, semester: r.semester ?? 1 })),
                                    groupCourseIds
                                  );
                                }}
                                options={typeOptions}
                              />
                              <Select
                                allowClear
                                style={{ width: "calc(100% - 294px)" }}
                                placeholder="Semester"
                                value={rule.semester}
                                disabled={hasExistingGroupTiming}
                                onChange={(v) => {
                                  const next = rowRules.map((r, i) => (i === ruleIdx ? { ...r, semester: v } : r));
                                  setCoreRuleTimingRules(idx, next, groupCourseIds);
                                }}
                                options={semesterOptions}
                              />
                              <Button
                                danger
                                style={{ width: 76 }}
                                disabled={hasExistingGroupTiming}
                                onClick={() => {
                                  const next = rowRules.filter((_, i) => i !== ruleIdx);
                                  setCoreRuleTimingRules(idx, next, groupCourseIds);
                                }}
                              >
                                Delete
                              </Button>
                            </div>
                          );
                        })}
                        <Button
                          onClick={() => {
                            const rowRules = coreRuleTimingRules(row);
                            if (rowRules.some((r) => r.type === "FIXED")) return;
                            if (rowRules.length === 0) {
                              setCoreRuleTimingRules(idx, [{ type: "FIXED", semester: 1 }], groupCourseIds);
                              return;
                            }
                            if (!rowRules.some((r) => r.type === "MIN")) {
                              setCoreRuleTimingRules(idx, [...rowRules, { type: "MIN", semester: 1 }], groupCourseIds);
                              return;
                            }
                            if (!rowRules.some((r) => r.type === "MAX")) {
                              setCoreRuleTimingRules(idx, [...rowRules, { type: "MAX", semester: 1 }], groupCourseIds);
                            }
                          }}
                          disabled={!row.primary_course_id || hasExistingGroupTiming || coreRuleTimingRules(row).some((r) => r.type === "FIXED") || coreRuleTimingRules(row).length >= 2}
                        >
                          Add Timing Rule
                        </Button>
                      </Space>
                    </Card>
                  );
                })}
              </Space>
            </Card>
          ))}
        </Space>
      </Modal>
      <Card title="Requirements Designer (Integrated Tree)">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text type="secondary">
            Define reusable core/major/minor requirement nodes and map courses directly to each node.
          </Typography.Text>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
            <Space>
              <Select
                showSearch
                optionFilterProp="label"
                style={{ width: 320 }}
                value={rulesetFilter}
                onChange={setRulesetFilter}
                options={rulesetFilterOptions}
              />
              <Button size="small" onClick={() => setTreeExpandedKeys((filteredTree || []).map((n) => n.id))}>
                Expand Top
              </Button>
              <Button size="small" onClick={() => setTreeExpandedKeys(allRequirementKeys)}>
                Expand All
              </Button>
              <Button size="small" onClick={() => setTreeExpandedKeys([])}>
                Collapse All
              </Button>
            </Space>
            <Button type="primary" size="small" onClick={openNewRequirementEditor}>
              Add Node
            </Button>
          </div>
          <Tree
            treeData={integratedTreeData}
            draggable
            blockNode
            onDrop={handleTreeDrop}
            onSelect={handleRequirementTreeSelect}
            expandedKeys={treeExpandedKeys}
            onExpand={setTreeExpandedKeys}
            selectedKeys={selectedRuleNodeId ? [selectedRuleNodeId] : []}
          />
        </Space>
      </Card>
      <Card title="Validation Rules + Results">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text strong>Validation Dashboard (All Rules)</Typography.Text>
          <pre>{JSON.stringify(validationDashboardQ.data, null, 2)}</pre>
          <Space>
            <Typography.Text type="secondary">Rule Category</Typography.Text>
            <Select
              style={{ width: 260 }}
              value={validationDomainFilter}
              onChange={setValidationDomainFilter}
              options={[
                { value: "ALL", label: "All Rules" },
                { value: "PROGRAM_PATHWAY", label: "Program/Major Pathway" },
                { value: "CURRICULUM_INTEGRITY", label: "Curriculum Integrity" },
                { value: "RESOURCES", label: "Resources" },
                { value: "GENERAL", label: "General" },
              ]}
            />
          </Space>
          <Typography.Text strong>Create Rule</Typography.Text>
          <Space wrap>
            <Input style={{ width: 240 }} placeholder="Rule name" value={newRuleName} onChange={(e) => setNewRuleName(e.target.value)} />
            <Select
              style={{ width: 120 }}
              value={newRuleTier}
              onChange={setNewRuleTier}
              options={[
                { value: 1, label: "Tier 1" },
                { value: 2, label: "Tier 2" },
                { value: 3, label: "Tier 3" }
              ]}
            />
            <Select
              style={{ width: 140 }}
              value={newRuleSeverity}
              onChange={setNewRuleSeverity}
              options={[
                { value: "WARNING", label: "WARNING" },
                { value: "FAIL", label: "FAIL" }
              ]}
            />
            <Select
              style={{ width: 120 }}
              value={newRuleActive}
              onChange={setNewRuleActive}
              options={[
                { value: "YES", label: "Active" },
                { value: "NO", label: "Inactive" }
              ]}
            />
            <Input.TextArea
              style={{ width: 380 }}
              rows={4}
              placeholder='{"key":"value"}'
              value={newRuleConfig}
              onChange={(e) => setNewRuleConfig(e.target.value)}
            />
            <Button onClick={createValidationRule}>Create</Button>
          </Space>
          <Typography.Text strong>Edit Selected Rule</Typography.Text>
          <Space wrap>
            <Select
              style={{ width: 320 }}
              placeholder="Select rule"
              value={editRuleId}
              onChange={loadRuleForEdit}
              options={(validationRulesQ.data || []).map((r) => ({ value: r.id, label: `${r.name} (T${r.tier})` }))}
            />
            <Select
              style={{ width: 120 }}
              value={editRuleTier}
              onChange={setEditRuleTier}
              options={[
                { value: 1, label: "Tier 1" },
                { value: 2, label: "Tier 2" },
                { value: 3, label: "Tier 3" }
              ]}
            />
            <Select
              style={{ width: 140 }}
              value={editRuleSeverity}
              onChange={setEditRuleSeverity}
              options={[
                { value: "WARNING", label: "WARNING" },
                { value: "FAIL", label: "FAIL" }
              ]}
            />
            <Select
              style={{ width: 120 }}
              value={editRuleActive}
              onChange={setEditRuleActive}
              options={[
                { value: "YES", label: "Active" },
                { value: "NO", label: "Inactive" }
              ]}
            />
            <Input.TextArea style={{ width: 380 }} rows={4} value={editRuleConfig} onChange={(e) => setEditRuleConfig(e.target.value)} />
            <Button onClick={updateValidationRule}>Update</Button>
          </Space>
          <Table
            size="small"
            rowKey="id"
            dataSource={filteredValidationRules}
            pagination={{ pageSize: 6 }}
            columns={[
              { title: "Name", dataIndex: "name" },
              {
                title: "Category",
                dataIndex: "domain",
                render: (v) => {
                  if (v === "PROGRAM_PATHWAY") return "Program/Major Pathway";
                  if (v === "CURRICULUM_INTEGRITY") return "Curriculum Integrity";
                  if (v === "RESOURCES") return "Resources";
                  return "General";
                },
              },
              { title: "Tier", dataIndex: "tier" },
              { title: "Severity", dataIndex: "severity" },
              { title: "Active", dataIndex: "active", render: (v) => (v ? "Yes" : "No") },
              {
                title: "Actions",
                render: (_, r) => (
                  <Space>
                    <Button size="small" onClick={() => toggleValidationRule(r.id, r.active)}>
                      {r.active ? "Disable" : "Enable"}
                    </Button>
                    <Button size="small" danger onClick={() => deleteValidationRule(r.id)}>
                      Delete
                    </Button>
                  </Space>
                )
              }
            ]}
          />
        </Space>
      </Card>
      <Card title="Prerequisite Graph">
        <Table
          size="small"
          pagination={{ pageSize: 8 }}
          rowKey="id"
          dataSource={prereqGraphQ.data?.edges || []}
          columns={[
            { title: "From", dataIndex: "from", render: (v) => nodeLabel[v] || v },
            { title: "To", dataIndex: "to", render: (v) => nodeLabel[v] || v },
            { title: "Type", dataIndex: "relationship_type" },
            { title: "Enforcement", dataIndex: "enforcement" }
          ]}
        />
      </Card>
      <Card title="Version Comparison">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Space>
            <Select
              style={{ width: 260 }}
              placeholder="From version"
              value={compareFrom}
              onChange={setCompareFrom}
              options={(versionsQ.data || []).map((v) => ({ value: v.id, label: `${v.name} (${v.status})` }))}
            />
            <Select
              style={{ width: 260 }}
              placeholder="To version"
              value={compareTo}
              onChange={setCompareTo}
              options={(versionsQ.data || []).map((v) => ({ value: v.id, label: `${v.name} (${v.status})` }))}
            />
          </Space>
          <pre>{JSON.stringify(versionDiffQ.data, null, 2)}</pre>
        </Space>
      </Card>
      <Card title="Course Detail Editor">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Select
            style={{ width: 360 }}
            placeholder="Select course"
            value={selectedCourseId}
            onChange={setSelectedCourseId}
            options={(coursesQ.data || []).map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
          />
          <Tabs
            items={[
              {
                key: "general",
                label: "General",
                children: <pre>{JSON.stringify(courseDetailQ.data?.general, null, 2)}</pre>
              },
              {
                key: "scheduling",
                label: "Scheduling",
                children: <pre>{JSON.stringify(courseDetailQ.data?.scheduling, null, 2)}</pre>
              },
              {
                key: "prereq",
                label: "Prerequisites",
                children: (
                  <Space direction="vertical" style={{ width: "100%" }}>
                    <Space wrap>
                      <Select
                        style={{ width: 260 }}
                        placeholder="Required course"
                        value={newPrereqId}
                        onChange={setNewPrereqId}
                        options={(coursesQ.data || [])
                          .filter((c) => c.id !== selectedCourseId)
                          .map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
                      />
                      <Select
                        style={{ width: 180 }}
                        value={newPrereqType}
                        onChange={setNewPrereqType}
                        options={[
                          { value: "PREREQUISITE", label: "PREREQUISITE" },
                          { value: "COREQUISITE", label: "COREQUISITE" }
                        ]}
                      />
                      <Select
                        style={{ width: 140 }}
                        value={newPrereqEnforcement}
                        onChange={setNewPrereqEnforcement}
                        options={[
                          { value: "HARD", label: "HARD" },
                          { value: "SOFT", label: "SOFT" }
                        ]}
                      />
                      <Button onClick={addPrerequisite}>Add Link</Button>
                    </Space>
                    <pre>{JSON.stringify(courseDetailQ.data?.prerequisites, null, 2)}</pre>
                  </Space>
                )
              },
              {
                key: "requirements",
                label: "Requirements",
                children: (
                  <Space direction="vertical" style={{ width: "100%" }}>
                    <Space wrap>
                      <Select
                        style={{ width: 360 }}
                        placeholder="Requirement to link"
                        value={linkRequirementId}
                        onChange={setLinkRequirementId}
                        options={(requirementsTreeQ.data?.tree || []).map((r) => ({ value: r.id, label: r.name }))}
                      />
                      <Button onClick={linkRequirementToCourse}>Link Requirement</Button>
                    </Space>
                    <pre>{JSON.stringify(courseFulfillmentQ.data, null, 2)}</pre>
                  </Space>
                )
              },
              {
                key: "subs",
                label: "Substitutions",
                children: (
                  <Space direction="vertical" style={{ width: "100%" }}>
                    <Space wrap>
                      <Select
                        style={{ width: 280 }}
                        placeholder="Substitute course"
                        value={substituteCourseId}
                        onChange={setSubstituteCourseId}
                        options={(coursesQ.data || [])
                          .filter((c) => c.id !== selectedCourseId)
                          .map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
                      />
                      <Select
                        style={{ width: 180 }}
                        value={subBidirectional ? "YES" : "NO"}
                        onChange={(v) => setSubBidirectional(v === "YES")}
                        options={[
                          { value: "NO", label: "One-Way" },
                          { value: "YES", label: "Bidirectional" }
                        ]}
                      />
                      <Select
                        style={{ width: 180 }}
                        value={subRequiresApproval ? "YES" : "NO"}
                        onChange={(v) => setSubRequiresApproval(v === "YES")}
                        options={[
                          { value: "NO", label: "No Approval" },
                          { value: "YES", label: "Requires Approval" }
                        ]}
                      />
                      <Button onClick={addSubstitution}>Add Substitution</Button>
                    </Space>
                    <pre>{JSON.stringify(substitutionsQ.data, null, 2)}</pre>
                  </Space>
                )
              },
              {
                key: "resources",
                label: "Resources",
                children: <pre>{JSON.stringify(courseDetailQ.data?.resources, null, 2)}</pre>
              },
              {
                key: "history",
                label: "History",
                children: (
                  <List
                    size="small"
                    dataSource={(auditQ.data || []).filter((a) => a.entity_type === "Course" && a.entity_id === selectedCourseId)}
                    renderItem={(a) => <List.Item>{a.created_at} | {a.action} | {a.payload || ""}</List.Item>}
                  />
                )
              }
            ]}
          />
        </Space>
      </Card>
      <Card title="Collaboration: Comments">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Input.TextArea
            rows={2}
            value={commentText}
            onChange={(e) => setCommentText(e.target.value)}
            placeholder="Add design annotation"
          />
          <Button onClick={submitComment}>Add Comment</Button>
          <pre>{JSON.stringify(commentsQ.data, null, 2)}</pre>
        </Space>
      </Card>
      <Card title="Change Requests">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Input value={changeTitle} onChange={(e) => setChangeTitle(e.target.value)} placeholder="Change request title" />
          <Button onClick={submitChange}>Propose Change</Button>
          <pre>{JSON.stringify(changesQ.data, null, 2)}</pre>
        </Space>
      </Card>
      <Card title="Cadet Gap Analysis">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Select
            allowClear
            style={{ width: 360 }}
            placeholder="Select cadet"
            value={selectedCadetId}
            onChange={setSelectedCadetId}
            options={(cadetsQ.data || []).map((c) => ({ value: c.id, label: `${c.name} (C/O ${c.class_year})` }))}
          />
          <pre>{JSON.stringify(gapQ.data, null, 2)}</pre>
        </Space>
      </Card>
      <Card title="Transition Planner">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Space wrap>
            <Input
              style={{ width: 160 }}
              placeholder="Class Year"
              value={transitionClassYear}
              onChange={(e) => setTransitionClassYear(e.target.value)}
            />
            <Button onClick={assignCohort}>Assign Active Version To Cohort</Button>
          </Space>
          <pre>{JSON.stringify(cohortQ.data, null, 2)}</pre>
          <Space wrap>
            <Select
              style={{ width: 240 }}
              placeholder="From Version"
              value={transitionFromVersion}
              onChange={setTransitionFromVersion}
              options={(versionsQ.data || []).map((v) => ({ value: v.id, label: `${v.name} (${v.status})` }))}
            />
            <Select
              style={{ width: 240 }}
              placeholder="To Version"
              value={transitionToVersion}
              onChange={setTransitionToVersion}
              options={(versionsQ.data || []).map((v) => ({ value: v.id, label: `${v.name} (${v.status})` }))}
            />
            <Select
              style={{ width: 260 }}
              placeholder="From Course"
              value={transitionFromCourse}
              onChange={setTransitionFromCourse}
              options={(coursesQ.data || []).map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
            />
            <Select
              style={{ width: 260 }}
              placeholder="To Course"
              value={transitionToCourse}
              onChange={setTransitionToCourse}
              options={(coursesQ.data || []).map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
            />
            <Button onClick={addEquivalency}>Add Equivalency</Button>
          </Space>
          <pre>{JSON.stringify(equivalencyQ.data, null, 2)}</pre>
          <Typography.Text strong>Transition Impact</Typography.Text>
          <pre>{JSON.stringify(transitionImpactQ.data, null, 2)}</pre>
        </Space>
      </Card>
    </Space>
  );
}
