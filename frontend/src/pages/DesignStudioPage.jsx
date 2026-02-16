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
  const [reqCourseIds, setReqCourseIds] = useState([]);
  const [treeExpandedKeys, setTreeExpandedKeys] = useState([]);
  const [reqEditorOpen, setReqEditorOpen] = useState(false);
  const [viewMode, setViewMode] = useState("GENERIC_CORE");
  const [viewProgramId, setViewProgramId] = useState();
  const [compareVersionForView, setCompareVersionForView] = useState();
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
  const canvasViewQ = useQuery({
    queryKey: ["canvas-view", selectedVersion?.id, viewMode, viewProgramId, compareVersionForView],
    enabled: !!selectedVersion?.id,
    queryFn: () => {
      const params = new URLSearchParams({ mode: viewMode });
      if (viewProgramId) params.set("program_id", viewProgramId);
      if (compareVersionForView) params.set("compare_version_id", compareVersionForView);
      return authed(`/design/canvas/${selectedVersion.id}/view?${params.toString()}`);
    }
  });
  const impactQ = useQuery({
    queryKey: ["impact", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/impact/${selectedVersion.id}`)
  });
  const impactDetailQ = useQuery({
    queryKey: ["impact-detail", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/impact-analysis/${selectedVersion.id}`)
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

  async function move(planItemId, targetSemester, targetPosition = 0) {
    await authed("/design/canvas/move", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ plan_item_id: planItemId, target_semester: targetSemester, target_position: targetPosition })
    });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["canvas"] }),
      qc.invalidateQueries({ queryKey: ["impact"] }),
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
      qc.invalidateQueries({ queryKey: ["impact", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["canvas-view", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
    ]);
  }

  async function removeFromCanvas(planItemId) {
    await authed(`/design/canvas/${encodeURIComponent(planItemId)}`, { method: "DELETE" });
    if (!selectedVersion?.id) return;
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["canvas", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["impact", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["canvas-view", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion.id] }),
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
    async function resolveProgramId(category, majorName, division) {
      if (category !== "MAJOR") return null;
      const name = (majorName || "").trim();
      if (!name) return null;
      const existing = (programsQ.data || []).find((p) => p.name.toLowerCase() === name.toLowerCase() && p.program_type === "MAJOR");
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
        body: JSON.stringify({ version_id: selectedVersion.id, name, program_type: "MAJOR", division: division || null }),
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
      window.alert("Subnodes are only allowed under top-level Core/Major/PE nodes.");
      return;
    }
    if (parent) {
      const parentCategory = String(parent.category || "CORE").toUpperCase();
      setReqLockedCategory(parentCategory);
      setEditReqCategory(parentCategory);
      if (parentCategory === "MAJOR") {
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

  function openRequirementCourseModal(requirementId) {
    setReqCourseRequirementId(requirementId);
    setReqCourseIds([]);
    setReqCourseModalOpen(true);
  }

  async function linkCoursesToRequirementNode() {
    if (!reqCourseRequirementId || !(reqCourseIds || []).length) return;
    for (const courseId of reqCourseIds) {
      await authed("/requirements/fulfillment", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ requirement_id: reqCourseRequirementId, course_id: courseId, is_primary: true }),
      });
    }
    setReqCourseIds([]);
    setReqCourseModalOpen(false);
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["requirement-fulfillment-version", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["impact-detail"] }),
    ]);
  }

  async function updateRulesetRequirement() {
    if (!selectedRuleNodeId || !selectedVersion?.id) return;
    if (editReqCategory === "MAJOR" && !(editReqMajorName || "").trim()) return;
    async function resolveProgramId(category, majorName, division) {
      if (category !== "MAJOR") return null;
      const name = (majorName || "").trim();
      if (!name) return null;
      const existing = (programsQ.data || []).find((p) => p.name.toLowerCase() === name.toLowerCase() && p.program_type === "MAJOR");
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
        body: JSON.stringify({ version_id: selectedVersion.id, name, program_type: "MAJOR", division: division || null }),
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
        return `Major - ${(editReqMajorName || "Unspecified").trim()}`;
      }
      if (editReqCategory === "MAJOR") {
        if (editReqMajorMode === "TRACK") return `Track - ${(editReqTrackName || "").trim()}: ${logicTxt}`;
        return `Major Requirement: ${logicTxt}`;
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
      major_mode: editReqCategory === "MAJOR" || (editReqCategory === "CORE" && isSubNode) ? editReqMajorMode : null,
      track_name:
        editReqCategory === "CORE"
          ? editReqMajorMode === "TRACK"
            ? (editReqCoreTrack || "").trim() || null
            : null
          : editReqCategory === "MAJOR" && editReqMajorMode === "TRACK"
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
      if (editReqCategory === "MAJOR" && !(editReqMajorName || "").trim()) return;
      if (isSubNodeCreate && editReqCategory === "MAJOR" && editReqMajorMode === "TRACK" && !(editReqTrackName || "").trim()) return;
      if (isSubNodeCreate && editReqCategory === "CORE" && editReqMajorMode === "TRACK" && !(editReqCoreTrack || "").trim()) return;
      async function resolveProgramId(category, majorName, division) {
        if (category !== "MAJOR") return null;
        const name = (majorName || "").trim();
        if (!name) return null;
        const existing = (programsQ.data || []).find((p) => p.name.toLowerCase() === name.toLowerCase() && p.program_type === "MAJOR");
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
          body: JSON.stringify({ version_id: selectedVersion.id, name, program_type: "MAJOR", division: division || null }),
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
          return `Major - ${editReqMajorName.trim()}`;
        }
        if (editReqCategory === "MAJOR") {
          if (editReqMajorMode === "TRACK") return `Track - ${(editReqTrackName || "").trim()}: ${logicTxt}`;
          return `Major Requirement: ${logicTxt}`;
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
          if (editReqCategory === "MAJOR") {
            if (resolvedProgramId && r.program_id === resolvedProgramId) return true;
            const rProgramName = (programsQ.data || []).find((p) => p.id === r.program_id)?.name || "";
            return normalize(rProgramName) === normalize(editReqMajorName);
          }
          return true;
        }

        if (editReqCategory === "MAJOR") {
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
        major_mode: editReqCategory === "MAJOR" || (editReqCategory === "CORE" && isSubNodeCreate) ? editReqMajorMode : null,
        track_name:
          editReqCategory === "CORE"
            ? editReqMajorMode === "TRACK"
              ? (editReqCoreTrack || "").trim() || null
              : null
            : editReqCategory === "MAJOR" && editReqMajorMode === "TRACK"
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
      qc.invalidateQueries({ queryKey: ["impact-detail"] }),
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
      qc.invalidateQueries({ queryKey: ["validation"] })
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
      qc.invalidateQueries({ queryKey: ["impact-detail"] })
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
    await Promise.all([qc.invalidateQueries({ queryKey: ["validation-rules"] }), qc.invalidateQueries({ queryKey: ["validation"] })]);
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
    await Promise.all([qc.invalidateQueries({ queryKey: ["validation-rules"] }), qc.invalidateQueries({ queryKey: ["validation"] })]);
  }

  async function toggleValidationRule(ruleId, isActive) {
    await authed(`/design/validation-rules/${ruleId}/toggle?active=${isActive ? "false" : "true"}`, { method: "POST" });
    await Promise.all([qc.invalidateQueries({ queryKey: ["validation-rules"] }), qc.invalidateQueries({ queryKey: ["validation"] })]);
  }

  async function deleteValidationRule(ruleId) {
    await authed(`/design/validation-rules/${ruleId}`, { method: "DELETE" });
    if (editRuleId === ruleId) setEditRuleId(undefined);
    await Promise.all([qc.invalidateQueries({ queryKey: ["validation-rules"] }), qc.invalidateQueries({ queryKey: ["validation"] })]);
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
    const category = (raw?.category || n?.category || (raw?.program_id || n?.program_id ? "MAJOR" : "CORE")).toUpperCase();
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
      });
    }
    function mapNode(n) {
      const canAddSubNode = !n.parent_requirement_id;
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
      const sourceCourses = (n.courses && n.courses.length ? n.courses : mappedByReq[n.id] || []);
      const courseLeaves = sourceCourses.map((c) => ({
        key: `course:${c.id}`,
        title: (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, width: "100%" }}>
            <span>{c.course_number || "Course"}</span>
            <Button
              size="small"
              danger
              style={{ marginLeft: "auto" }}
              onClick={(e) => {
                e.stopPropagation();
                removeCourseFromRequirement(c.id, n.id);
              }}
            >
              Unlink
            </Button>
          </div>
        ),
        isLeaf: true,
        disableCheckbox: true,
        selectable: false,
      }));
      reqNode.children = [...courseLeaves, ...childReqs];
      return reqNode;
    }
    return filteredTree.map(mapNode);
  }, [filteredTree, requirementFulfillmentVersionQ.data]);
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
  const rulesetFilterOptions = useMemo(() => {
    const coreTracks = Array.from(
      new Set(
        Object.values(requirementNodeMap)
          .filter((n) => (n.category || "").toUpperCase() === "CORE" && n.track_name)
          .map((n) => n.track_name)
      )
    ).sort();
    const majors = (programsQ.data || []).filter((p) => p.program_type === "MAJOR");
    const minors = (programsQ.data || []).filter((p) => p.program_type === "MINOR");
    const divisionList = [
      { value: "DIVISION:BASIC_SCIENCES_AND_MATH", label: "Division - Basic Sciences and Math" },
      { value: "DIVISION:ENGINEERING_SCIENCES", label: "Division - Engineering Sciences" },
      { value: "DIVISION:HUMANITIES", label: "Division - Humanities" },
      { value: "DIVISION:SOCIAL_SCIENCES", label: "Division - Social Sciences" },
    ];
    const other = [
      { value: "CORE_ALL", label: "Core - All" },
      ...coreTracks.map((t) => ({ value: `CORE_TRACK:${t}`, label: `Core - ${t}` })),
      ...divisionList,
      { value: "MAJOR_ALL", label: "Majors - All" },
      ...majors.map((p) => ({ value: `MAJOR:${p.id}`, label: `Major - ${p.name}` })),
      { value: "MINOR_ALL", label: "Minors - All" },
      ...minors.map((p) => ({ value: `MINOR:${p.id}`, label: `Minor - ${p.name}` })),
      { value: "PE", label: "PE" },
    ].sort((a, b) => a.label.localeCompare(b.label));
    return [{ value: "ALL", label: "No Filter" }, ...other];
  }, [programsQ.data, requirementNodeMap]);

  useEffect(() => {
    const f = canvasFilter || "ALL";
    if (f.startsWith("MAJOR:") || f.startsWith("MINOR:")) {
      setViewMode("MAJOR_SPECIFIC");
      setViewProgramId(f.split(":")[1] || undefined);
      return;
    }
    if (f.startsWith("DIVISION:")) {
      setViewMode("WING_AGGREGATE");
      setViewProgramId(undefined);
      return;
    }
    setViewMode("GENERIC_CORE");
    setViewProgramId(undefined);
  }, [canvasFilter]);

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

  const isReqEditMode = !!selectedRuleNodeId;
  const isSubNodeCreate = !isReqEditMode && reqScopeLocked && !!editReqParentId;
  const isEditingSubNode = isReqEditMode && !!editReqParentId;

  function renderChecklist(nodes, level = 0) {
    return (nodes || []).map((r) => (
      <div key={`${r.requirement_id}-${level}`} style={{ marginLeft: level * 14, marginBottom: 6 }}>
        <Space>
          <Tag color={r.is_satisfied ? "green" : "red"}>{r.is_satisfied ? "Met" : "Open"}</Tag>
          <Typography.Text strong={level === 0}>{r.name}</Typography.Text>
          <Typography.Text type="secondary">
            {r.satisfied_units}/{r.required_units}
          </Typography.Text>
        </Space>
        {r.children?.length ? <div>{renderChecklist(r.children, level + 1)}</div> : null}
      </div>
    ));
  }

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
            <Select
              allowClear
              style={{ width: 220 }}
              placeholder="Compare version (for Comparison)"
              value={compareVersionForView}
              onChange={setCompareVersionForView}
              options={(versionsQ.data || []).map((v) => ({ value: v.id, label: `${v.name} (${v.status})` }))}
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
                      <Space style={{ width: "100%", justifyContent: "space-between" }}>
                        <Typography.Text strong>{course.course_number}</Typography.Text>
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
            options={(programsQ.data || []).map((p) => ({ value: p.id, label: `${p.name} (${p.program_type})` }))}
          />
          <Typography.Text type="secondary">
            Requirements green up as the canvas satisfies them (including substitution credit).
          </Typography.Text>
          <Space>
            <Tag color="blue">Completion {checklistQ.data?.summary?.completion_percent ?? 0}%</Tag>
            <Typography.Text type="secondary">
              {checklistQ.data?.summary?.top_level_satisfied ?? 0}/{checklistQ.data?.summary?.top_level_total ?? 0} top-level requirements met
            </Typography.Text>
          </Space>
          <div>{renderChecklist(checklistQ.data?.requirements || [])}</div>
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
              { value: "PE", label: "PE" },
            ]}
          />
          {editReqCategory === "MAJOR" && (
            <AutoComplete
              style={{ width: "100%" }}
              options={majorNameOptions}
              value={editReqMajorName}
              onChange={setEditReqMajorName}
              disabled={reqScopeLocked && reqLockedCategory === "MAJOR"}
              placeholder="Major (search or type new)"
              filterOption={(inputValue, option) => (option?.value || "").toLowerCase().includes(inputValue.toLowerCase())}
            />
          )}
          {editReqCategory === "MAJOR" && !isSubNodeCreate && !isEditingSubNode && (
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
          {editReqCategory === "MAJOR" && (isSubNodeCreate || isReqEditMode) && (
            <Select
              style={{ width: "100%" }}
              value={editReqMajorMode}
              onChange={setEditReqMajorMode}
              options={[
                { value: "REQUIREMENT", label: "Major Requirement" },
                { value: "TRACK", label: "Major Track" },
              ]}
            />
          )}
          {editReqCategory === "MAJOR" && (isSubNodeCreate || isReqEditMode) && editReqMajorMode === "TRACK" && (
            <AutoComplete
              style={{ width: "100%" }}
              options={majorTrackOptions}
              value={editReqTrackName}
              onChange={setEditReqTrackName}
              placeholder="Major track (search or type new)"
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
        onCancel={() => setReqCourseModalOpen(false)}
        onOk={linkCoursesToRequirementNode}
        okText="Link Course(s)"
      >
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text type="secondary">
            {requirementFullPathMap[reqCourseRequirementId || ""] || requirementNodeMap[reqCourseRequirementId || ""]?.name || "None"}
          </Typography.Text>
          <Select
            mode="multiple"
            allowClear
            showSearch
            optionFilterProp="label"
            style={{ width: "100%" }}
            placeholder="Select course(s)"
            value={reqCourseIds}
            onChange={setReqCourseIds}
            options={(coursesQ.data || []).map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
          />
        </Space>
      </Modal>
      <Card title="Impact Analysis">
        <pre>{JSON.stringify(impactQ.data, null, 2)}</pre>
      </Card>
      <Card title="Detailed Impact Analysis">
        <pre>{JSON.stringify(impactDetailQ.data, null, 2)}</pre>
      </Card>
      <Card title="Canvas View Mode Output">
        <pre>{JSON.stringify(canvasViewQ.data, null, 2)}</pre>
      </Card>
      <Card title="Validation Status">
        <pre>{JSON.stringify(validationQ.data, null, 2)}</pre>
      </Card>
      <Card title="Validation Dashboard">
        <pre>{JSON.stringify(validationDashboardQ.data, null, 2)}</pre>
      </Card>
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
      <Card title="Validation Rule Editor">
        <Space direction="vertical" style={{ width: "100%" }}>
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
            <Input style={{ width: 280 }} placeholder='{"key":"value"}' value={newRuleConfig} onChange={(e) => setNewRuleConfig(e.target.value)} />
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
            <Input style={{ width: 280 }} value={editRuleConfig} onChange={(e) => setEditRuleConfig(e.target.value)} />
            <Button onClick={updateValidationRule}>Update</Button>
          </Space>
          <Table
            size="small"
            rowKey="id"
            dataSource={validationRulesQ.data || []}
            pagination={{ pageSize: 6 }}
            columns={[
              { title: "Name", dataIndex: "name" },
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
