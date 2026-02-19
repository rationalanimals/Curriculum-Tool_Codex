import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AutoComplete, Button, Card, Input, InputNumber, List, Modal, Select, Space, Switch, Table, Tabs, Tag, Tooltip, Tree, Typography } from "antd";

const API = "http://127.0.0.1:8000";
const DEFAULT_PERIOD_ROWS = [
  { index: 16, short_label: "S7", label: "Semester 7", kind: "ACADEMIC", layout_row: 0, layout_col: 0 },
  { index: 17, short_label: "S8", label: "Semester 8", kind: "ACADEMIC", layout_row: 0, layout_col: 1 },
  { index: 18, short_label: "SU4P1", label: "Summer 4 Period 1", kind: "SUMMER", layout_row: 0, layout_col: 2 },
  { index: 19, short_label: "SU4P2", label: "Summer 4 Period 2", kind: "SUMMER", layout_row: 0, layout_col: 3 },
  { index: 20, short_label: "SU4P3", label: "Summer 4 Period 3", kind: "SUMMER", layout_row: 0, layout_col: 4 },
  { index: 11, short_label: "S5", label: "Semester 5", kind: "ACADEMIC", layout_row: 1, layout_col: 0 },
  { index: 12, short_label: "S6", label: "Semester 6", kind: "ACADEMIC", layout_row: 1, layout_col: 1 },
  { index: 13, short_label: "SU3P1", label: "Summer 3 Period 1", kind: "SUMMER", layout_row: 1, layout_col: 2 },
  { index: 14, short_label: "SU3P2", label: "Summer 3 Period 2", kind: "SUMMER", layout_row: 1, layout_col: 3 },
  { index: 15, short_label: "SU3P3", label: "Summer 3 Period 3", kind: "SUMMER", layout_row: 1, layout_col: 4 },
  { index: 6, short_label: "S3", label: "Semester 3", kind: "ACADEMIC", layout_row: 2, layout_col: 0 },
  { index: 7, short_label: "S4", label: "Semester 4", kind: "ACADEMIC", layout_row: 2, layout_col: 1 },
  { index: 8, short_label: "SU2P1", label: "Summer 2 Period 1", kind: "SUMMER", layout_row: 2, layout_col: 2 },
  { index: 9, short_label: "SU2P2", label: "Summer 2 Period 2", kind: "SUMMER", layout_row: 2, layout_col: 3 },
  { index: 10, short_label: "SU2P3", label: "Summer 2 Period 3", kind: "SUMMER", layout_row: 2, layout_col: 4 },
  { index: 1, short_label: "S1", label: "Semester 1", kind: "ACADEMIC", layout_row: 3, layout_col: 0 },
  { index: 2, short_label: "S2", label: "Semester 2", kind: "ACADEMIC", layout_row: 3, layout_col: 1 },
  { index: 3, short_label: "SU1P1", label: "Summer 1 Period 1", kind: "SUMMER", layout_row: 3, layout_col: 2 },
  { index: 4, short_label: "SU1P2", label: "Summer 1 Period 2", kind: "SUMMER", layout_row: 3, layout_col: 3 },
  { index: 5, short_label: "SU1P3", label: "Summer 1 Period 3", kind: "SUMMER", layout_row: 3, layout_col: 4 },
  { index: 0, short_label: "SU0", label: "Summer 0", kind: "SUMMER", layout_row: 4, layout_col: 4 },
];
const CANVAS_GRID = [
  [16, 17, 18, 19, 20],
  [11, 12, 13, 14, 15],
  [6, 7, 8, 9, 10],
  [1, 2, 3, 4, 5],
  [null, null, null, null, 0],
];
const VALIDATION_DOMAIN_ORDER = [
  "Residency and Graduation",
  "Non-Academic Graduation",
  "Program/Major Pathway",
  "Accreditation",
  "Curriculum Integrity",
  "Resources",
  "General",
  "Definitional",
  "Cadet Performance",
];
const VALIDATION_RULE_JSON_GUIDE = [
  {
    title: "Category only",
    description: "Assign the rule to a category group in the Validation Rules tree.",
    json: '{"domain":"General"}',
  },
  {
    title: "Cadet Performance category",
    description: "Advisor-mode category for cadet-specific policy rules (not applied in Design Studio feasibility/checklist).",
    json: '{"domain":"Cadet Performance"}',
  },
  {
    title: "Program feasibility gate",
    description: "Flags infeasible selected major/minor combinations from Program Feasibility analysis.",
    json: '{"domain":"Program/Major Pathway","type":"PROGRAM_FEASIBILITY_GATE"}',
  },
  {
    title: "Residency minimum in-residence hours",
    description: "Minimum total in-residence credit hours required in the canvas plan.",
    json: '{"domain":"Residency and Graduation","type":"RESIDENCY_MIN_HOURS","min_hours":125,"applies_to":["COURSE_OF_STUDY","GLOBAL_VALIDATION"]}',
  },
  {
    title: "Residency minimum academic semesters",
    description: "Minimum number of loaded academic semesters required in the plan.",
    json: '{"domain":"Residency and Graduation","type":"RESIDENCY_MIN_ACADEMIC_SEMESTERS","min_semesters":8,"applies_to":["COURSE_OF_STUDY","GLOBAL_VALIDATION"]}',
  },
  {
    title: "Rule scope (`applies_to`)",
    description: "Optional context scope for a validation rule: COURSE_OF_STUDY, PROGRAM_FEASIBILITY, GLOBAL_VALIDATION.",
    json: '{"domain":"General","applies_to":["COURSE_OF_STUDY"]}',
  },
  {
    title: "Semester credit upper bound",
    description: "Caps academic semester load. Optional summer cap can also be set here.",
    json: '{"domain":"Curriculum Integrity","max_credits":24,"max_credits_per_summer_period":9}',
  },
  {
    title: "Minimum section size",
    description: "Checks that sections meet minimum enrollment threshold.",
    json: '{"domain":"Resources","minimum":6}',
  },
  {
    title: "Prerequisite ordering",
    description: "Uses prerequisite graph to check ordering violations in the canvas.",
    json: '{"domain":"Curriculum Integrity"}',
  },
  {
    title: "Instructor load limits",
    description: "Checks instructor-assigned sections against load constraints.",
    json: '{"domain":"Resources"}',
  },
  {
    title: "Classroom capacity constraints",
    description: "Checks section enrollment and assignment against room capacity.",
    json: '{"domain":"Resources"}',
  },
  {
    title: "Instructor qualification constraints",
    description: "Checks assigned instructors against qualification links for each course.",
    json: '{"domain":"Resources"}',
  },
  {
    title: "ABET placeholder",
    description: "Program-specific accreditation placeholder configuration.",
    json: '{"domain":"Accreditation","type":"abet_placeholder","program":"Computer Science"}',
  },
  {
    title: "ABET EAC Math/Basic Science minimum",
    description: "Checks minimum ABET EAC math/basic-science credits from course bucket tags.",
    json: '{"domain":"Accreditation","type":"ABET_EAC_MATH_BASIC_SCI_MIN","bucket_code":"ABET_MATH_BASIC_SCI","min_credits":30,"program_names":["Aeronautical Engineering"]}',
  },
  {
    title: "ABET EAC Engineering Topics minimum",
    description: "Checks minimum ABET EAC engineering-topics credits from course bucket tags.",
    json: '{"domain":"Accreditation","type":"ABET_EAC_ENGINEERING_TOPICS_MIN","bucket_code":"ABET_ENGINEERING_TOPICS","min_credits":45,"program_names":["Aeronautical Engineering"]}',
  },
  {
    title: "Program/Major Pathway: Minor minimum courses",
    description: "Checks minimum number of courses in each selected minor.",
    json: '{"domain":"Program/Major Pathway","type":"MINOR_MIN_COURSES","min_courses":5}',
  },
  {
    title: "Program/Major Pathway: Minor minimum hours",
    description: "Checks minimum credit hours in each selected minor.",
    json: '{"domain":"Program/Major Pathway","type":"MINOR_MIN_HOURS","min_hours":15}',
  },
  {
    title: "Program/Major Pathway: Minor upper-level minimum",
    description: "Checks minimum count of upper-level courses in each selected minor.",
    json: '{"domain":"Program/Major Pathway","type":"MINOR_MIN_UPPER_LEVEL_COURSES","min_count":3,"min_level":300}',
  },
  {
    title: "Program/Major Pathway: Double major divisional separation",
    description: "Checks selected double-major combinations that require separate divisions for divisional majors.",
    json: '{"domain":"Program/Major Pathway","type":"DOUBLE_MAJOR_DIVISION_SEPARATION"}',
  },
  {
    title: "Program/Major Pathway: Double major additional hours minimum",
    description: "Checks required additional non-core hours beyond the larger major in double-major combinations.",
    json: '{"domain":"Program/Major Pathway","type":"DOUBLE_MAJOR_ADDITIONAL_HOURS_MIN","min_additional_hours":12}',
  },
  {
    title: "Program/Major Pathway Definition: Upper-level course threshold",
    description: "Defines what course number qualifies as upper-level for pathway checks.",
    json: '{"domain":"Definitional","type":"DEF_UPPER_LEVEL_COURSE_NUMBER","min_level":300}',
  },
  {
    title: "Non-Academic bucket minimum credits",
    description: "Checks minimum credits for a tagged non-academic bucket.",
    json: '{"domain":"Non-Academic Graduation","type":"BUCKET_MIN_CREDITS","bucket_code":"NONACAD_PE","min_credits":5}',
  },
  {
    title: "Non-Academic bucket minimum courses",
    description: "Checks minimum distinct tagged courses for a non-academic bucket.",
    json: '{"domain":"Non-Academic Graduation","type":"BUCKET_MIN_COURSES","bucket_code":"NONACAD_LEADERSHIP","min_courses":4}',
  },
  {
    title: "Domain-local display order",
    description: "Optional ordering key used when dragging rules within a category.",
    json: '{"domain":"General","domain_order":1}',
  },
];

async function authed(path, opts = {}) {
  const token = localStorage.getItem("session_token") || "";
  const withToken = `${path}${path.includes("?") ? "&" : "?"}session_token=${encodeURIComponent(token)}`;
  const r = await fetch(`${API}${withToken}`, opts);
  if (!r.ok) {
    let detail = "";
    try {
      const payload = await r.json();
      detail = payload?.detail ? `: ${payload.detail}` : "";
    } catch {
      detail = "";
    }
    throw new Error(`Request failed (${r.status})${detail}`);
  }
  return r.json();
}

function withDot(code) {
  const t = String(code || "").trim();
  if (!t) return "";
  return t.endsWith(".") ? t : `${t}.`;
}

function formatRequirementName(name, logicType, pickN, optionTotal) {
  let raw = String(name || "");
  const logic = String(logicType || "").toUpperCase();
  const n = Number(pickN || 0);
  const total = Number(optionTotal || 0);
  raw = raw.replace(/\bAny One\b/gi, "Pick 1");
  const isTopProgramNode = /^\s*(Core|PE|Major\s*-|Minor\s*-)/i.test(raw);
  if (isTopProgramNode && logic === "ALL_REQUIRED") {
    return raw
      .replace(/\s*:\s*All\s*Required(?:\s*\/\s*\d+)?$/i, "")
      .replace(/\s*:\s*Pick\s*\d+(?:\s*\/\s*\d+)?$/i, "")
      .trim();
  }
  const pickPattern = /\bPick\s*(?:N|\d+)(?:\s*\/\s*\d+)?\b/gi;
  if (n > 0 && (logic === "PICK_N" || logic === "ANY_N" || pickPattern.test(raw))) {
    const suffix = total > 0 ? `Pick ${n}/${total}` : `Pick ${n}`;
    const base = raw
      .replace(pickPattern, "")
      .replace(/\s*:\s*$/g, "")
      .replace(/\s{2,}/g, " ")
      .trim();
    return base ? `${base}: ${suffix}` : suffix;
  }
  if (logic === "ALL_REQUIRED" && total > 0) {
    const allPattern = /\bAll\s*Required(?:\s*\/\s*\d+)?\b/gi;
    if (allPattern.test(raw)) {
      return raw.replace(allPattern, `All Required/${total}`);
    }
    return `${raw}: All Required/${total}`;
  }
  return raw;
}

const COURSE_OWNERSHIP_OPTIONS = [
  { value: "DF", label: "DF (Dean of Faculty)" },
  { value: "CW", label: "CW (Cadet Wing)" },
  { value: "AD", label: "AD (Athletics)" },
  { value: "TG", label: "TG (Airmanship)" },
];

const STATUS_TAG_STYLE_SHORT = {
  minWidth: 48,
  textAlign: "center",
  fontFamily: "Menlo, Consolas, monospace",
  fontSize: 12,
  letterSpacing: 0.25,
};

const STATUS_TAG_STYLE_LONG = {
  minWidth: 88,
  textAlign: "center",
  fontFamily: "Menlo, Consolas, monospace",
  fontSize: 12,
  letterSpacing: 0.25,
};

function normalizeStatusToken(raw, fallback = "PASS") {
  const token = String(raw || "").trim().toUpperCase();
  if (!token) return fallback;
  if (token === "WARNING") return "WARN";
  return token;
}

function ScrollPane({ maxHeight = 560, children }) {
  return (
    <div className="studio-scroll-shell" style={{ maxHeight }}>
      <div className="studio-scroll-viewport" style={{ maxHeight }}>
        {children}
      </div>
    </div>
  );
}

export function DesignStudioPage() {
  const qc = useQueryClient();
  const treeExpandInitVersionRef = useRef(null);
  const [commentText, setCommentText] = useState("");
  const [changeTitle, setChangeTitle] = useState("");
  const [selectedVersionId, setSelectedVersionId] = useState();
  const [datasetBundleName, setDatasetBundleName] = useState("");
  const [datasetModules, setDatasetModules] = useState(["ALL"]);
  const [selectedSavedDatasetId, setSelectedSavedDatasetId] = useState();
  const [selectedSuggestedSequenceId, setSelectedSuggestedSequenceId] = useState();
  const datasetImportInputRef = useRef(null);
  const [canvasViewMode, setCanvasViewMode] = useState("STANDARD");
  const [courseTitleHoverEnabled, setCourseTitleHoverEnabled] = useState(false);
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
  const [editReqOptionSlotKey, setEditReqOptionSlotKey] = useState("");
  const [editReqOptionSlotCapacity, setEditReqOptionSlotCapacity] = useState("1");
  const [editReqMajorName, setEditReqMajorName] = useState("");
  const [editReqDivision, setEditReqDivision] = useState();
  const [editReqCoreTrack, setEditReqCoreTrack] = useState("");
  const [reqScopeLocked, setReqScopeLocked] = useState(false);
  const [reqLockedCategory, setReqLockedCategory] = useState();
  const [reqCourseModalOpen, setReqCourseModalOpen] = useState(false);
  const [reqLinkKind, setReqLinkKind] = useState("COURSE");
  const [reqLinkKindLocked, setReqLinkKindLocked] = useState(false);
  const [reqCourseRequirementId, setReqCourseRequirementId] = useState();
  const [reqCourseId, setReqCourseId] = useState();
  const [reqCourseTimingRules, setReqCourseTimingRules] = useState([]);
  const [reqSubDraftRows, setReqSubDraftRows] = useState([]);
  const [basketRequirementId, setBasketRequirementId] = useState();
  const [basketLinkId, setBasketLinkId] = useState();
  const [basketSelectedId, setBasketSelectedId] = useState();
  const [basketName, setBasketName] = useState("");
  const [basketDescription, setBasketDescription] = useState("");
  const [basketMinCount, setBasketMinCount] = useState("1");
  const [basketCourseIds, setBasketCourseIds] = useState([]);
  const [basketSubGroupRows, setBasketSubGroupRows] = useState([]);
  const [treeExpandedKeys, setTreeExpandedKeys] = useState([]);
  const [reqEditorOpen, setReqEditorOpen] = useState(false);
  const [coreRulesModalOpen, setCoreRulesModalOpen] = useState(false);
  const [coreRulesProgramId, setCoreRulesProgramId] = useState();
  const [coreRulesProgramName, setCoreRulesProgramName] = useState("");
  const [coreRulesRows, setCoreRulesRows] = useState([]);
  const [coreRulesReqMeta, setCoreRulesReqMeta] = useState([]);
  const [compareFrom, setCompareFrom] = useState();
  const [compareTo, setCompareTo] = useState();
  const [selectedCadetId, setSelectedCadetId] = useState();
  const [selectedCourseId, setSelectedCourseId] = useState();
  const [courseSearchValue, setCourseSearchValue] = useState("");
  const [newCourseModalOpen, setNewCourseModalOpen] = useState(false);
  const [newCourseBusy, setNewCourseBusy] = useState(false);
  const [newCourseForm, setNewCourseForm] = useState({
    course_number: "",
    title: "",
    credit_hours: "3",
    designated_semester: null,
    offered_periods_json: "",
    standing_requirement: "",
    additional_requirements_text: "",
    min_section_size: "6",
    ownership_code: "DF",
  });
  const [newCourseBucketCode, setNewCourseBucketCode] = useState("ABET_MATH_BASIC_SCI");
  const [newCourseBucketHours, setNewCourseBucketHours] = useState("");
  const [newPrereqId, setNewPrereqId] = useState();
  const [newPrereqType, setNewPrereqType] = useState("PREREQUISITE");
  const [newPrereqEnforcement, setNewPrereqEnforcement] = useState("HARD");
  const [newPrereqGroupKey, setNewPrereqGroupKey] = useState("");
  const [newPrereqGroupLabel, setNewPrereqGroupLabel] = useState("");
  const [newPrereqGroupMinRequired, setNewPrereqGroupMinRequired] = useState("1");
  const [prereqEdits, setPrereqEdits] = useState({});
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
  const [newRuleSeverity, setNewRuleSeverity] = useState("FAIL");
  const [newRuleActive, setNewRuleActive] = useState("YES");
  const [newRuleConfig, setNewRuleConfig] = useState("{}");
  const [editRuleId, setEditRuleId] = useState();
  const [editRuleTier, setEditRuleTier] = useState(1);
  const [editRuleSeverity, setEditRuleSeverity] = useState("FAIL");
  const [editRuleActive, setEditRuleActive] = useState("YES");
  const [editRuleConfig, setEditRuleConfig] = useState("{}");
  const [validationDomainFilter, setValidationDomainFilter] = useState("ALL");
  const [validationTreeExpandedKeys, setValidationTreeExpandedKeys] = useState([]);
  const [ruleModalOpen, setRuleModalOpen] = useState(false);
  const [ruleModalEditId, setRuleModalEditId] = useState();
  const [ruleFormName, setRuleFormName] = useState("");
  const [ruleFormTier, setRuleFormTier] = useState(1);
  const [ruleFormSeverity, setRuleFormSeverity] = useState("FAIL");
  const [ruleFormActive, setRuleFormActive] = useState("YES");
  const [ruleFormConfig, setRuleFormConfig] = useState("{}");
  const [ruleFormDomain, setRuleFormDomain] = useState("General");
  const [ruleFormDomainSearch, setRuleFormDomainSearch] = useState("");
  const [ruleCategoryGuideOpen, setRuleCategoryGuideOpen] = useState(false);
  const [prereqFromQuery, setPrereqFromQuery] = useState("");
  const [prereqToQuery, setPrereqToQuery] = useState("");
  const [feasibilitySearch, setFeasibilitySearch] = useState("");
  const [courseGeneralForm, setCourseGeneralForm] = useState({
    course_number: "",
    title: "",
  });
  const [courseSchedulingForm, setCourseSchedulingForm] = useState({
    credit_hours: "",
    designated_semester: null,
    offered_periods_json: "",
    standing_requirement: "",
    additional_requirements_text: "",
    min_section_size: "",
    ownership_code: "DF",
  });
  const [courseSaveBusy, setCourseSaveBusy] = useState(false);
  const periodsQ = useQuery({ queryKey: ["period-metadata"], queryFn: () => authed("/meta/periods") });
  const periodRows = useMemo(() => {
    const rows = periodsQ.data?.periods || [];
    return rows.length ? rows : DEFAULT_PERIOD_ROWS;
  }, [periodsQ.data]);
  const planPeriods = useMemo(() => periodRows.map((p) => Number(p.index)), [periodRows]);
  const periodLabelMap = useMemo(() => {
    const out = {};
    for (const p of periodRows) out[Number(p.index)] = p.label;
    return out;
  }, [periodRows]);
  const periodShortLabelMap = useMemo(() => {
    const out = {};
    for (const p of periodRows) out[Number(p.index)] = p.short_label;
    return out;
  }, [periodRows]);
  const periodLabel = (period) => periodLabelMap[Number(period)] || `Period ${period}`;
  const periodShortLabel = (period) => periodShortLabelMap[Number(period)] || `P${period}`;
  const semesterOptions = planPeriods.map((s) => ({ value: s, label: periodLabel(s) }));
  const timingTypeOptions = [
    { value: "FIXED", label: "Fixed period" },
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

  useEffect(() => {
    setSelectedSavedDatasetId(undefined);
    setSelectedSuggestedSequenceId(undefined);
  }, [selectedVersion?.id]);

  const canvasQ = useQuery({
    queryKey: ["canvas", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/canvas/${selectedVersion.id}`)
  });
  const datasetSavedQ = useQuery({
    queryKey: ["dataset-saved", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/datasets/${selectedVersion.id}/saved`)
  });
  const suggestedSequencesQ = useQuery({
    queryKey: ["suggested-sequences", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/design/canvas/${selectedVersion.id}/suggested`)
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
    queryFn: () => authed(`/courses?version_id=${selectedVersion.id}&limit=5000`)
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
  const courseBucketsQ = useQuery({
    queryKey: ["course-buckets", selectedCourseId],
    enabled: !!selectedCourseId,
    queryFn: () => authed(`/courses/${selectedCourseId}/buckets`),
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
  const basketsQ = useQuery({
    queryKey: ["baskets", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/baskets?version_id=${selectedVersion.id}`),
  });
  const basketSubstitutionsVersionQ = useQuery({
    queryKey: ["basket-substitutions-version", selectedVersion?.id],
    enabled: !!selectedVersion?.id,
    queryFn: () => authed(`/baskets/substitutions/version/${selectedVersion.id}`),
  });

  useEffect(() => {
    const general = courseDetailQ.data?.general;
    const scheduling = courseDetailQ.data?.scheduling;
    if (!general) {
      setCourseGeneralForm({ course_number: "", title: "" });
      setCourseSchedulingForm({
        credit_hours: "",
        designated_semester: null,
        offered_periods_json: "",
        standing_requirement: "",
        additional_requirements_text: "",
        min_section_size: "",
        ownership_code: "DF",
      });
      return;
    }
    setCourseGeneralForm({
      course_number: String(general.course_number || ""),
      title: String(general.title || ""),
    });
    setCourseSchedulingForm({
      credit_hours: scheduling?.credit_hours == null ? "" : String(scheduling.credit_hours),
      designated_semester: scheduling?.designated_semester ?? null,
      offered_periods_json: String(scheduling?.offered_periods_json || ""),
      standing_requirement: String(scheduling?.standing_requirement || ""),
      additional_requirements_text: String(scheduling?.additional_requirements_text || ""),
      min_section_size: scheduling?.min_section_size == null ? "" : String(scheduling.min_section_size),
      ownership_code: String(scheduling?.ownership_code || general?.ownership_code || "DF"),
    });
  }, [courseDetailQ.data?.general, courseDetailQ.data?.scheduling, selectedCourseId]);

  useEffect(() => {
    if (!selectedCourseId) {
      setCourseSearchValue("");
      return;
    }
    const selected = (coursesQ.data || []).find((c) => c.id === selectedCourseId);
    setCourseSearchValue(selected ? `${selected.course_number} - ${selected.title}` : "");
  }, [selectedCourseId, coursesQ.data]);

  useEffect(() => {
    const rows = courseDetailQ.data?.prerequisites || [];
    const next = {};
    for (const r of rows) {
      next[r.id] = {
        required_course_id: r.required_course_id,
        relationship_type: r.relationship_type || "PREREQUISITE",
        enforcement: r.enforcement || "HARD",
        prerequisite_group_key: r.prerequisite_group_key || "",
        group_label: r.group_label || "",
        group_min_required: String(r.group_min_required || 1),
      };
    }
    setPrereqEdits(next);
  }, [courseDetailQ.data?.prerequisites]);

  useEffect(() => {
    if (canvasViewMode === "VERBOSE") setCourseTitleHoverEnabled(true);
  }, [canvasViewMode]);

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

  function selectedDatasetModulesCsv() {
    const mods = (datasetModules || []).map((m) => String(m || "").toUpperCase()).filter(Boolean);
    if (!mods.length || mods.includes("ALL")) return "ALL";
    return mods.join(",");
  }

  async function saveDatasetBundle() {
    if (!selectedVersion?.id) return;
    const name = (datasetBundleName || "").trim();
    if (!name) return;
    const modulesCsv = selectedDatasetModulesCsv();
    await authed(
      `/design/datasets/${selectedVersion.id}/save?name=${encodeURIComponent(name)}&modules=${encodeURIComponent(modulesCsv)}`,
      { method: "POST" }
    );
    setDatasetBundleName("");
    await qc.invalidateQueries({ queryKey: ["dataset-saved", selectedVersion.id] });
  }

  async function exportDatasetBundle() {
    if (!selectedVersion?.id) return;
    const modulesCsv = selectedDatasetModulesCsv();
    const payload = await authed(`/design/datasets/${selectedVersion.id}/export?modules=${encodeURIComponent(modulesCsv)}`);
    const fileNameSafeVersion = String(selectedVersion.name || "dataset-bundle").replace(/[^a-z0-9._-]+/gi, "_");
    const fileName = `${fileNameSafeVersion}_dataset_bundle.json`;
    const fileText = JSON.stringify(payload, null, 2);
    if (window.showSaveFilePicker) {
      const handle = await window.showSaveFilePicker({
        suggestedName: fileName,
        types: [{ description: "JSON Files", accept: { "application/json": [".json"] } }],
      });
      const writable = await handle.createWritable();
      await writable.write(fileText);
      await writable.close();
      return;
    }
    const blob = new Blob([fileText], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  async function importDatasetBundleFromFile(file) {
    if (!selectedVersion?.id || !file) return;
    const raw = await file.text();
    const parsed = JSON.parse(raw);
    const modulesCsv = selectedDatasetModulesCsv();
    const modulesQuery = modulesCsv === "ALL" ? "" : `&modules=${encodeURIComponent(modulesCsv)}`;
    const result = await authed(`/design/datasets/${selectedVersion.id}/import?replace_existing=true${modulesQuery}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(parsed),
    });
    if (result?.mismatches?.length) {
      window.alert(`Imported with mismatches:\n- ${result.mismatches.join("\n- ")}`);
    }
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["courses", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["programs", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-list", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirement-fulfillment-version", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirement-substitutions-version", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation-rules"] }),
      qc.invalidateQueries({ queryKey: ["canvas", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["dataset-saved", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["suggested-sequences", selectedVersion.id] }),
    ]);
  }

  async function loadSavedDatasetBundle() {
    if (!selectedVersion?.id || !selectedSavedDatasetId) return;
    const modulesCsv = selectedDatasetModulesCsv();
    const modulesQuery = modulesCsv === "ALL" ? "" : `&modules=${encodeURIComponent(modulesCsv)}`;
    const result = await authed(
      `/design/datasets/${selectedVersion.id}/saved/${encodeURIComponent(selectedSavedDatasetId)}/load?replace_existing=true${modulesQuery}`,
      { method: "POST" }
    );
    if (result?.mismatches?.length) {
      window.alert(`Loaded with mismatches:\n- ${result.mismatches.join("\n- ")}`);
    }
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["courses", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["programs", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-list", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirement-fulfillment-version", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirement-substitutions-version", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation-rules"] }),
      qc.invalidateQueries({ queryKey: ["canvas", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["dataset-saved", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["suggested-sequences", selectedVersion.id] }),
    ]);
  }

  async function loadSuggestedSequenceToCanvas() {
    if (!selectedVersion?.id || !selectedSuggestedSequenceId) return;
    await authed(
      `/design/canvas/${selectedVersion.id}/suggested/${encodeURIComponent(selectedSuggestedSequenceId)}/load?replace_existing=true`,
      { method: "POST" }
    );
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["canvas", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion.id] }),
    ]);
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
      if (logic === "ANY_ONE") return "Pick 1";
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
      logic_type: rulesetLogic === "ANY_ONE" ? "PICK_N" : rulesetLogic,
      pick_n: (rulesetLogic === "PICK_N" || rulesetLogic === "ANY_ONE") ? Number(rulesetPickN || 1) : null,
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
    setEditReqOptionSlotKey("");
    setEditReqOptionSlotCapacity("1");
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
    setReqLinkKind("COURSE");
    setReqLinkKindLocked(!!opts.primaryCourseId);
    setReqCourseRequirementId(requirementId);
    setBasketRequirementId(requirementId);
    setReqCourseId(opts.primaryCourseId || undefined);
    setReqCourseTimingRules(
      timingRulesFromFields(opts.requiredSemester || null, opts.requiredSemesterMin || null, opts.requiredSemesterMax || null)
    );
    setReqSubDraftRows([]);
    if (!opts.primaryCourseId) {
      setBasketLinkId(undefined);
      setBasketSelectedId(undefined);
      setBasketName("");
      setBasketDescription("");
      setBasketMinCount("1");
      setBasketCourseIds([]);
    }
    setReqCourseModalOpen(true);
  }

  function resetBasketModal() {
    setBasketRequirementId(undefined);
    setBasketLinkId(undefined);
    setBasketSelectedId(undefined);
    setBasketName("");
    setBasketDescription("");
    setBasketMinCount("1");
    setBasketCourseIds([]);
    setBasketSubGroupRows([]);
  }

  function deriveBasketSubstituteRows(basketId, courseIds, requirementId) {
    const bId = basketId || "";
    const reqId = requirementId || "";
    const idSet = new Set((courseIds || []).filter(Boolean));
    if (!idSet.size) return [];
    const basketRows = (basketSubstitutionsVersionQ.data || []).filter(
      (s) => s.basket_id === bId && idSet.has(s.primary_course_id) && idSet.has(s.substitute_course_id)
    );
    const reqRows = (requirementSubstitutionsVersionQ.data || []).filter(
      (s) => s.requirement_id === reqId && idSet.has(s.primary_course_id) && idSet.has(s.substitute_course_id)
    );
    const rows = basketRows.length ? basketRows : reqRows;
    if (!rows.length) return [];
    const adj = {};
    for (const s of rows) {
      if (!adj[s.primary_course_id]) adj[s.primary_course_id] = new Set();
      if (!adj[s.substitute_course_id]) adj[s.substitute_course_id] = new Set();
      adj[s.primary_course_id].add(s.substitute_course_id);
      adj[s.substitute_course_id].add(s.primary_course_id);
    }
    const seen = new Set();
    const out = [];
    const allIds = Array.from(idSet);
    for (const cid of allIds) {
      if (seen.has(cid) || !adj[cid] || !adj[cid].size) continue;
      const q = [cid];
      const comp = [];
      seen.add(cid);
      while (q.length) {
        const cur = q.shift();
        comp.push(cur);
        for (const nxt of Array.from(adj[cur] || [])) {
          if (seen.has(nxt)) continue;
          seen.add(nxt);
          q.push(nxt);
        }
      }
      if (comp.length <= 1) continue;
      const sorted = [...comp].sort((a, b) =>
        String(courseMapById[a]?.course_number || courseMapById[a]?.title || a)
          .localeCompare(String(courseMapById[b]?.course_number || courseMapById[b]?.title || b))
      );
      out.push({ primary_course_id: sorted[0], substitute_course_ids: sorted.slice(1) });
    }
    return out;
  }

  function openRequirementBasketModal(requirementId, basketLink) {
    setReqLinkKind("BASKET");
    setReqLinkKindLocked(!!basketLink);
    resetBasketModal();
    setBasketRequirementId(requirementId);
    setReqCourseRequirementId(requirementId);
    setReqCourseId(undefined);
    setReqCourseTimingRules([]);
    setReqSubDraftRows([]);
    if (basketLink) {
      setBasketLinkId(basketLink.id);
      setBasketSelectedId(basketLink.basket_id || undefined);
      setBasketName(basketLink.basket_name || "");
      setBasketMinCount(String(basketLink.min_count || 1));
      setBasketCourseIds((basketLink.courses || []).map((x) => x.course_id).filter(Boolean));
      const b = (basketsQ.data || []).find((x) => x.id === basketLink.basket_id);
      setBasketDescription(b?.description || "");
      const ids = (basketLink.courses || []).map((x) => x.course_id).filter(Boolean);
      setBasketSubGroupRows(deriveBasketSubstituteRows(basketLink.basket_id, ids, requirementId));
    }
    setReqCourseModalOpen(true);
  }

  useEffect(() => {
    if (!reqCourseModalOpen || reqLinkKind !== "BASKET") return;
    if (!basketSelectedId) return;
    const ids = Array.from(new Set((basketCourseIds || []).filter(Boolean)));
    setBasketSubGroupRows((prev) => {
      if (prev.length) {
        return prev
          .map((r) => {
            const primary = ids.includes(r.primary_course_id) ? r.primary_course_id : undefined;
            const subs = Array.from(new Set((r.substitute_course_ids || []).filter((x) => x && x !== primary && ids.includes(x))));
            return { primary_course_id: primary, substitute_course_ids: subs };
          })
          .filter((r) => r.primary_course_id && (r.substitute_course_ids || []).length);
      }
      return deriveBasketSubstituteRows(basketSelectedId, ids, basketRequirementId);
    });
  }, [
    reqCourseModalOpen,
    reqLinkKind,
    basketSelectedId,
    basketCourseIds,
    basketSubstitutionsVersionQ.data,
  ]);

  async function saveRequirementBasketModal() {
    if (!selectedVersion?.id || !basketRequirementId) return;
    if (basketValidationErrors.length) return;
    const parsedMin = Number(basketMinCount || 1);
    const minCount = Number.isFinite(parsedMin) && parsedMin > 0 ? Math.floor(parsedMin) : 1;
    const selectedExisting = basketSelectedId ? (basketsQ.data || []).find((b) => b.id === basketSelectedId) : null;
    let targetBasketId = selectedExisting?.id || null;

    if (!targetBasketId) {
      const trimmedName = (basketName || "").trim();
      if (!trimmedName) return;
      const created = await authed("/baskets", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          version_id: selectedVersion.id,
          name: trimmedName,
          description: (basketDescription || "").trim() || null,
          sort_order: null,
        }),
      });
      targetBasketId = created.id;
    } else {
      await authed(`/baskets/${targetBasketId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          version_id: selectedVersion.id,
          name: (basketName || selectedExisting?.name || "").trim() || selectedExisting?.name || "Basket",
          description: (basketDescription || "").trim() || null,
          sort_order: selectedExisting?.sort_order ?? 0,
        }),
      });
    }

    const latestBaskets = await authed(`/baskets?version_id=${selectedVersion.id}`);
    const targetBasket = (latestBaskets || []).find((b) => b.id === targetBasketId);
    const existingItems = targetBasket?.items || [];
    const existingCourseIds = new Set(existingItems.map((x) => x.course_id));
    const desiredCourseIds = Array.from(new Set((basketCourseIds || []).filter(Boolean)));
    for (const cid of desiredCourseIds) {
      if (existingCourseIds.has(cid)) continue;
      await authed("/baskets/items", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ basket_id: targetBasketId, course_id: cid, sort_order: null }),
      });
    }
    for (const row of existingItems) {
      if (desiredCourseIds.includes(row.course_id)) continue;
      await authed(`/baskets/items/${row.id}`, { method: "DELETE" });
    }

    if (basketLinkId) {
      await authed(`/requirements/baskets/${basketLinkId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requirement_id: basketRequirementId,
          basket_id: targetBasketId,
          min_count: minCount,
          max_count: null,
          sort_order: null,
        }),
      });
    } else {
      await authed("/requirements/baskets", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requirement_id: basketRequirementId,
          basket_id: targetBasketId,
          min_count: minCount,
          max_count: null,
          sort_order: null,
        }),
      });
    }

    const allBasketSubs = (basketSubstitutionsVersionQ.data || []).filter((s) => s.basket_id === targetBasketId);
    const allReqSubs = (requirementSubstitutionsVersionQ.data || []).filter((s) => s.requirement_id === basketRequirementId);
    const desiredCourseSet = new Set(desiredCourseIds);
    const relevantExistingSource = allBasketSubs.length ? allBasketSubs : allReqSubs;
    const relevantExisting = relevantExistingSource.filter(
      (s) => desiredCourseSet.has(s.primary_course_id) && desiredCourseSet.has(s.substitute_course_id)
    );
    const normalizePair = (a, b) => [a, b].sort().join("|");
    const existingByNorm = {};
    for (const row of relevantExisting) {
      const key = normalizePair(row.primary_course_id, row.substitute_course_id);
      if (!existingByNorm[key]) existingByNorm[key] = [];
      existingByNorm[key].push(row);
    }
    const desiredRows = (basketSubGroupRows || [])
      .map((r) => ({
        primary_course_id: r.primary_course_id,
        substitute_course_ids: Array.from(
          new Set((r.substitute_course_ids || []).filter((id) => id && id !== r.primary_course_id && desiredCourseSet.has(id)))
        ),
      }))
      .filter((r) => r.primary_course_id && desiredCourseSet.has(r.primary_course_id));
    const desiredNorm = new Set();
    for (const row of desiredRows) {
      for (const sid of row.substitute_course_ids || []) {
        desiredNorm.add(normalizePair(row.primary_course_id, sid));
      }
    }
    const useBasketScoped = allBasketSubs.length > 0 || !!targetBasketId;
    const createPath = useBasketScoped ? "/baskets/substitutions" : "/requirements/substitutions";
    const deletePath = (id) => (useBasketScoped ? `/baskets/substitutions/${id}` : `/requirements/substitutions/${id}`);
    try {
      for (const [key, rows] of Object.entries(existingByNorm)) {
        if (desiredNorm.has(key)) continue;
        for (const row of rows) {
          await authed(deletePath(row.id), { method: "DELETE" });
        }
      }
      for (const row of desiredRows) {
        for (const sid of row.substitute_course_ids || []) {
          const key = normalizePair(row.primary_course_id, sid);
          if (existingByNorm[key]?.length) continue;
          const body = useBasketScoped
            ? {
                basket_id: targetBasketId,
                primary_course_id: row.primary_course_id,
                substitute_course_id: sid,
                is_bidirectional: true,
              }
            : {
                requirement_id: basketRequirementId,
                primary_course_id: row.primary_course_id,
                substitute_course_id: sid,
                is_bidirectional: true,
              };
          await authed(createPath, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
          });
        }
      }
    } catch (err) {
      // Fallback for partially-updated backend deployments: use requirement-scoped substitutions.
      for (const [key, rows] of Object.entries(existingByNorm)) {
        if (desiredNorm.has(key)) continue;
        for (const row of rows) {
          if (row.requirement_id === basketRequirementId) {
            await authed(`/requirements/substitutions/${row.id}`, { method: "DELETE" });
          }
        }
      }
      for (const row of desiredRows) {
        for (const sid of row.substitute_course_ids || []) {
          const key = normalizePair(row.primary_course_id, sid);
          if (existingByNorm[key]?.length) continue;
          await authed("/requirements/substitutions", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              requirement_id: basketRequirementId,
              primary_course_id: row.primary_course_id,
              substitute_course_id: sid,
              is_bidirectional: true,
            }),
          });
        }
      }
      // Preserve a visible signal for diagnostics.
      if (err instanceof Error) {
        window.alert(`Saved with fallback substitution path. ${err.message}`);
      }
    }

    setReqCourseModalOpen(false);
    setReqLinkKindLocked(false);
    resetBasketModal();
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["baskets", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["basket-substitutions-version", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["basket-substitutions-version", selectedVersion.id] }),
    ]);
  }

  async function unlinkRequirementBasket(linkId) {
    if (!linkId || !selectedVersion?.id) return;
    await authed(`/requirements/baskets/${linkId}`, { method: "DELETE" });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["baskets", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["basket-substitutions-version", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion.id] }),
    ]);
  }

  async function deleteRequirementBasket(linkId, basketId) {
    if (!selectedVersion?.id) return;
    if (basketId) {
      await authed(`/baskets/${basketId}`, { method: "DELETE" });
    } else if (linkId) {
      await authed(`/requirements/baskets/${linkId}`, { method: "DELETE" });
    } else {
      return;
    }
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["baskets", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] }),
      qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion.id] }),
    ]);
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
    setReqLinkKindLocked(false);
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
        if (logic === "ANY_ONE") return "Pick 1";
        if (logic === "OPTION_SLOT") return `Option Slot (${Number(editReqOptionSlotCapacity || 1)})`;
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
      logic_type: editReqLogic === "ANY_ONE" ? "PICK_N" : editReqLogic,
      pick_n: (editReqLogic === "PICK_N" || editReqLogic === "ANY_ONE") ? Number(editReqPickN || 1) : null,
      option_slot_key: editReqLogic === "OPTION_SLOT" ? (editReqOptionSlotKey || "").trim() || null : null,
      option_slot_capacity: editReqLogic === "OPTION_SLOT" ? Number(editReqOptionSlotCapacity || 1) : null,
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
        if (logic === "ANY_ONE") return "Pick 1";
        if (logic === "OPTION_SLOT") return `Option Slot (${Number(editReqOptionSlotCapacity || 1)})`;
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
          if (cLogic === "OPTION_SLOT") {
            return (
              String(r.option_slot_key || "").trim().toLowerCase() === String(editReqOptionSlotKey || "").trim().toLowerCase()
              && Number(r.option_slot_capacity || 1) === Number(editReqOptionSlotCapacity || 1)
            );
          }
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
          if (cLogic === "OPTION_SLOT") {
            return (
              String(r.option_slot_key || "").trim().toLowerCase() === String(editReqOptionSlotKey || "").trim().toLowerCase()
              && Number(r.option_slot_capacity || 1) === Number(editReqOptionSlotCapacity || 1)
            );
          }
          return true;
        }
        const rLogic = String(r.logic_type || "ALL_REQUIRED").toUpperCase();
        const cLogic = String(editReqLogic || "ALL_REQUIRED").toUpperCase();
        if (rLogic !== cLogic) return false;
        if (cLogic === "PICK_N") return Number(r.pick_n || 0) === Number(editReqPickN || 1);
        if (cLogic === "OPTION_SLOT") {
          return (
            String(r.option_slot_key || "").trim().toLowerCase() === String(editReqOptionSlotKey || "").trim().toLowerCase()
            && Number(r.option_slot_capacity || 1) === Number(editReqOptionSlotCapacity || 1)
          );
        }
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
        logic_type: editReqLogic === "ANY_ONE" ? "PICK_N" : editReqLogic,
        pick_n: (editReqLogic === "PICK_N" || editReqLogic === "ANY_ONE") ? Number(editReqPickN || 1) : null,
        option_slot_key: editReqLogic === "OPTION_SLOT" ? (editReqOptionSlotKey || "").trim() || null : null,
        option_slot_capacity: editReqLogic === "OPTION_SLOT" ? Number(editReqOptionSlotCapacity || 1) : null,
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

  async function saveCourseDetail() {
    if (!selectedCourseId || !courseDetailQ.data?.general?.version_id) return;
    const payload = {
      version_id: courseDetailQ.data.general.version_id,
      course_number: normalizeCourseNumber(courseGeneralForm.course_number),
      title: String(courseGeneralForm.title || "").trim(),
      credit_hours: Number(courseSchedulingForm.credit_hours || 0),
      designated_semester:
        courseSchedulingForm.designated_semester == null || courseSchedulingForm.designated_semester === ""
          ? null
          : Number(courseSchedulingForm.designated_semester),
      offered_periods_json: String(courseSchedulingForm.offered_periods_json || "").trim() || null,
      standing_requirement: String(courseSchedulingForm.standing_requirement || "").trim() || null,
      additional_requirements_text: String(courseSchedulingForm.additional_requirements_text || "").trim() || null,
      min_section_size: Number(courseSchedulingForm.min_section_size || 0),
      ownership_code: String(courseSchedulingForm.ownership_code || "DF"),
    };
    if (!payload.course_number || !payload.title || Number.isNaN(payload.credit_hours) || Number.isNaN(payload.min_section_size)) {
      window.alert("Please provide valid values for course number, title, credit hours, and minimum section size.");
      return;
    }
    setCourseSaveBusy(true);
    try {
      await authed(`/courses/${selectedCourseId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["course-detail", selectedCourseId] }),
        qc.invalidateQueries({ queryKey: ["courses", selectedVersion?.id] }),
      ]);
    } finally {
      setCourseSaveBusy(false);
    }
  }

  async function createCourseFromEditor() {
    if (!selectedVersion?.id) return;
    const payload = {
      version_id: selectedVersion.id,
      course_number: normalizeCourseNumber(newCourseForm.course_number),
      title: String(newCourseForm.title || "").trim(),
      credit_hours: Number(newCourseForm.credit_hours || 0),
      designated_semester:
        newCourseForm.designated_semester == null || newCourseForm.designated_semester === ""
          ? null
          : Number(newCourseForm.designated_semester),
      offered_periods_json: String(newCourseForm.offered_periods_json || "").trim() || null,
      standing_requirement: String(newCourseForm.standing_requirement || "").trim() || null,
      additional_requirements_text: String(newCourseForm.additional_requirements_text || "").trim() || null,
      min_section_size: Number(newCourseForm.min_section_size || 0),
      ownership_code: String(newCourseForm.ownership_code || "DF"),
    };
    if (!payload.course_number || !payload.title || Number.isNaN(payload.credit_hours) || Number.isNaN(payload.min_section_size)) {
      window.alert("Please provide valid values for course number, title, credit hours, and minimum section size.");
      return;
    }
    setNewCourseBusy(true);
    try {
      const created = await authed("/courses", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setNewCourseModalOpen(false);
      setNewCourseForm({
        course_number: "",
        title: "",
        credit_hours: "3",
        designated_semester: null,
        offered_periods_json: "",
        standing_requirement: "",
        additional_requirements_text: "",
        min_section_size: "6",
        ownership_code: "DF",
      });
      setSelectedCourseId(created.id);
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["courses", selectedVersion?.id] }),
        qc.invalidateQueries({ queryKey: ["course-detail", created.id] }),
      ]);
    } finally {
      setNewCourseBusy(false);
    }
  }

  async function addPrerequisite() {
    if (!selectedCourseId || !newPrereqId) return;
    const groupKey = String(newPrereqGroupKey || "").trim();
    const groupMin = Number(newPrereqGroupMinRequired || 1);
    await authed("/prerequisites", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        course_id: selectedCourseId,
        required_course_id: newPrereqId,
        relationship_type: newPrereqType,
        enforcement: newPrereqEnforcement,
        prerequisite_group_key: groupKey || null,
        group_min_required: groupKey ? (Number.isFinite(groupMin) && groupMin > 0 ? Math.floor(groupMin) : 1) : null,
        group_label: groupKey ? (String(newPrereqGroupLabel || "").trim() || null) : null,
      })
    });
    setNewPrereqId(undefined);
    setNewPrereqGroupKey("");
    setNewPrereqGroupLabel("");
    setNewPrereqGroupMinRequired("1");
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["course-detail", selectedCourseId] }),
      qc.invalidateQueries({ queryKey: ["prereq-graph"] }),
      qc.invalidateQueries({ queryKey: ["validation"] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
    ]);
  }

  async function deletePrerequisiteRow(prerequisiteId) {
    if (!prerequisiteId) return;
    await authed(`/prerequisites/${prerequisiteId}`, { method: "DELETE" });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["course-detail", selectedCourseId] }),
      qc.invalidateQueries({ queryKey: ["prereq-graph"] }),
      qc.invalidateQueries({ queryKey: ["validation"] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
    ]);
  }

  async function savePrerequisiteRow(row) {
    const draft = prereqEdits[row.id];
    if (!draft?.required_course_id) {
      window.alert("Required course is required.");
      return;
    }
    const groupKey = String(draft.prerequisite_group_key || "").trim();
    const groupMin = Number(draft.group_min_required || 1);
    await authed(`/prerequisites/${row.id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        course_id: selectedCourseId,
        required_course_id: draft.required_course_id,
        relationship_type: draft.relationship_type || "PREREQUISITE",
        enforcement: draft.enforcement || "HARD",
        prerequisite_group_key: groupKey || null,
        group_min_required: groupKey ? (Number.isFinite(groupMin) && groupMin > 0 ? Math.floor(groupMin) : 1) : null,
        group_label: groupKey ? (String(draft.group_label || "").trim() || null) : null,
      }),
    });
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

  async function addCourseBucket() {
    if (!selectedCourseId || !newCourseBucketCode) return;
    const payload = {
      bucket_code: String(newCourseBucketCode || "").trim(),
      credit_hours_override: newCourseBucketHours === "" ? null : Number(newCourseBucketHours),
      sort_order: 0,
    };
    await authed(`/courses/${selectedCourseId}/buckets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setNewCourseBucketHours("");
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["course-buckets", selectedCourseId] }),
      qc.invalidateQueries({ queryKey: ["course-detail", selectedCourseId] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion?.id] }),
    ]);
  }

  async function deleteCourseBucket(bucketTagId) {
    if (!bucketTagId) return;
    await authed(`/courses/buckets/${bucketTagId}`, { method: "DELETE" });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["course-buckets", selectedCourseId] }),
      qc.invalidateQueries({ queryKey: ["course-detail", selectedCourseId] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion?.id] }),
    ]);
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
    setEditRuleSeverity(String(r.severity || "FAIL").toUpperCase() === "WARNING" ? "WARN" : (r.severity || "FAIL"));
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

  function parseRuleConfig(rule) {
    try {
      return JSON.parse(rule?.config_json || "{}");
    } catch {
      return {};
    }
  }

  function inferValidationDomain(rule) {
    const name = String(rule?.name || "").toLowerCase();
    const cfg = parseRuleConfig(rule);
    const explicit = String(cfg?.domain || "").trim();
    if (explicit) return explicit;
    if (
      name.includes("abet") ||
      name.includes("major") ||
      cfg.program ||
      cfg.program_id ||
      cfg.major ||
      cfg.pathway
    ) {
      return "Program/Major Pathway";
    }
    if (
      name.includes("prerequisite") ||
      name.includes("ordering") ||
      name.includes("co-requisite") ||
      name.includes("integrity")
    ) {
      return "Curriculum Integrity";
    }
    if (
      name.includes("section") ||
      name.includes("instructor") ||
      name.includes("classroom") ||
      name.includes("capacity") ||
      name.includes("resource")
    ) {
      return "Resources";
    }
    return "General";
  }

  function compareRuleOrder(a, b) {
    const aCfg = parseRuleConfig(a);
    const bCfg = parseRuleConfig(b);
    const aOrder = Number(aCfg?.domain_order);
    const bOrder = Number(bCfg?.domain_order);
    const aHasOrder = Number.isFinite(aOrder) && aOrder > 0;
    const bHasOrder = Number.isFinite(bOrder) && bOrder > 0;
    if (aHasOrder && bHasOrder && aOrder !== bOrder) return aOrder - bOrder;
    if (aHasOrder !== bHasOrder) return aHasOrder ? -1 : 1;
    const aNum = Number(String(a?.rule_code || "").replace(/^R/i, ""));
    const bNum = Number(String(b?.rule_code || "").replace(/^R/i, ""));
    const aHas = Number.isFinite(aNum) && aNum > 0;
    const bHas = Number.isFinite(bNum) && bNum > 0;
    if (aHas && bHas && aNum !== bNum) return aNum - bNum;
    if (aHas !== bHas) return aHas ? -1 : 1;
    return String(a?.name || "").localeCompare(String(b?.name || ""));
  }

  async function handleValidationTreeDrop(info) {
    const dragKey = String(info.dragNode?.key || "");
    const dropKey = String(info.node?.key || "");
    if (!dragKey.startsWith("vrule:")) return;
    if (!dropKey.startsWith("vrule:")) return;
    const dragId = dragKey.replace("vrule:", "");
    const dropId = dropKey.replace("vrule:", "");
    if (!dragId || !dropId || dragId === dropId) return;
    const all = validationRulesWithDomain || [];
    const dragRule = all.find((r) => r.id === dragId);
    const dropRule = all.find((r) => r.id === dropId);
    if (!dragRule || !dropRule || dragRule.domain !== dropRule.domain) return;
    const ordered = [...all].filter((r) => r.domain === dragRule.domain).sort(compareRuleOrder);
    const from = ordered.findIndex((r) => r.id === dragId);
    const target = ordered.findIndex((r) => r.id === dropId);
    if (from < 0 || target < 0) return;
    const next = [...ordered];
    const [moved] = next.splice(from, 1);
    let insertAt = target;
    const dropPos = Number(info.dropPosition);
    const nodePos = Number(String(info.node?.pos || "").split("-").pop());
    const relative = Number.isFinite(dropPos) && Number.isFinite(nodePos) ? dropPos - nodePos : 0;
    if (relative > 0) insertAt += 1;
    if (from < target && relative <= 0) insertAt -= 1;
    if (insertAt < 0) insertAt = 0;
    if (insertAt > next.length) insertAt = next.length;
    next.splice(insertAt, 0, moved);
    for (let idx = 0; idx < next.length; idx += 1) {
      const r = next[idx];
      const cfg = parseRuleConfig(r);
      cfg.domain = dragRule.domain;
      cfg.domain_order = idx + 1;
      await authed(`/design/validation-rules/${r.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ config: cfg }),
      });
    }
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["validation-rules"] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
    ]);
  }

  function openCreateRuleModal() {
    setRuleModalEditId(undefined);
    setRuleFormName("");
    setRuleFormTier(1);
    setRuleFormSeverity("FAIL");
    setRuleFormActive("YES");
    setRuleFormConfig("{}");
    setRuleFormDomain("General");
    setRuleFormDomainSearch("");
    setRuleModalOpen(true);
  }

  function openEditRuleModal(rule) {
    setRuleModalEditId(rule.id);
    setRuleFormName(rule.name || "");
    setRuleFormTier(Number(rule.tier || 1));
    setRuleFormSeverity(String(rule.severity || "FAIL").toUpperCase() === "WARNING" ? "WARN" : (rule.severity || "FAIL"));
    setRuleFormActive(rule.active ? "YES" : "NO");
    setRuleFormConfig(rule.config_json || "{}");
    const cfg = parseRuleConfig(rule);
    setRuleFormDomain(String(cfg.domain || inferValidationDomain(rule) || "General"));
    setRuleFormDomainSearch("");
    setRuleModalOpen(true);
  }

  async function saveRuleModal() {
    if (!ruleFormName.trim()) return;
    let cfg = {};
    try {
      cfg = JSON.parse(ruleFormConfig || "{}");
    } catch {
      return;
    }
    cfg.domain = String(ruleFormDomain || "General").trim() || "General";
    const payload = {
      name: ruleFormName.trim(),
      tier: Number(ruleFormTier),
      severity: ruleFormSeverity,
      active: ruleFormActive === "YES",
      config: cfg,
    };
    if (ruleModalEditId) {
      await authed(`/design/validation-rules/${ruleModalEditId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    } else {
      await authed("/design/validation-rules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    }
    setRuleModalOpen(false);
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
      .filter((r) => {
        if (r.program_id) return false;
        if (String(r.category || "").toUpperCase() !== "CORE") return false;
        const logic = String(r.logic_type || "").toUpperCase();
        if (["PICK_N", "ANY_ONE", "ONE_OF", "ANY_N"].includes(logic)) return true;
        // Also allow core requirements that contain substitution groups so majors
        // can pin a specific variant even when the source core rule is not Pick/Any.
        return (coreRuleChoiceOptionsByReq[r.id] || []).some((o) => (o.group_course_ids || []).length > 1);
      })
      .sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
    const reqMeta = optionalCoreReqs.map((req) => {
      const logic = String(req.logic_type || "ALL_REQUIRED").toUpperCase();
      const subGroupCount = (coreRuleChoiceOptionsByReq[req.id] || []).filter((o) => (o.group_course_ids || []).length > 1).length;
      const restrictToSubGroups = !["PICK_N", "ANY_N", "ANY_ONE", "ONE_OF"].includes(logic);
      const slotTotal = logic === "PICK_N" || logic === "ANY_N" ? Math.max(1, Number(req.pick_n || 1)) : Math.max(1, subGroupCount);
      return {
        requirement_id: req.id,
        requirement_name: formatRequirementName(req.name, req.logic_type, req.pick_n),
        slot_total: slotTotal,
        restrict_to_sub_groups: restrictToSubGroups,
      };
    });
    const rows = [];
    for (const req of optionalCoreReqs) {
      const logic = String(req.logic_type || "ALL_REQUIRED").toUpperCase();
      const meta = reqMeta.find((m) => m.requirement_id === req.id);
      const slotCount =
        logic === "PICK_N" || logic === "ANY_N"
          ? Math.max(1, Number(req.pick_n || 1))
          : 0; // all-required substitute-group rules are opt-in via "Add Substitute Group Rule"
      for (let i = 0; i < slotCount; i += 1) {
        rows.push({
          requirement_id: req.id,
          requirement_name: meta?.requirement_name || formatRequirementName(req.name, req.logic_type, req.pick_n),
          slot_index: i,
          slot_total: meta?.slot_total || slotCount,
          primary_course_id: undefined,
          substitute_course_ids: [],
          required_semester: undefined,
          required_semester_min: undefined,
          required_semester_max: undefined,
          restrict_to_sub_groups: !!meta?.restrict_to_sub_groups,
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
      const usedSlotsByReq = {};
      for (const g of groups) {
        const selected = Array.from(new Set((g.course_numbers || []).map(resolveCourseIdFromToken).filter(Boolean)));
        if (!selected.length) continue;
        let reqId = g.source_requirement_id || g.requirement_id || null;
        let slotIndex = Number(g.slot_index || 0);
        let candidateOptions = reqId ? (coreRuleChoiceOptionsByReq[reqId] || []) : [];
        // Legacy fallback: infer requirement when source_requirement_id was not stored.
        if (!reqId || !candidateOptions.length) {
          let bestReq = null;
          let bestScore = -1;
          for (const req of optionalCoreReqs) {
            const opts = coreRuleChoiceOptionsByReq[req.id] || [];
            if (!opts.length) continue;
            const reqIds = new Set(opts.flatMap((o) => o.group_course_ids || []));
            const score = selected.filter((id) => reqIds.has(id)).length;
            if (score > bestScore) {
              bestScore = score;
              bestReq = req.id;
            }
          }
          if (bestReq && bestScore > 0) {
            reqId = bestReq;
            candidateOptions = coreRuleChoiceOptionsByReq[reqId] || [];
            const used = usedSlotsByReq[reqId] || new Set();
            const slotRows = rows.filter((r) => r.requirement_id === reqId).map((r) => Number(r.slot_index || 0));
            const firstOpen = slotRows.find((s) => !used.has(s));
            if (firstOpen != null) slotIndex = firstOpen;
          }
        }
        if (!reqId || !candidateOptions.length) continue;
        const representative =
          selected.find((sid) => candidateOptions.some((o) => o.value === sid))
          || ((coreRuleRepresentativeByReqCourse[reqId] || {})[selected[0]])
          || selected[0];
        const selectedChoice = candidateOptions.find((o) => o.value === representative);
        const groupIds = new Set((selectedChoice?.group_course_ids || []));
        const fullGroupSelected = isFullChoiceGroupSelection(reqId, selected);
        const sanitizedSubs = groupIds.size > 1
          ? selected.filter((id) => id && groupIds.has(id))
          : [];
        let targetIdx = rows.findIndex((r) => r.requirement_id === reqId && r.slot_index === slotIndex);
        if (targetIdx < 0) {
          const meta = reqMeta.find((m) => m.requirement_id === reqId);
          rows.push({
            requirement_id: reqId,
            requirement_name: meta?.requirement_name || requirementById[reqId]?.name || "Core Requirement",
            slot_index: slotIndex,
            slot_total: meta?.slot_total || Math.max(1, Number(requirementById[reqId]?.pick_n || 1)),
            primary_course_id: undefined,
            substitute_course_ids: [],
            required_semester: undefined,
            required_semester_min: undefined,
            required_semester_max: undefined,
            restrict_to_sub_groups: !!meta?.restrict_to_sub_groups,
          });
          targetIdx = rows.length - 1;
        }
        rows[targetIdx] = {
          ...rows[targetIdx],
          // Keep the selected group visible even when the full group is allowed.
          // Empty substitute_course_ids already encodes "do not narrow".
          primary_course_id: representative,
          substitute_course_ids: fullGroupSelected ? [] : sanitizedSubs,
          required_semester: g.required_semester || undefined,
          required_semester_min: g.required_semester_min || undefined,
          required_semester_max: g.required_semester_max || undefined,
          restrict_to_sub_groups: rows[targetIdx].restrict_to_sub_groups,
        };
        if (targetIdx >= 0) {
          usedSlotsByReq[reqId] = usedSlotsByReq[reqId] || new Set();
          usedSlotsByReq[reqId].add(slotIndex);
        }
      }
    }

    setCoreRulesProgramId(majorProgram.id);
    setCoreRulesProgramName(majorProgram.name);
    setCoreRulesReqMeta(reqMeta);
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
      const explicitSelected = groupIds.length > 1
        ? Array.from(new Set((r.substitute_course_ids || []).filter(Boolean))).filter((id) => groupIds.includes(id))
        : [];
      const timingFields = timingFieldsFromRules(coreRuleTimingRules(r));
      const selectedForConflict = explicitSelected.length
        ? explicitSelected
        : (groupIds.length > 1 ? groupIds : (r.primary_course_id ? [r.primary_course_id] : groupIds));
      const conflictCid = hasTimingConflictWithExisting(selectedForConflict, timingFields);
      if (conflictCid) {
        const cnum = courseMapById[conflictCid]?.course_number || "course";
        const choiceLabel = Number(r.slot_total || 1) > 1 ? ` - Choice ${Number(r.slot_index || 0) + 1}` : "";
        validationErrors.push(`${r.requirement_name}${choiceLabel}: timing conflicts with existing rule for ${cnum}.`);
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
              const choiceLabel = Number(r.slot_total || 1) > 1 ? ` - Choice ${Number(r.slot_index || 0) + 1}` : "";
              validationErrors.push(
                `${r.requirement_name}${choiceLabel}: timing conflicts with existing rule for ${num}.`
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
        const explicitSelected = groupIds.length > 1
          ? Array.from(new Set((r.substitute_course_ids || []).filter(Boolean))).filter((id) => groupIds.includes(id))
          : [];
        const ids = explicitSelected.length
          ? explicitSelected
          : (groupIds.length > 1 ? groupIds : (r.primary_course_id ? [r.primary_course_id] : []));
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
          name: Number(r.slot_total || 1) > 1
            ? `${r.requirement_name} - Choice ${Number(r.slot_index || 0) + 1}`
            : `${r.requirement_name}`,
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

  async function deleteCoreRulesRule(programId, programName) {
    const existingRule = findExistingCoreRulesRule(programId, programName);
    if (!existingRule) return;
    await authed(`/design/validation-rules/${existingRule.id}`, { method: "DELETE" });
    await Promise.all([
      qc.invalidateQueries({ queryKey: ["validation-rules"] }),
      qc.invalidateQueries({ queryKey: ["validation", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["validation-dashboard", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
      qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion?.id] }),
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
    const dragIsBasket = dragKey.startsWith("basket:");
    const dropIsBasket = dropKey.startsWith("basket:");
    const dragIsBasketCourse = dragKey.startsWith("basket-course:");
    const dropIsBasketCourse = dropKey.startsWith("basket-course:");
    const dragIsCourse = dragKey.startsWith("course:");
    const dropIsCourse = dropKey.startsWith("course:");

    if (dragIsBasketCourse) {
      const dragItemId = dragKey.split(":")[2] || "";
      const dragged = basketCourseItemById[dragItemId];
      if (!dragged) return;

      let targetBasketId = null;
      if (dropIsBasketCourse) {
        const dropItemId = dropKey.split(":")[2] || "";
        targetBasketId = basketCourseItemById[dropItemId]?.basket_id || null;
      } else if (dropIsBasket) {
        const dropLinkId = dropKey.split(":")[1] || "";
        targetBasketId = basketLinkById[dropLinkId]?.basket_id || null;
      }
      if (!targetBasketId) return;

      const allItems = Object.values(basketCourseItemById || {});
      const byBasket = {};
      allItems.forEach((x) => {
        byBasket[x.basket_id] = byBasket[x.basket_id] || [];
        byBasket[x.basket_id].push({ ...x, id: x.id });
      });
      const sourceBasketId = dragged.basket_id;
      const sourceRows = [...(byBasket[sourceBasketId] || [])];
      const targetRows = sourceBasketId === targetBasketId ? sourceRows : [...(byBasket[targetBasketId] || [])];
      const sourceWithoutDragged = sourceRows.filter((r) => r.id !== dragItemId);
      const targetWithoutDragged =
        sourceBasketId === targetBasketId ? sourceWithoutDragged : targetRows.filter((r) => r.id !== dragItemId);

      let insertIndex = targetWithoutDragged.length;
      if (dropIsBasketCourse) {
        const dropItemId = dropKey.split(":")[2] || "";
        const idx = targetWithoutDragged.findIndex((r) => r.id === dropItemId);
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

      const movedRow = { ...dragged, basket_id: targetBasketId };
      const nextTargetRows = [...targetWithoutDragged];
      nextTargetRows.splice(insertIndex, 0, movedRow);

      const payload = [];
      if (sourceBasketId !== targetBasketId) {
        sourceWithoutDragged.forEach((r, i) => payload.push({ item_id: r.id, basket_id: sourceBasketId, sort_order: i }));
      }
      nextTargetRows.forEach((r, i) => payload.push({ item_id: r.id, basket_id: targetBasketId, sort_order: i }));

      await authed("/baskets/items/reorder", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["baskets", selectedVersion?.id] }),
        qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion?.id] }),
        qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
      ]);
      return;
    }

    if (dragIsBasket) {
      const dragLinkId = dragKey.split(":")[1] || "";
      const dragged = basketLinkById[dragLinkId];
      if (!dragged) return;

      let targetRequirementId = null;
      if (dropIsBasket) {
        const dropLinkId = dropKey.split(":")[1] || "";
        targetRequirementId = basketLinkById[dropLinkId]?.requirement_id || null;
      } else if (!dropIsCourse && !dropIsBasketCourse && !dropKey.startsWith("core-rules:") && !dropKey.startsWith("core-rule-group:")) {
        targetRequirementId = dropKey || null;
      }
      if (!targetRequirementId || !requirementById[targetRequirementId]) return;

      const allLinks = Object.values(basketLinkById || {});
      const byReq = {};
      allLinks.forEach((x) => {
        byReq[x.requirement_id] = byReq[x.requirement_id] || [];
        byReq[x.requirement_id].push({ ...x, id: x.id });
      });
      const sourceRequirementId = dragged.requirement_id;
      const sourceRows = [...(byReq[sourceRequirementId] || [])];
      const targetRows = sourceRequirementId === targetRequirementId ? sourceRows : [...(byReq[targetRequirementId] || [])];
      const sourceWithoutDragged = sourceRows.filter((r) => r.id !== dragLinkId);
      const targetWithoutDragged =
        sourceRequirementId === targetRequirementId ? sourceWithoutDragged : targetRows.filter((r) => r.id !== dragLinkId);

      let insertIndex = targetWithoutDragged.length;
      if (dropIsBasket) {
        const dropLinkId = dropKey.split(":")[1] || "";
        const idx = targetWithoutDragged.findIndex((r) => r.id === dropLinkId);
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
          payload.push({ link_id: r.id, requirement_id: sourceRequirementId, sort_order: i })
        );
      }
      nextTargetRows.forEach((r, i) =>
        payload.push({ link_id: r.id, requirement_id: targetRequirementId, sort_order: i })
      );

      await authed("/requirements/baskets/reorder", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["requirements-tree", selectedVersion?.id] }),
        qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion?.id] }),
      ]);
      return;
    }

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

    if (dropIsCourse || dropIsBasket || dropIsBasketCourse) return;
    const treeData = (requirementsTreeQ.data?.tree || []).map(function mapNode(n) {
      return {
        key: n.id,
        title: `${n.node_code ? `${withDot(n.node_code)} ` : ""}${formatRequirementName(n.name, n.logic_type, n.pick_n, requirementOptionTotal(n))} (${n.logic_type})`,
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
    const logicTypeRaw = raw?.logic_type || n?.logic_type || "ALL_REQUIRED";
    const logicType = String(logicTypeRaw).toUpperCase() === "ANY_ONE" ? "PICK_N" : logicTypeRaw;
    const pickN = (String(logicTypeRaw).toUpperCase() === "ANY_ONE")
      ? 1
      : (raw?.pick_n ?? n?.pick_n);
    const optionSlotKey = raw?.option_slot_key ?? n?.option_slot_key ?? "";
    const optionSlotCapacity = raw?.option_slot_capacity ?? n?.option_slot_capacity ?? 1;
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
    setEditReqOptionSlotKey(optionSlotKey);
    setEditReqOptionSlotCapacity(String(optionSlotCapacity || 1));
    setEditReqMajorName(inferredMajorName);
    setEditReqDivision(programDivision);
    setEditReqCoreTrack(inferredCoreTrack);
  }

  function handleRequirementTreeSelect(keys, info) {
    const key = info?.node?.key || keys?.[0];
    if (
      !key
      || String(key).startsWith("course:")
      || String(key).startsWith("basket:")
      || String(key).startsWith("basket-course:")
    ) return;
    loadRequirementNodeToEditor(String(key));
  }

  const nodeLabel = useMemo(() => {
    const map = {};
    for (const n of prereqGraphQ.data?.nodes || []) map[n.id] = n.label;
    return map;
  }, [prereqGraphQ.data]);
  const courseLabelById = useMemo(() => {
    const map = {};
    for (const c of coursesQ.data || []) {
      map[c.id] = `${c.course_number}${c.title ? ` - ${c.title}` : ""}`;
    }
    return map;
  }, [coursesQ.data]);
  const courseTitleByNumber = useMemo(() => {
    const map = {};
    for (const c of coursesQ.data || []) {
      map[normalizeCourseNumber(c.course_number)] = String(c.title || "");
    }
    return map;
  }, [coursesQ.data]);
  function renderCourseCodeWithHover(courseNumber) {
    const code = String(courseNumber || "").trim();
    if (!code) return <span>Missing course</span>;
    const title = courseTitleByNumber[normalizeCourseNumber(code)];
    const enabled = courseTitleHoverEnabled || canvasViewMode === "VERBOSE";
    if (!enabled || !title) return <span>{code}</span>;
    return (
      <Tooltip title={title}>
        <span>{code}</span>
      </Tooltip>
    );
  }
  function renderCourseCodeSeries(courseNumbers) {
    const nums = (courseNumbers || []).map((n) => String(n || "").trim()).filter(Boolean);
    if (!nums.length) return <span>Missing course</span>;
    return (
      <span>
        {nums.map((n, idx) => (
          <span key={`${n}-${idx}`}>
            {idx > 0 ? " / " : ""}
            {renderCourseCodeWithHover(n)}
          </span>
        ))}
      </span>
    );
  }
  const courseAutoOptions = useMemo(
    () => (coursesQ.data || []).map((c) => ({ value: `${c.course_number} - ${c.title}`, courseId: c.id })),
    [coursesQ.data]
  );
  const requirementLinkOptions = useMemo(() => {
    const out = [];
    function walk(nodes) {
      for (const n of nodes || []) {
        const category = String(n.category || "").toUpperCase();
        const prefixParts = [];
        if (n.node_code) prefixParts.push(withDot(n.node_code));
        if (category === "CORE") prefixParts.push("Core");
        if (category === "MAJOR") prefixParts.push(`Major${n.program_name ? ` - ${n.program_name}` : ""}`);
        if (category === "MINOR") prefixParts.push(`Minor${n.program_name ? ` - ${n.program_name}` : ""}`);
        if (category === "PE") prefixParts.push("PE");
        const prefix = prefixParts.filter(Boolean).join(" | ");
        out.push({ value: n.id, label: `${prefix} | ${formatRequirementName(n.name, n.logic_type, n.pick_n, requirementOptionTotal(n))}` });
        walk(n.children || []);
      }
    }
    walk(requirementsTreeQ.data?.tree || []);
    return out;
  }, [requirementsTreeQ.data]);
  const prereqEdgesEnriched = useMemo(() => {
    const edges = prereqGraphQ.data?.edges || [];
    const groupedCounts = {};
    for (const e of edges) {
      const gk = String(e.prerequisite_group_key || "").trim();
      if (!gk) continue;
      const rel = String(e.relationship_type || "PREREQUISITE").toUpperCase();
      const key = `${e.to}|${rel}|${gk}`;
      groupedCounts[key] = (groupedCounts[key] || 0) + 1;
    }
    return edges.map((e) => {
      const fromLabel = nodeLabel[e.from] || e.from;
      const toLabel = nodeLabel[e.to] || e.to;
      const rel = String(e.relationship_type || "PREREQUISITE").toUpperCase();
      const gk = String(e.prerequisite_group_key || "").trim();
      const groupCount = gk ? groupedCounts[`${e.to}|${rel}|${gk}`] || 0 : 0;
      const minReq = Number(e.group_min_required || 1);
      const groupDisplay = gk
        ? `${e.group_label || "Disjunction"} (${Math.min(minReq, Math.max(1, groupCount))}/${Math.max(1, groupCount)})`
        : "All Required";
      return {
        ...e,
        from_label: fromLabel,
        to_label: toLabel,
        group_display: groupDisplay,
        _group_count: groupCount,
        _group_key: gk,
      };
    });
  }, [prereqGraphQ.data, nodeLabel]);
  const prereqFromOptions = useMemo(() => {
    const labels = Array.from(new Set(prereqEdgesEnriched.map((e) => e.from_label).filter(Boolean))).sort();
    return labels.map((x) => ({ value: x, label: x }));
  }, [prereqEdgesEnriched]);
  const prereqToOptions = useMemo(() => {
    const labels = Array.from(new Set(prereqEdgesEnriched.map((e) => e.to_label).filter(Boolean))).sort();
    return labels.map((x) => ({ value: x, label: x }));
  }, [prereqEdgesEnriched]);
  const prereqEdgesFiltered = useMemo(() => {
    const fromSearch = String(prereqFromQuery || "").trim().toLowerCase();
    const toSearch = String(prereqToQuery || "").trim().toLowerCase();
    return prereqEdgesEnriched.filter((e) => {
      if (fromSearch && !String(e.from_label || "").toLowerCase().includes(fromSearch)) return false;
      if (toSearch && !String(e.to_label || "").toLowerCase().includes(toSearch)) return false;
      return true;
    });
  }, [prereqEdgesEnriched, prereqFromQuery, prereqToQuery]);

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
    if (exact != null) return `${periodShortLabel(exact)} fixed`;
    const parts = [];
    if (min) parts.push(`>= ${periodShortLabel(min)}`);
    if (max) parts.push(`<= ${periodShortLabel(max)}`);
    return parts.join(", ");
  };
  const coreRuleTimingRules = (row) =>
    timingRulesFromFields(row?.required_semester || null, row?.required_semester_min || null, row?.required_semester_max || null);
  const allowedSemestersForTiming = (timing) => {
    const allowed = new Set();
    for (const s of planPeriods) {
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
  const basketLinkById = useMemo(() => {
    const out = {};
    Object.values(requirementNodeMap || {}).forEach((n) => {
      (n.baskets || []).forEach((b) => {
        out[b.id] = { ...b, requirement_id: n.id };
      });
    });
    return out;
  }, [requirementNodeMap]);
  const basketCourseItemById = useMemo(() => {
    const out = {};
    Object.values(basketLinkById || {}).forEach((b) => {
      (b.courses || []).forEach((c) => {
        out[c.id] = { ...c, basket_link_id: b.id, basket_id: b.basket_id, requirement_id: b.requirement_id };
      });
    });
    return out;
  }, [basketLinkById]);
  const basketValidationErrors = useMemo(() => {
    if (reqLinkKind !== "BASKET") return [];
    const errors = [];
    const uniqueCourseIds = Array.from(new Set((basketCourseIds || []).filter(Boolean)));
    const idSet = new Set(uniqueCourseIds);
    const parsedMin = Number(basketMinCount || 1);
    const minCount = Number.isFinite(parsedMin) && parsedMin > 0 ? Math.floor(parsedMin) : 0;
    if (!minCount) errors.push("Min count must be a whole number greater than 0.");
    if (!uniqueCourseIds.length) errors.push("Select at least one basket course.");
    if (minCount && uniqueCourseIds.length && minCount > uniqueCourseIds.length) {
      errors.push("Min count cannot exceed the number of selected basket courses.");
    }
    const usingExisting = !!basketSelectedId;
    if (!usingExisting && !(basketName || "").trim()) {
      errors.push("Basket name is required when not using an existing basket.");
    }
    const siblingLinks = (requirementNodeMap[basketRequirementId || ""]?.baskets || []);
    if (basketSelectedId && siblingLinks.some((b) => b.basket_id === basketSelectedId && b.id !== basketLinkId)) {
      errors.push("This basket is already linked to the selected requirement.");
    }
    const seenPairs = new Set();
    (basketSubGroupRows || []).forEach((row, idx) => {
      const primary = row.primary_course_id;
      const subs = Array.from(new Set((row.substitute_course_ids || []).filter(Boolean)));
      if (!primary && !subs.length) return;
      if (!primary || !idSet.has(primary)) {
        errors.push(`Substitute group ${idx + 1}: select a primary course from Basket courses.`);
        return;
      }
      if (!subs.length) {
        errors.push(`Substitute group ${idx + 1}: select at least one substitute.`);
        return;
      }
      for (const sid of subs) {
        if (sid === primary) {
          errors.push(`Substitute group ${idx + 1}: primary course cannot be its own substitute.`);
          continue;
        }
        if (!idSet.has(sid)) {
          errors.push(`Substitute group ${idx + 1}: substitute courses must be selected in Basket courses.`);
          continue;
        }
        const key = [primary, sid].sort().join("|");
        if (seenPairs.has(key)) {
          errors.push(`Substitute group ${idx + 1}: duplicate substitute pair detected.`);
        } else {
          seenPairs.add(key);
        }
      }
    });
    return errors;
  }, [
    reqLinkKind,
    basketCourseIds,
    basketMinCount,
    basketSelectedId,
    basketName,
    requirementNodeMap,
    basketRequirementId,
    basketLinkId,
    basketSubGroupRows,
  ]);
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
            <span>{`${n.node_code ? `${withDot(n.node_code)} ` : ""}${formatRequirementName(n.name, n.logic_type, n.pick_n, requirementOptionTotal(n))}`}</span>
            <Space size={4} className="tree-node-actions">
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
                Add Course/Basket
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
      const basketLeaves = (n.baskets || []).map((b) => ({
        key: `basket:${b.id}`,
        title: (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, width: "100%" }}>
            <span>{`${b.basket_name || "Basket"} (${b.min_count || 1}/${(b.courses || []).length})`}</span>
            <Space className="tree-node-actions">
              <Tag color="blue">Basket</Tag>
              <Button
                size="small"
                onClick={(e) => {
                  e.stopPropagation();
                  openRequirementBasketModal(n.id, b);
                }}
              >
                Edit
              </Button>
              <Button
                size="small"
                danger
                onClick={(e) => {
                  e.stopPropagation();
                  deleteRequirementBasket(b.id, b.basket_id);
                }}
              >
                Delete
              </Button>
            </Space>
          </div>
        ),
        children: (() => {
          const courses = [...(b.courses || [])];
          if (!courses.length) return [];
          const byCourseId = {};
          courses.forEach((bc) => {
            byCourseId[bc.course_id] = bc;
          });
          const setIds = new Set(courses.map((bc) => bc.course_id).filter(Boolean));
          const adj = {};
          const basketSubRows = (b.substitutions || []).filter(
            (s) => setIds.has(s.primary_course_id) && setIds.has(s.substitute_course_id)
          );
          const fallbackReqSubRows = (requirementSubstitutionsVersionQ.data || []).filter(
            (s) => s.requirement_id === n.id && setIds.has(s.primary_course_id) && setIds.has(s.substitute_course_id)
          );
          const subRows = basketSubRows.length ? basketSubRows : fallbackReqSubRows;
          for (const s of subRows) {
            if (!adj[s.primary_course_id]) adj[s.primary_course_id] = new Set();
            if (!adj[s.substitute_course_id]) adj[s.substitute_course_id] = new Set();
            adj[s.primary_course_id].add(s.substitute_course_id);
            adj[s.substitute_course_id].add(s.primary_course_id);
          }
          const seen = new Set();
          const groups = [];
          for (const bc of courses) {
            const cid = bc.course_id;
            if (!cid || seen.has(cid)) continue;
            if (!adj[cid] || !adj[cid].size) {
              seen.add(cid);
              groups.push([cid]);
              continue;
            }
            const q = [cid];
            const comp = [];
            seen.add(cid);
            while (q.length) {
              const cur = q.shift();
              comp.push(cur);
              for (const nxt of Array.from(adj[cur] || [])) {
                if (seen.has(nxt)) continue;
                seen.add(nxt);
                q.push(nxt);
              }
            }
            groups.push(comp);
          }
          const displayRows = groups.map((groupIds, groupIdx) => {
              const orderedIds = [...groupIds].sort((a, b2) =>
                String(courseMapById[a]?.course_number || courseMapById[a]?.title || a)
                  .localeCompare(String(courseMapById[b2]?.course_number || courseMapById[b2]?.title || b2), undefined, { sensitivity: "base" })
              );
              const titleParts = orderedIds.map((cid) => {
                const item = byCourseId[cid];
                return item?.course_number || item?.course_title || courseMapById[cid]?.course_number || courseMapById[cid]?.title || "Missing course";
              });
              const firstItem = byCourseId[orderedIds[0]];
              return {
                key: `basket-course:${b.id}:${firstItem?.id || groupIdx}`,
                sortLabel: titleParts.join(" / "),
                title: renderCourseCodeSeries(titleParts),
                isLeaf: true,
                selectable: false,
                disableCheckbox: true,
              };
            });
          return displayRows
            .sort((a, b2) => String(a.sortLabel || "").localeCompare(String(b2.sortLabel || ""), undefined, { sensitivity: "base" }))
            .map(({ sortLabel, ...row }) => row);
        })(),
        disableCheckbox: true,
        selectable: false,
      }));
      const sourceCourses =
        (mappedByReq[n.id] && mappedByReq[n.id].length ? mappedByReq[n.id] : null) ||
        (n.courses && n.courses.length ? n.courses : []);
      const sortedSourceCourses = [...sourceCourses].sort((a, b) =>
        String(a?.course_number || a?.course_title || "")
          .localeCompare(String(b?.course_number || b?.course_title || ""), undefined, { sensitivity: "base" })
      );
      const courseLeaves = sortedSourceCourses.map((c) => ({
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
                const rendered = orderedIds
                  .filter((id) => {
                    if (!id || seen.has(id)) return false;
                    seen.add(id);
                    return true;
                  })
                  .map((id) => courseMapById[id]?.course_number || courseMapById[id]?.title || "Missing course");
                return renderCourseCodeSeries(rendered);
              })()}
            </span>
            {formatSemesterConstraint(c) ? <Tag>{formatSemesterConstraint(c)}</Tag> : null}
            <Space className="tree-node-actions">
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
                <span>
                  {(() => {
                    const srcReq = requirementById[g.source_requirement_id];
                    const srcName = srcReq
                      ? formatRequirementName(srcReq.name, srcReq.logic_type, srcReq.pick_n, srcReq.available_option_count || srcReq.direct_course_count)
                      : formatRequirementName(
                        g.name || `Core Rule ${idx + 1}`,
                        requirementById[g.source_requirement_id]?.logic_type,
                        requirementById[g.source_requirement_id]?.pick_n
                      );
                    const slotTotal = Math.max(1, Number(srcReq?.pick_n || 1));
                    const showChoice = slotTotal > 1 && Number.isFinite(Number(g.slot_index));
                    const slotLabel = showChoice ? ` - Choice ${Number(g.slot_index) + 1}` : "";
                    return `${withDot(`${n.node_code || "R1"}.C1.R${idx + 1}`)} ${srcName}${slotLabel}: `;
                  })()}
                  {renderCourseCodeSeries(normalizeCoreRuleCourseNumbers(g.course_numbers || []))}
                </span>
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
              <span>{`${withDot(`${n.node_code || "R1"}.C1`)} Core Rules`}</span>
              <Space className="tree-node-actions">
                <Button
                  size="small"
                  onClick={(e) => {
                    e.stopPropagation();
                    openCoreRulesBuilder(n.program_id, n.program_name);
                  }}
                >
                  Edit
                </Button>
                <Button
                  size="small"
                  danger
                  onClick={(e) => {
                    e.stopPropagation();
                    deleteCoreRulesRule(n.program_id, n.program_name);
                  }}
                >
                  Delete
                </Button>
              </Space>
            </div>
          ),
          children: groupLeaves,
          disableCheckbox: true,
          selectable: false,
          draggable: false,
        };
      }
      const leafNodes = [...basketLeaves, ...courseLeaves];
      reqNode.children = coreRuleNode
        ? [coreRuleNode, ...leafNodes, ...childReqs]
        : [...leafNodes, ...childReqs];
      return reqNode;
    }
    return filteredTree.map(mapNode);
  }, [filteredTree, requirementFulfillmentVersionQ.data, requirementSubstitutionsVersionQ.data, courseMapById, courseIdByNumber, requirementById, validationRulesQ.data, treeExpandedKeys]);
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
    const basketSubsByReq = {};
    const courseNumberFallbackById = {};
    for (const f of requirementFulfillmentVersionQ.data || []) {
      coursesByReq[f.requirement_id] = coursesByReq[f.requirement_id] || [];
      coursesByReq[f.requirement_id].push(f.course_id);
      if (f.course_id && f.course_number) courseNumberFallbackById[f.course_id] = f.course_number;
    }
    const walkReqTree = (nodes) => {
      for (const n of nodes || []) {
        const rid = n?.id || n?.requirement_id;
        if (rid) {
          for (const b of n.baskets || []) {
            for (const c of b.courses || []) {
              if (!c?.course_id) continue;
              coursesByReq[rid] = coursesByReq[rid] || [];
              coursesByReq[rid].push(c.course_id);
              if (c.course_id && c.course_number) courseNumberFallbackById[c.course_id] = c.course_number;
            }
            for (const s of b.substitutions || []) {
              if (!s?.primary_course_id || !s?.substitute_course_id) continue;
              basketSubsByReq[rid] = basketSubsByReq[rid] || [];
              basketSubsByReq[rid].push({
                primary_course_id: s.primary_course_id,
                substitute_course_id: s.substitute_course_id,
              });
            }
          }
        }
        walkReqTree(n.children || []);
      }
    };
    walkReqTree(requirementsTreeQ.data?.tree || []);
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
      for (const s of [...(subsByReq[reqId] || []), ...(basketSubsByReq[reqId] || [])]) {
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
        const ordered = group.sort((a, b) =>
          String(courseMapById[a]?.course_number || courseNumberFallbackById[a] || courseMapById[a]?.title || "").localeCompare(
            String(courseMapById[b]?.course_number || courseNumberFallbackById[b] || courseMapById[b]?.title || "")
          )
        );
        const label = ordered
          .map((cid) => courseMapById[cid]?.course_number || courseNumberFallbackById[cid] || courseMapById[cid]?.title || cid)
          .join(" / ");
        groups.push({ value: ordered[0], label, group_course_ids: ordered });
      }
      byReq[reqId] = groups.sort((a, b) => a.label.localeCompare(b.label));
    }
    return byReq;
  }, [requirementFulfillmentVersionQ.data, requirementSubstitutionsVersionQ.data, requirementsTreeQ.data, courseMapById]);
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
    for (const m of coreRulesReqMeta || []) {
      map[m.requirement_id] = {
        requirement_id: m.requirement_id,
        requirement_name: m.requirement_name,
        slot_total: Math.max(1, Number(m.slot_total || 1)),
        restrict_to_sub_groups: !!m.restrict_to_sub_groups,
        rows: [],
      };
      out.push(map[m.requirement_id]);
    }
    for (const r of coreRulesRows || []) {
      const key = r.requirement_id;
      if (!map[key]) {
        map[key] = {
          requirement_id: r.requirement_id,
          requirement_name: r.requirement_name,
          slot_total: Math.max(1, Number(r.slot_total || 1)),
          restrict_to_sub_groups: !!r.restrict_to_sub_groups,
          rows: [],
        };
        out.push(map[key]);
      }
      map[key].rows.push(r);
    }
    out.forEach((g) => {
      g.rows.sort((a, b) => Number(a.slot_index || 0) - Number(b.slot_index || 0));
    });
    return out;
  }, [coreRulesRows, coreRulesReqMeta]);
  const validationRulesWithDomain = useMemo(() => {
    return (validationRulesQ.data || [])
      .filter((r) => {
        const cfg = parseRuleConfig(r);
        const t = String(cfg.type || "").toUpperCase();
        return !["MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"].includes(t);
      })
      .map((r) => ({ ...r, domain: inferValidationDomain(r) }));
  }, [validationRulesQ.data]);
  const validationRuleMetaByCode = useMemo(() => {
    const out = {};
    for (const r of validationRulesWithDomain || []) {
      const key = String(r.rule_code || "").trim();
      if (!key) continue;
      out[key] = { name: r.name || "", domain: r.domain || "General" };
    }
    return out;
  }, [validationRulesWithDomain]);
  const validationRuleMetaByName = useMemo(() => {
    const out = {};
    for (const r of validationRulesWithDomain || []) {
      const key = String(r.name || "").trim();
      if (!key) continue;
      out[key] = { code: r.rule_code || "", domain: r.domain || "General" };
    }
    return out;
  }, [validationRulesWithDomain]);
  const filteredValidationRules = useMemo(
    () =>
      validationDomainFilter === "ALL"
        ? validationRulesWithDomain
        : validationRulesWithDomain.filter((r) => r.domain === validationDomainFilter),
    [validationDomainFilter, validationRulesWithDomain]
  );
  const validationCategoryOptions = useMemo(() => {
    const defaults = ["Curriculum Integrity", "Definitional", "General", "Program/Major Pathway", "Resources"];
    const seen = new Set(defaults);
    for (const r of validationRulesWithDomain) {
      const d = String(r.domain || "").trim();
      if (d) seen.add(d);
    }
    return Array.from(seen)
      .sort((a, b) => a.localeCompare(b))
      .map((d) => ({ value: d }));
  }, [validationRulesWithDomain]);
  const validationDomainFilterOptions = useMemo(
    () => [{ value: "ALL", label: "All Rules" }, ...validationCategoryOptions.map((o) => ({ value: o.value, label: o.value }))],
    [validationCategoryOptions]
  );
  const orderedValidationRules = useMemo(
    () => [...filteredValidationRules].sort(compareRuleOrder),
    [filteredValidationRules]
  );
  const validationTopKeys = useMemo(() => {
    const domains = new Set(orderedValidationRules.map((r) => r.domain || "General"));
    return Array.from(domains).sort().map((d) => `vdom:${d}`);
  }, [orderedValidationRules]);
  const validationAllKeys = useMemo(() => {
    const out = [...validationTopKeys];
    for (const r of orderedValidationRules) out.push(`vrule:${r.id}`);
    return out;
  }, [orderedValidationRules, validationTopKeys]);
  const validationTreeData = useMemo(() => {
    const grouped = {};
    for (const r of orderedValidationRules) {
      const d = r.domain || "General";
      grouped[d] = grouped[d] || [];
      grouped[d].push(r);
    }
    return Object.entries(grouped)
      .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
      .map(([domain, rules]) => ({
        key: `vdom:${domain}`,
        title: `${domain} (${rules.length})`,
        selectable: false,
        children: rules.map((r) => ({
          key: `vrule:${r.id}`,
          isLeaf: true,
          selectable: false,
          draggable: true,
          title: (
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, width: "100%" }}>
              <Space>
                <Typography.Text>{`${r.rule_code ? `${r.rule_code}. ` : ""}${r.name}`}</Typography.Text>
              </Space>
              <Space className="tree-node-actions">
                <Button size="small" onClick={() => toggleValidationRule(r.id, r.active)}>
                  {r.active ? "Disable" : "Enable"}
                </Button>
                <Button size="small" onClick={() => openEditRuleModal(r)}>
                  Edit
                </Button>
                <Button size="small" danger onClick={() => deleteValidationRule(r.id)}>
                  Delete
                </Button>
              </Space>
            </div>
          ),
        })),
      }));
  }, [orderedValidationRules]);
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
    const vid = selectedVersion?.id || null;
    if (!vid) return;
    if (treeExpandInitVersionRef.current === vid) return;
    if (!(filteredTree || []).length) return;
    treeExpandInitVersionRef.current = vid;
    // Initialize once per version; do not auto-expand on local tree mutations.
    setTreeExpandedKeys((filteredTree || []).map((n) => n.id));
  }, [selectedVersion?.id, filteredTree]);

  useEffect(() => {
    if (!selectedVersion?.id) return;
    qc.invalidateQueries({ queryKey: ["design-checklist", selectedVersion.id] });
  }, [requirementsTreeQ.dataUpdatedAt, selectedVersion?.id]);
  useEffect(() => {
    if (!selectedVersion?.id) return;
    qc.invalidateQueries({ queryKey: ["design-feasibility", selectedVersion.id] });
  }, [requirementsTreeQ.dataUpdatedAt, validationRulesQ.dataUpdatedAt, selectedVersion?.id]);
  useEffect(() => {
    setValidationTreeExpandedKeys(validationTopKeys);
  }, [validationTopKeys]);

  const isReqEditMode = !!selectedRuleNodeId;
  const isSubNodeCreate = !isReqEditMode && reqScopeLocked && !!editReqParentId;
  const isEditingSubNode = isReqEditMode && !!editReqParentId;
  function hasTimingIssue(node) {
    if ((node?.fixed_semester_violations || []).length) return true;
    for (const child of node?.children || []) {
      if (hasTimingIssue(child)) return true;
    }
    return false;
  }

  function renderChecklist(nodes, level = 0) {
    return (
      <div style={{ display: "flex", flexDirection: "column" }}>
        {(nodes || []).map((r) => (
          <div key={`${r.requirement_id}-${level}`} style={{ marginLeft: level * 14, padding: "2px 0" }}>
            <Space style={{ whiteSpace: "nowrap" }}>
              <span style={{ display: "inline-block", width: 62 }}>
                <Tag style={STATUS_TAG_STYLE_SHORT} color={r.is_satisfied && !hasTimingIssue(r) ? "green" : "red"}>
                  {r.is_satisfied && !hasTimingIssue(r) ? "MET" : "OPEN"}
                </Tag>
              </span>
              {String(r.requirement_id || "").startsWith("core-rule:")
                ? (
                  <>
                    <Typography.Text strong={level === 0}>{`${r.node_code ? `${withDot(r.node_code)} ` : ""}${formatRequirementName(r.name, r.logic_type, r.pick_n, r.available_option_count || r.direct_course_count)}`}</Typography.Text>
                    <Typography.Text type="secondary">
                      {r.satisfied_units}/{r.required_units}
                    </Typography.Text>
                  </>
                )
                : (
                  <>
                    <Typography.Text strong={level === 0}>{`${r.node_code ? `${withDot(r.node_code)} ` : ""}${formatRequirementName(r.name, r.logic_type, r.pick_n, r.available_option_count || r.direct_course_count)}`}</Typography.Text>
                    <Typography.Text type="secondary">
                      {r.satisfied_units}/{r.required_units}
                    </Typography.Text>
                  </>
                )}
              {(r.fixed_semester_violations || []).length ? (
                <Typography.Text type="danger">{(r.fixed_semester_violations || []).join(" | ")}</Typography.Text>
              ) : null}
            </Space>
            {r.children?.length ? <div>{renderChecklist(r.children, level + 1)}</div> : null}
          </div>
        ))}
      </div>
    );
  }
  function validationColor(status) {
    const token = normalizeStatusToken(status, "PASS");
    if (token === "FAIL") return "red";
    if (token === "WARN") return "orange";
    return "green";
  }
  function formatPrereqType(v) {
    return String(v || "").toUpperCase() === "COREQUISITE" ? "Corequisite" : "Prerequisite";
  }
  function formatEnforcement(v) {
    return String(v || "").toUpperCase() === "SOFT" ? "Soft" : "Hard";
  }
  function renderValidationLineItems(items, emptyText = "None") {
    const rows = (items || []).map((it, idx) => {
      const code = String(it?.rule_code || "").trim();
      const name = String(it?.rule_name || "").trim();
      const metaByCode = validationRuleMetaByCode[code];
      const metaByName = validationRuleMetaByName[name];
      const domain = String(
        metaByCode?.domain ||
        metaByName?.domain ||
        inferValidationDomain({ name, config_json: "{}" }) ||
        "General"
      );
      return { ...(it || {}), _idx: idx, _domain: domain };
    });
    if (!rows.length) return <Typography.Text type="secondary">{emptyText}</Typography.Text>;
    const grouped = {};
    for (const row of rows) {
      const key = row._domain || "General";
      grouped[key] = grouped[key] || [];
      grouped[key].push(row);
    }
    const domains = Object.keys(grouped).sort((a, b) => {
      const ia = VALIDATION_DOMAIN_ORDER.indexOf(a);
      const ib = VALIDATION_DOMAIN_ORDER.indexOf(b);
      const va = ia >= 0 ? ia : 999;
      const vb = ib >= 0 ? ib : 999;
      if (va !== vb) return va - vb;
      return String(a).localeCompare(String(b));
    });
    const bucketLabelByCode = {
      NONACAD_PE: "Physical Education",
      NONACAD_LEADERSHIP: "Leadership Sequence",
      NONACAD_MIL_TRAINING: "Military Training",
      NONACAD_AIRMANSHIP: "Airmanship",
      NONACAD_ATHLETICS: "Athletics",
      ABET_MATH_BASIC_SCI: "ABET Math/Basic Science",
      ABET_ENGINEERING_TOPICS: "ABET Engineering Topics",
    };
    function friendlyRuleLabel(raw) {
      const t = String(raw || "").trim();
      return t
        .replace(/^Non-Academic:\s*/i, "")
        .replace(/^Program\/Major Pathway:\s*/i, "")
        .replace(/^Program\/Major Pathway Definition:\s*/i, "")
        .replace(/^ABET EAC:\s*/i, "ABET: ")
        .replace(/\s+/g, " ");
    }
    function friendlyMessage(raw) {
      const msg = String(raw || "").trim();
      if (!msg) return "";
      const creditsMatch = msg.match(/^([A-Z0-9_]+)\s+credits\s+([0-9]+(?:\.[0-9]+)?);\s+minimum\s+([0-9]+(?:\.[0-9]+)?)\.?$/i);
      if (creditsMatch) {
        const code = String(creditsMatch[1] || "").toUpperCase();
        const actual = creditsMatch[2];
        const min = creditsMatch[3];
        const label = bucketLabelByCode[code] || code;
        return `${label} credits ${actual}/${min}.`;
      }
      const coursesMatch = msg.match(/^([A-Z0-9_]+)\s+courses\s+([0-9]+(?:\.[0-9]+)?);\s+minimum\s+([0-9]+(?:\.[0-9]+)?)\.?$/i);
      if (coursesMatch) {
        const code = String(coursesMatch[1] || "").toUpperCase();
        const actual = coursesMatch[2];
        const min = coursesMatch[3];
        const label = bucketLabelByCode[code] || code;
        return `${label} courses ${actual}/${min}.`;
      }
      let out = msg;
      for (const [code, label] of Object.entries(bucketLabelByCode)) {
        out = out.replaceAll(code, label);
      }
      const trimmed = out.replace(/\s+/g, " ").trim().replace(/(?:\s*\.)+$/, "");
      const normalized = trimmed.replace(/\.{2,}/g, ".");
      return normalized ? `${normalized}.` : "";
    }
    return (
      <Space direction="vertical" style={{ width: "100%" }}>
        {domains.map((domain) => (
          <div key={`validation-domain-${domain}`}>
            <Typography.Text strong>{domain}</Typography.Text>
            <List
              size="small"
              dataSource={grouped[domain]}
              renderItem={(it) => (
                <List.Item style={{ paddingLeft: 14 }}>
                  <Space style={{ whiteSpace: "nowrap" }}>
                    <Tag style={STATUS_TAG_STYLE_SHORT} color={validationColor(it.status || "PASS")}>{normalizeStatusToken(it.status || "PASS")}</Tag>
                    <Typography.Text>{`${it.rule_code ? `${it.rule_code}. ` : ""}${friendlyRuleLabel(it.rule_name || "Validation Rule")}`}</Typography.Text>
                    {it.message ? <Typography.Text type={normalizeStatusToken(it.status || "PASS") === "FAIL" ? "danger" : "secondary"}>{friendlyMessage(it.message)}</Typography.Text> : null}
                  </Space>
                </List.Item>
              )}
            />
          </div>
        ))}
      </Space>
    );
  }
  function resolveCourseIdFromToken(token) {
    const raw = String(token || "").trim();
    if (!raw) return undefined;
    if (courseMapById[raw]) return raw;
    const byExact = courseIdByNumber[raw];
    if (byExact) return byExact;
    const byNorm = courseIdByNormalizedNumber[normalizeCourseNumber(raw)];
    if (byNorm) return byNorm;
    return undefined;
  }
  function normalizeCoreRuleCourseNumbers(tokens) {
    return (tokens || [])
      .map((t) => String(t || "").trim())
      .filter(Boolean)
      .map((t) => {
        const cid = resolveCourseIdFromToken(t);
        return cid ? (courseMapById[cid]?.course_number || t) : t;
      });
  }
  function inferCoreRequirementIdFromCourseIds(courseIds) {
    const ids = Array.from(new Set((courseIds || []).filter(Boolean)));
    if (!ids.length) return null;
    let bestReq = null;
    let bestScore = -1;
    for (const [reqId, opts] of Object.entries(coreRuleChoiceOptionsByReq || {})) {
      const allowed = new Set((opts || []).flatMap((o) => o.group_course_ids || []));
      const score = ids.filter((id) => allowed.has(id)).length;
      if (score > bestScore) {
        bestScore = score;
        bestReq = reqId;
      }
    }
    return bestScore > 0 ? bestReq : null;
  }
  function isFullChoiceGroupSelection(reqId, selectedIds) {
    if (!reqId) return false;
    const sel = new Set((selectedIds || []).filter(Boolean));
    if (sel.size <= 1) return false;
    for (const o of coreRuleChoiceOptionsByReq[reqId] || []) {
      const grp = new Set((o.group_course_ids || []).filter(Boolean));
      if (grp.size > 1 && grp.size === sel.size) {
        let same = true;
        for (const id of grp) {
          if (!sel.has(id)) {
            same = false;
            break;
          }
        }
        if (same) return true;
      }
    }
    return false;
  }
  function requirementOptionTotal(node) {
    if (!node) return 0;
    const mappedRows = (requirementFulfillmentVersionQ.data || []).filter((m) => m.requirement_id === node.id);
    const directIds = new Set(
      ((mappedRows.length ? mappedRows : (node.courses || [])) || [])
        .map((c) => c?.course_id || c?.courseId)
        .filter(Boolean)
    );
    const basketIds = new Set(
      (node.baskets || [])
        .flatMap((b) => (b.courses || []).map((c) => c?.course_id))
        .filter(Boolean)
    );
    const total = new Set([...directIds, ...basketIds]).size;
    if (total > 0) return total;
    return Array.isArray(node.children) ? node.children.length : 0;
  }
  function renderConsistencyTree(nodes, level = 0) {
    return (
      <div style={{ display: "flex", flexDirection: "column" }}>
        {(nodes || []).map((n, idx) => (
          <div key={`${n.requirement_id || n.node_code || idx}-${level}`} style={{ marginLeft: level * 14, padding: "2px 0" }}>
            <Space style={{ whiteSpace: "nowrap" }}>
              <span style={{ display: "inline-block", width: 96 }}>
                <Tag style={STATUS_TAG_STYLE_LONG} color={n.status === "INCONSISTENT" ? "red" : "green"}>
                  {n.status === "INCONSISTENT" ? "INCONSISTENT" : "CONSISTENT"}
                </Tag>
              </span>
              <Typography.Text>{`${n.node_code ? `${withDot(n.node_code)} ` : ""}${formatRequirementName(n.name, n.logic_type, n.pick_n, n.available_option_count || n.direct_course_count)}`}</Typography.Text>
              {n.message ? <Typography.Text type="danger">{n.message}</Typography.Text> : null}
            </Space>
            {n.children?.length ? <div>{renderConsistencyTree(n.children, level + 1)}</div> : null}
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
      width: 108,
      render: (v) => {
        const raw = String(v || "").toUpperCase();
        if (raw === "MAJOR") return "Major";
        if (raw === "MINOR") return "Minor";
        if (raw === "DOUBLE_MAJOR") return "Double Major";
        if (raw === "DOUBLE_MINOR") return "Double Minor";
        if (raw === "MAJOR_MINOR") return "Major/Minor";
        return String(v || "")
          .toLowerCase()
          .replaceAll("_", " ")
          .replace(/\b\w/g, (m) => m.toUpperCase());
      },
      filters: [
        { text: "Major", value: "MAJOR" },
        { text: "Minor", value: "MINOR" },
        { text: "Double Major", value: "DOUBLE_MAJOR" },
        { text: "Double Minor", value: "DOUBLE_MINOR" },
        { text: "Major/Minor", value: "MAJOR_MINOR" },
      ],
      onFilter: (value, row) => String(row.kind || "").toUpperCase() === String(value || "").toUpperCase(),
      sorter: (a, b) => String(a.kind || "").localeCompare(String(b.kind || "")),
    },
    {
      title: "Combination",
      dataIndex: "label",
      key: "label",
      width: 250,
      render: (v) =>
        String(v || "")
          .replaceAll("Double Major - ", "")
          .replaceAll("Double Minor - ", "")
          .replaceAll("Major/Minor - ", "")
          .replaceAll("Major - ", "")
          .replaceAll("Minor - ", ""),
      sorter: (a, b) => String(a.label || "").localeCompare(String(b.label || "")),
    },
    {
      title: "Overall Status",
      dataIndex: "overall_status",
      key: "overall_status",
      width: 220,
      render: (_, row) => (
        <Space>
          <Tag style={STATUS_TAG_STYLE_SHORT} color={validationColor(row.status || "PASS")}>{normalizeStatusToken(row.status || "PASS")}</Tag>
          <Tag style={STATUS_TAG_STYLE_LONG} color={row.consistency_status === "INCONSISTENT" ? "red" : "green"}>{(row.consistency_status || "CONSISTENT").toUpperCase()}</Tag>
        </Space>
      ),
      filters: [
        { text: "PASS", value: "PASS" },
        { text: "WARN", value: "WARN" },
        { text: "FAIL", value: "FAIL" },
      ],
      onFilter: (value, row) => normalizeStatusToken(row.status || "") === String(value || "").toUpperCase(),
      sorter: (a, b) => String(a.status || "").localeCompare(String(b.status || "")),
    },
    {
      title: (
        <span style={{ whiteSpace: "normal", lineHeight: 1.15 }}>
          Validation Rules
          <br />
          (Pass/Warn/Fail)
        </span>
      ),
      key: "validation_pf",
      width: 128,
      render: (_, row) => `${row.validation_pass_count ?? 0}/${row.validation_warn_count ?? 0}/${row.validation_fail_count ?? 0}`,
      sorter: (a, b) => (
        Number(a.validation_fail_count || 0) - Number(b.validation_fail_count || 0)
        || Number(a.validation_warn_count || 0) - Number(b.validation_warn_count || 0)
      ),
    },
    {
      title: "Program Design Rules (Consistent/Inconsistent)",
      key: "consistency_ci",
      width: 200,
      render: (_, row) => `${row.consistency_pass_count ?? 0}/${row.consistency_fail_count ?? 0}`,
      sorter: (a, b) => Number(a.consistency_fail_count || 0) - Number(b.consistency_fail_count || 0),
      filters: [
        { text: "Consistent", value: "CONSISTENT" },
        { text: "Inconsistent", value: "INCONSISTENT" },
      ],
      onFilter: (value, row) => String(row.consistency_status || "").toUpperCase() === String(value || "").toUpperCase(),
    },
    {
      title: <span style={{ whiteSpace: "normal", lineHeight: 1.15 }}>Min Credits</span>,
      dataIndex: "min_required_credits",
      key: "min_required_credits",
      width: 90,
      sorter: (a, b) => Number(a.min_required_credits || 0) - Number(b.min_required_credits || 0),
    },
    {
      title: "Mandatory Courses",
      dataIndex: "mandatory_course_count",
      key: "mandatory_course_count",
      width: 118,
      sorter: (a, b) => Number(a.mandatory_course_count || 0) - Number(b.mandatory_course_count || 0),
    },
  ];
  const normalizeFeasibilityLabel = (label) =>
    String(label || "")
      .replaceAll("Double Major - ", "")
      .replaceAll("Double Minor - ", "")
      .replaceAll("Major/Minor - ", "")
      .replaceAll("Major - ", "")
      .replaceAll("Minor - ", "")
      .trim();
  const feasibilityRowsFiltered = useMemo(() => {
    const rows = feasibilityQ.data?.rows || [];
    const q = String(feasibilitySearch || "").trim().toLowerCase();
    return rows.filter((r) => {
      if (q) {
        const label = normalizeFeasibilityLabel(r.label).toLowerCase();
        if (!label.includes(q)) return false;
      }
      return true;
    });
  }, [feasibilityQ.data, feasibilitySearch]);
  const feasibilitySearchOptions = useMemo(() => {
    const labels = Array.from(new Set((feasibilityQ.data?.rows || []).map((r) => normalizeFeasibilityLabel(r.label)).filter(Boolean))).sort();
    return labels.map((v) => ({ value: v, label: v }));
  }, [feasibilityQ.data]);
  const checklistProgramOptions = useMemo(() => {
    const roots = requirementsTreeQ.data?.tree || [];
    const rootProgramIds = new Set(
      roots
        .map((r) => r?.program_id)
        .filter(Boolean)
    );
    return (programsQ.data || [])
      .filter((p) => rootProgramIds.has(p.id))
      .map((p) => ({
        value: p.id,
        label: `${String(p.program_type || "").toUpperCase() === "MINOR" ? "Minor" : "Major"} - ${p.name}`,
      }))
      .sort((a, b) => String(a.label).localeCompare(String(b.label)));
  }, [programsQ.data, requirementsTreeQ.data]);
  useEffect(() => {
    const allowed = new Set((checklistProgramOptions || []).map((o) => o.value));
    setChecklistProgramIds((prev) => (prev || []).filter((id) => allowed.has(id)));
  }, [checklistProgramOptions]);
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
    const matches = rows.filter((r) => sameIds(r.program_ids));
    if (!matches.length) return null;
    return matches[0];
  }, [checklistProgramIds, feasibilityQ.data]);
  const checklistValidationItems = useMemo(() => {
    return checklistQ.data?.validation_items || [];
  }, [checklistQ.data]);
  const isSimplifiedCanvas = canvasViewMode === "SIMPLIFIED";
  const isVerboseCanvas = canvasViewMode === "VERBOSE";
  const showCourseTitleHover = courseTitleHoverEnabled || isVerboseCanvas;
  const sectionTitleStyle = { fontSize: 14, margin: 0 };
  const categorySelectOptions = useMemo(() => {
    const typed = String(ruleFormDomainSearch || "").trim();
    const base = validationCategoryOptions.map((o) => ({ value: o.value, label: o.value }));
    if (!typed) return base;
    if (base.some((o) => String(o.value).toLowerCase() === typed.toLowerCase())) return base;
    return [{ value: typed, label: typed }, ...base];
  }, [validationCategoryOptions, ruleFormDomainSearch]);

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
              style={{ width: 210 }}
              value={canvasViewMode}
              onChange={setCanvasViewMode}
              options={[
                { value: "SIMPLIFIED", label: "Simplified" },
                { value: "STANDARD", label: "Standard" },
                { value: "VERBOSE", label: "Verbose" },
              ]}
            />
            <Space size={6}>
              <Typography.Text type="secondary">Hover short titles</Typography.Text>
              <Switch checked={showCourseTitleHover} onChange={setCourseTitleHoverEnabled} disabled={isVerboseCanvas} />
            </Space>
          </Space>
          <Space>
            <Typography.Text type="secondary">Version Lifecycle</Typography.Text>
            <Button size="small" onClick={() => setVersionStatus("DRAFT")}>DRAFT</Button>
            <Button size="small" onClick={() => setVersionStatus("UNDER_REVIEW")}>UNDER_REVIEW</Button>
            <Button size="small" onClick={() => setVersionStatus("APPROVED")}>APPROVED</Button>
            <Button size="small" onClick={() => setVersionStatus("ACTIVE")}>ACTIVE</Button>
            <Button size="small" onClick={() => setVersionStatus("ARCHIVED")}>ARCHIVED</Button>
          </Space>
          <Space wrap>
            <Select
              mode="multiple"
              size="small"
              style={{ width: 280 }}
              value={datasetModules}
              onChange={(vals) => setDatasetModules(vals?.length ? vals : ["ALL"])}
              options={[
                { value: "ALL", label: "ALL" },
                { value: "COURSES", label: "COURSES" },
                { value: "RULES", label: "RULES" },
                { value: "CANVAS", label: "CANVAS" },
                { value: "REPORTS", label: "REPORTS" },
              ]}
            />
            <Input
              size="small"
              style={{ width: 260 }}
              placeholder="Saved dataset bundle name"
              value={datasetBundleName}
              onChange={(e) => setDatasetBundleName(e.target.value)}
            />
            <Button size="small" type="primary" onClick={saveDatasetBundle} disabled={!selectedVersion?.id || !datasetBundleName.trim()}>
              Save Dataset
            </Button>
            <Button size="small" onClick={exportDatasetBundle} disabled={!selectedVersion?.id}>
              Export Dataset
            </Button>
            <Button size="small" onClick={() => datasetImportInputRef.current?.click()} disabled={!selectedVersion?.id}>
              Import Dataset
            </Button>
            <input
              ref={datasetImportInputRef}
              type="file"
              accept=".json,application/json"
              style={{ display: "none" }}
              onChange={async (e) => {
                const file = e.target.files?.[0];
                e.target.value = "";
                if (!file) return;
                await importDatasetBundleFromFile(file);
              }}
            />
            <Select
              allowClear
              size="small"
              style={{ width: 340 }}
              placeholder="Saved dataset bundle"
              value={selectedSavedDatasetId}
              onChange={setSelectedSavedDatasetId}
              options={(datasetSavedQ.data || []).map((s) => ({
                value: s.id,
                label: `${s.name} [${s.modules_csv}] (${new Date(s.created_at).toLocaleString()})`,
              }))}
            />
            <Button size="small" onClick={loadSavedDatasetBundle} disabled={!selectedVersion?.id || !selectedSavedDatasetId}>
              Load Dataset
            </Button>
          </Space>
          <Space wrap>
            <Typography.Text type="secondary">Suggested Major Sequences</Typography.Text>
            <Select
              showSearch
              allowClear
              style={{ width: 520 }}
              placeholder="Select a COI suggested sequence"
              value={selectedSuggestedSequenceId}
              onChange={setSelectedSuggestedSequenceId}
              optionFilterProp="label"
              options={(suggestedSequencesQ.data?.rows || []).map((s) => ({
                value: s.id,
                label: `${s.name}${s.item_count ? ` (${s.item_count} items)` : ""}${s.options_note ? " [options noted]" : ""}`,
              }))}
            />
            <Button
              size="small"
              type="primary"
              onClick={loadSuggestedSequenceToCanvas}
              disabled={!selectedVersion?.id || !selectedSuggestedSequenceId}
            >
              Load Suggested Sequence
            </Button>
          </Space>
        </Space>
      </Card>
      <div className="timeline-grid">
        {CANVAS_GRID.flatMap((row, rowIdx) =>
          row.map((periodIdx, colIdx) => {
            if (periodIdx == null) {
              return <div key={`blank-${rowIdx}-${colIdx}`} className="timeline-blank-cell" />;
            }
            const periodCourses = (canvasQ.data?.[String(periodIdx)] || [])
              .filter((course) => !canvasFilteredCourseIds || canvasFilteredCourseIds.has(course.course_id));
            return (
              <Card
                key={`period-${periodIdx}`}
                size="small"
                title={
                  <Space>
                    <span>{periodLabel(periodIdx)}</span>
                    <Tag>{periodCourses.reduce((sum, c) => sum + Number(c.credits || 0), 0)} cr</Tag>
                  </Space>
                }
                extra={
                  <Button size="small" type="primary" onClick={() => openAddCourseModal(periodIdx)}>
                    +
                  </Button>
                }
                className="semester-card timeline-cell"
                onDragOver={(e) => e.preventDefault()}
                onDrop={(e) => dropToSemester(periodIdx, e)}
              >
                {periodCourses.map((course) => (
                  <Tooltip
                    key={course.plan_item_id}
                    title={showCourseTitleHover ? (course.title || "No short title") : null}
                  >
                    {isSimplifiedCanvas ? (
                      <div
                        draggable
                        onDragStart={(e) => e.dataTransfer.setData("text/plain", course.plan_item_id)}
                        onDragOver={(e) => e.preventDefault()}
                        onDrop={(e) => dropToCourse(periodIdx, course.plan_item_id, e)}
                        style={{ marginBottom: 6, cursor: "grab" }}
                      >
                        <Space style={{ width: "100%", justifyContent: "space-between" }}>
                          <Typography.Text strong>{course.course_number}</Typography.Text>
                          <Tag>{course.credits} cr</Tag>
                        </Space>
                      </div>
                    ) : (
                      <Card
                        size="small"
                        draggable
                        onDragStart={(e) => e.dataTransfer.setData("text/plain", course.plan_item_id)}
                        onDragOver={(e) => e.preventDefault()}
                        onDrop={(e) => dropToCourse(periodIdx, course.plan_item_id, e)}
                        style={{ marginBottom: 8, cursor: "grab" }}
                      >
		                    <Space direction="vertical" style={{ width: "100%" }}>
			                      <Typography.Text strong>{course.course_number}</Typography.Text>
		                      <Space style={{ width: "100%", justifyContent: "flex-end" }} wrap>
		                        {canvasTimingByCourse[course.course_id] ? (
		                          (() => {
		                            const constraints = canvasTimingByCourse[course.course_id] || [];
		                            const inRequired = constraints.some((c) => periodIdx != null && semesterMatchesConstraint(periodIdx, c));
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
		                          <Button size="small" onClick={() => openEditCourseModal(course, periodIdx)}>
		                            Edit
		                          </Button>
		                          <Button size="small" danger onClick={() => removeFromCanvas(course.plan_item_id)}>
		                            Delete
		                          </Button>
		                        </Space>
		                      </Space>
                        {isVerboseCanvas && (
                          <Space>
                            <Typography.Text type="secondary">{course.title}</Typography.Text>
                            <Tag color="blue">{aspectLabel(course)}</Tag>
                            {course.major_program_name && <Tag color="geekblue">{course.major_program_name}</Tag>}
                          </Space>
                        )}
                      </Space>
                    </Card>
                    )}
                  </Tooltip>
                ))}
              </Card>
            );
          })
        )}
      </div>
      <Card title="Course of Study Feasibility (Core + Selected Majors And Minors)">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Select
            mode="multiple"
            allowClear
            style={{ width: "100%" }}
            placeholder="Select major/minor programs to evaluate with core requirements"
            value={checklistProgramIds}
            onChange={setChecklistProgramIds}
            options={checklistProgramOptions}
          />
          <Typography.Text type="secondary">
            Program Designer rules are checked first. Validation rule line items are listed separately below.
          </Typography.Text>
          {selectedChecklistFeasibility?.status === "FAIL" ? (
            <Space>
              <Tag style={STATUS_TAG_STYLE_SHORT} color="red">FAIL</Tag>
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
          <Typography.Title level={5} style={sectionTitleStyle}>Program Design Rules</Typography.Title>
          <div style={{ overflowX: "auto", overflowY: "auto", maxHeight: "44vh", paddingRight: 4 }}>
            {renderChecklist(checklistRequirementsTree || [])}
          </div>
          <Typography.Title level={5} style={sectionTitleStyle}>Validation Rules</Typography.Title>
          <div style={{ overflowX: "auto", overflowY: "auto", maxHeight: "34vh", paddingRight: 4 }}>
            {renderValidationLineItems(checklistValidationItems, "No validation findings for current course of study.")}
          </div>
        </Space>
      </Card>
      <Card title="Program Feasibility">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Space>
            <Tag style={STATUS_TAG_STYLE_SHORT} color="green">{`PASS ${feasibilityQ.data?.summary?.pass ?? 0}`}</Tag>
            <Tag style={STATUS_TAG_STYLE_SHORT} color="orange">{`WARN ${feasibilityQ.data?.summary?.warning ?? 0}`}</Tag>
            <Tag style={STATUS_TAG_STYLE_SHORT} color="red">{`FAIL ${feasibilityQ.data?.summary?.fail ?? 0}`}</Tag>
            <Typography.Text type="secondary">
              {`${feasibilityQ.data?.row_count ?? 0} combinations evaluated`}
            </Typography.Text>
          </Space>
          <AutoComplete
            allowClear
            style={{ width: 360 }}
            options={feasibilitySearchOptions}
            placeholder="Search combination (select or type)"
            value={feasibilitySearch}
            onSearch={setFeasibilitySearch}
            onChange={setFeasibilitySearch}
            filterOption={(inputValue, option) =>
              String(option?.value || "").toLowerCase().includes(String(inputValue || "").toLowerCase())
            }
          />
          <Table
            size="small"
            rowKey={(r) => `${r.kind}:${r.label}`}
            loading={feasibilityQ.isLoading}
            columns={feasibilityColumns}
            dataSource={feasibilityRowsFiltered}
            pagination={{ pageSize: 25, showSizeChanger: false }}
            scroll={{ x: 980, y: 420 }}
            expandable={{
              expandedRowRender: (row) => (
                <Space direction="vertical" style={{ width: "100%" }}>
                  <Typography.Title level={5} style={sectionTitleStyle}>Program Design Rules</Typography.Title>
                  <div style={{ overflowX: "auto", overflowY: "auto", maxHeight: "30vh", paddingRight: 4 }}>
                    {row.program_design_consistency_tree?.length
                      ? renderConsistencyTree(row.program_design_consistency_tree)
                      : <Typography.Text type="secondary">None</Typography.Text>}
                  </div>
                  <Typography.Title level={5} style={sectionTitleStyle}>Validation Rules</Typography.Title>
                  <div style={{ overflowX: "auto", overflowY: "auto", maxHeight: "24vh", paddingRight: 4 }}>
                    {renderValidationLineItems(row.validation_items || [], "No validation line items.")}
                  </div>
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
                { value: "OPTION_SLOT", label: "OPTION_SLOT" },
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
          {(isSubNodeCreate || isReqEditMode) && editReqLogic === "OPTION_SLOT" && (
            <Input
              style={{ width: "100%" }}
              placeholder="Option slot key (e.g., MATH_ELECTIVE)"
              value={editReqOptionSlotKey}
              onChange={(e) => setEditReqOptionSlotKey(e.target.value)}
            />
          )}
          {(isSubNodeCreate || isReqEditMode) && editReqLogic === "OPTION_SLOT" && (
            <Input
              style={{ width: "100%" }}
              placeholder="Option slot capacity"
              value={editReqOptionSlotCapacity}
              onChange={(e) => setEditReqOptionSlotCapacity(e.target.value)}
            />
          )}
          <Button type="primary" onClick={saveRequirementNode}>
            Save Node
          </Button>
        </Space>
      </Modal>
      <Modal
        title={reqLinkKind === "BASKET" ? (basketLinkId ? "Edit Requirement Basket" : "Add Course/Basket To Requirement") : "Add Course/Basket To Requirement"}
        open={reqCourseModalOpen}
        onCancel={() => {
          setReqCourseModalOpen(false);
          setReqLinkKindLocked(false);
          setReqSubDraftRows([]);
          setReqCourseTimingRules([]);
          resetBasketModal();
        }}
        onOk={reqLinkKind === "BASKET" ? saveRequirementBasketModal : saveRequirementCourseModal}
        okText={reqLinkKind === "BASKET" ? (basketLinkId ? "Save Basket" : "Add Basket") : "Save Course"}
        okButtonProps={reqLinkKind === "BASKET" ? { disabled: basketValidationErrors.length > 0 } : { disabled: !reqCourseId }}
      >
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text type="secondary">
            {requirementFullPathMap[reqCourseRequirementId || ""] || requirementNodeMap[reqCourseRequirementId || ""]?.name || "None"}
          </Typography.Text>
          <Select
            style={{ width: "100%" }}
            value={reqLinkKind}
            onChange={(v) => setReqLinkKind(v)}
            disabled={reqLinkKindLocked}
            options={[
              { value: "COURSE", label: "Add Course" },
              { value: "BASKET", label: "Add Basket" },
            ]}
          />

          {reqLinkKind === "COURSE" ? (
            <>
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
            </>
          ) : (
            <>
              <Select
                allowClear
                showSearch
                optionFilterProp="label"
                placeholder="Use existing basket (optional)"
                value={basketSelectedId}
                style={{ width: "100%" }}
                onChange={(v) => {
                  setBasketSelectedId(v);
                  if (!v) return;
                  const b = (basketsQ.data || []).find((x) => x.id === v);
                  if (!b) return;
                  setBasketName(b.name || "");
                  setBasketDescription(b.description || "");
                  const ids = (b.items || []).map((x) => x.course_id).filter(Boolean);
                  setBasketCourseIds(ids);
                  setBasketSubGroupRows(deriveBasketSubstituteRows(v, ids, basketRequirementId));
                }}
                options={(basketsQ.data || []).map((b) => ({
                  value: b.id,
                  label: `${b.name} (${(b.items || []).length})`,
                }))}
              />
              <Input
                placeholder="Basket name"
                value={basketName}
                onChange={(e) => setBasketName(e.target.value)}
              />
              <Input
                placeholder="Basket description (optional)"
                value={basketDescription}
                onChange={(e) => setBasketDescription(e.target.value)}
              />
              <Input
                placeholder="Min count"
                value={basketMinCount}
                onChange={(e) => setBasketMinCount(e.target.value)}
              />
              <Select
                mode="multiple"
                allowClear
                showSearch
                optionFilterProp="label"
                placeholder="Basket courses"
                value={basketCourseIds}
                style={{ width: "100%" }}
                onChange={(v) => setBasketCourseIds(v || [])}
                options={(coursesQ.data || []).map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
              />
              <Typography.Text strong>Substitute Groups (optional)</Typography.Text>
              {basketSubGroupRows.map((row, idx) => {
                const availableIds = Array.from(new Set((basketCourseIds || []).filter(Boolean)));
                const primary = row.primary_course_id;
                const substitutes = (row.substitute_course_ids || []);
                return (
                  <Card key={`basket-sub-row:${idx}`} size="small" style={{ width: "100%" }}>
                    <Space direction="vertical" style={{ width: "100%" }}>
                      <Select
                        allowClear
                        showSearch
                        optionFilterProp="label"
                        placeholder="Primary course"
                        style={{ width: "100%" }}
                        value={primary}
                        onChange={(v) =>
                          setBasketSubGroupRows((prev) =>
                            prev.map((r, i) =>
                              i === idx
                                ? {
                                    ...r,
                                    primary_course_id: v,
                                    substitute_course_ids: (r.substitute_course_ids || []).filter((sid) => sid && sid !== v),
                                  }
                                : r
                            )
                          )
                        }
                        options={availableIds.map((cid) => ({
                          value: cid,
                          label: `${courseMapById[cid]?.course_number || ""} - ${courseMapById[cid]?.title || "Course"}`,
                        }))}
                      />
                      {(substitutes || []).map((subId, subIdx) => (
                        <div key={`basket-sub-choice:${idx}:${subIdx}`} style={{ width: "100%", display: "flex", alignItems: "center", gap: 8 }}>
                          <Select
                            allowClear
                            showSearch
                            optionFilterProp="label"
                            placeholder={`Substitute ${subIdx + 1}`}
                            style={{ width: "calc(100% - 84px)" }}
                            value={subId}
                            onChange={(v) =>
                              setBasketSubGroupRows((prev) =>
                                prev.map((r, i) => {
                                  if (i !== idx) return r;
                                  const next = [...(r.substitute_course_ids || [])];
                                  next[subIdx] = v;
                                  const cleaned = next
                                    .filter(Boolean)
                                    .filter((sid) => sid && sid !== (r.primary_course_id || ""));
                                  return { ...r, substitute_course_ids: Array.from(new Set(cleaned)) };
                                })
                              )
                            }
                            options={availableIds
                              .filter((cid) => cid !== (primary || ""))
                              .filter((cid) => cid === subId || !(substitutes || []).includes(cid))
                              .map((cid) => ({
                                value: cid,
                                label: `${courseMapById[cid]?.course_number || ""} - ${courseMapById[cid]?.title || "Course"}`,
                              }))}
                          />
                          <Button
                            danger
                            style={{ width: 76 }}
                            onClick={() =>
                              setBasketSubGroupRows((prev) =>
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
                      <Button
                        onClick={() =>
                          setBasketSubGroupRows((prev) =>
                            prev.map((r, i) =>
                              i === idx ? { ...r, substitute_course_ids: [...(r.substitute_course_ids || []), undefined] } : r
                            )
                          )
                        }
                        disabled={!primary || (substitutes || []).filter(Boolean).length >= Math.max(availableIds.length - 1, 0)}
                      >
                        Add Substitute
                      </Button>
                      <Button
                        danger
                        onClick={() => setBasketSubGroupRows((prev) => prev.filter((_, i) => i !== idx))}
                      >
                        Delete Group
                      </Button>
                    </Space>
                  </Card>
                );
              })}
              <Button
                onClick={() =>
                  setBasketSubGroupRows((prev) => [...prev, { primary_course_id: undefined, substitute_course_ids: [] }])
                }
                disabled={!basketCourseIds.length}
              >
                Add Substitute Group
              </Button>
              {(basketValidationErrors || []).map((msg, idx) => (
                <Typography.Text key={`basket-error-${idx}`} type="danger">
                  {msg}
                </Typography.Text>
              ))}
            </>
          )}
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
            Define major-specific choices from Core Pick N nodes (including Pick 1).
          </Typography.Text>
          {coreRulesGroupedRows.map((group) => (
            <Card
              key={`core-rules-group:${group.requirement_id}`}
              size="small"
              style={{ borderColor: group.slot_total > 1 ? "#b7ccff" : undefined, background: group.slot_total > 1 ? "#fafcff" : undefined }}
              title={(
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
                  <span>{group.requirement_name}</span>
                  {group.restrict_to_sub_groups ? (
                    <Button
                      size="small"
                      type="primary"
                      onClick={() => {
                        const used = new Set((group.rows || []).map((r) => Number(r.slot_index || 0)));
                        let nextIdx = null;
                        for (let i = 0; i < Math.max(1, Number(group.slot_total || 1)); i += 1) {
                          if (!used.has(i)) {
                            nextIdx = i;
                            break;
                          }
                        }
                        if (nextIdx == null) return;
                        setCoreRulesRows((prev) => [
                          ...prev,
                          {
                            requirement_id: group.requirement_id,
                            requirement_name: group.requirement_name,
                            slot_index: nextIdx,
                            slot_total: Math.max(1, Number(group.slot_total || 1)),
                            primary_course_id: undefined,
                            substitute_course_ids: [],
                            required_semester: undefined,
                            required_semester_min: undefined,
                            required_semester_max: undefined,
                            restrict_to_sub_groups: true,
                          },
                        ]);
                      }}
                      disabled={(group.rows || []).length >= Math.max(1, Number(group.slot_total || 1))}
                    >
                      Add Substitute Group Rule
                    </Button>
                  ) : null}
                </div>
              )}
            >
              <Space direction="vertical" style={{ width: "100%" }}>
                {group.restrict_to_sub_groups && !(group.rows || []).length ? (
                  <Typography.Text type="secondary">No substitute-group constraints defined.</Typography.Text>
                ) : null}
                {group.rows.map((row) => {
                  const rowKey = `${row.requirement_id}:${row.slot_index}`;
                  const idx = (coreRulesRows || []).findIndex((x) => x.requirement_id === row.requirement_id && x.slot_index === row.slot_index);
                  const siblingSelected = new Set(
                    (group.rows || [])
                      .filter((r) => r.slot_index !== row.slot_index)
                      .map((r) => r.primary_course_id)
                      .filter(Boolean)
                  );
                  const rawChoiceOptions = (coreRuleChoiceOptionsByReq[row.requirement_id] || [])
                    .filter((o) => (row.restrict_to_sub_groups ? (o.group_course_ids || []).length > 1 : true));
                  const choiceOptions = rawChoiceOptions.filter((o) => !siblingSelected.has(o.value) || o.value === row.primary_course_id);
                  const selectedChoice = choiceOptions.find((o) => o.value === row.primary_course_id);
                  const groupCourseIds = selectedChoice?.group_course_ids || [];
                  const maxSubChoices = Math.max(0, groupCourseIds.length - 1);
                  const applicableChoiceIds = groupCourseIds.length > 1
                    ? Array.from(new Set((row.substitute_course_ids || []).filter((id) => id && groupCourseIds.includes(id))))
                    : [];
                  const applicableChoiceRows = groupCourseIds.length > 1
                    ? (row.substitute_course_ids || [])
                        .filter((id) => id == null || groupCourseIds.includes(id))
                    : [];
                  const existingTimingRows = groupCourseIds.flatMap((cid) => coreRequirementTimingByCourse[cid] || []);
                  const existingTimingLabels = Array.from(new Set(existingTimingRows.map((x) => formatSemesterConstraint(x)).filter(Boolean)));
                  const hasExistingGroupTiming = existingTimingLabels.length > 0;
                  return (
                    <Card
                      key={`core-rule-row:${rowKey}`}
                      size="small"
                      title={
                        row.restrict_to_sub_groups
                          ? `Substitute Group ${row.slot_index + 1}/${Math.max(1, row.slot_total || 1)}`
                          : `Pick ${row.slot_index + 1}/${Math.max(1, row.slot_total || 1)}`
                      }
                      extra={
                        row.restrict_to_sub_groups ? (
                          <Button
                            size="small"
                            danger
                            onClick={() =>
                              setCoreRulesRows((prev) =>
                                prev.filter((r) => !(r.requirement_id === row.requirement_id && Number(r.slot_index) === Number(row.slot_index)))
                              )
                            }
                          >
                            Delete
                          </Button>
                        ) : null
                      }
                    >
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
                        {applicableChoiceRows.map((subId, subIdx) => (
                          <div key={`core-sub-choice-${idx}-${subIdx}`} style={{ width: "100%", display: "flex", alignItems: "center", gap: 8 }}>
                            {(() => {
                              const selectedElsewhere = new Set(
                                applicableChoiceRows.filter((_, j) => j !== subIdx).filter(Boolean)
                              );
                              const optionsForThisSub = groupCourseIds
                                .filter((cid) => !selectedElsewhere.has(cid) || cid === subId)
                                .map((cid) => ({ value: cid, label: courseMapById[cid]?.course_number || courseMapById[cid]?.title || "Missing course" }));
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
                                    const next = [...applicableChoiceRows];
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
                                      ? { ...r, substitute_course_ids: applicableChoiceRows.filter((_, j) => j !== subIdx) }
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
                                  i === idx ? { ...r, substitute_course_ids: [...applicableChoiceRows, undefined] } : r
                                )
                              )
                            }
                            disabled={!row.primary_course_id || applicableChoiceRows.length >= maxSubChoices}
                          >
                            Choose Applicable Course (Optional)
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
      <Card title="Program Design Rules (Core, Majors, and Minors)">
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
          <ScrollPane maxHeight="68vh">
            <Tree
              treeData={integratedTreeData}
              motion={null}
              draggable
              blockNode
              onDrop={handleTreeDrop}
              onSelect={handleRequirementTreeSelect}
              expandedKeys={treeExpandedKeys}
              onExpand={setTreeExpandedKeys}
              selectedKeys={selectedRuleNodeId ? [selectedRuleNodeId] : []}
            />
          </ScrollPane>
        </Space>
      </Card>
      <Card title="Validation Rules">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text type="secondary">
            Define validation rules and categories used by Program Feasibility and Course of Study Feasibility.
          </Typography.Text>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
            <Space>
              <Select
                style={{ width: 280 }}
                value={validationDomainFilter}
                onChange={setValidationDomainFilter}
                options={validationDomainFilterOptions}
              />
              <Button size="small" onClick={() => setValidationTreeExpandedKeys(validationTopKeys)}>
                Expand Top
              </Button>
              <Button size="small" onClick={() => setValidationTreeExpandedKeys(validationAllKeys)}>
                Expand All
              </Button>
              <Button size="small" onClick={() => setValidationTreeExpandedKeys([])}>
                Collapse All
              </Button>
            </Space>
            <Button type="primary" size="small" onClick={openCreateRuleModal}>
              Add Rule
            </Button>
          </div>
          <ScrollPane maxHeight="68vh">
            <Tree
              treeData={validationTreeData}
              motion={null}
              blockNode
              draggable
              onDrop={handleValidationTreeDrop}
              expandedKeys={validationTreeExpandedKeys}
              onExpand={setValidationTreeExpandedKeys}
            />
          </ScrollPane>
        </Space>
      </Card>
      <Modal
        title={ruleModalEditId ? "Edit Validation Rule" : "Add Validation Rule"}
        open={ruleModalOpen}
        onCancel={() => setRuleModalOpen(false)}
        onOk={saveRuleModal}
        okText={ruleModalEditId ? "Save Rule" : "Add Rule"}
      >
        <Space direction="vertical" style={{ width: "100%" }}>
          <div style={{ display: "flex", justifyContent: "flex-end" }}>
            <Button type="link" style={{ paddingInline: 0 }} onClick={() => setRuleCategoryGuideOpen(true)}>
              Category Guide
            </Button>
          </div>
          <Typography.Text type="secondary">Rule Name</Typography.Text>
          <Input value={ruleFormName} onChange={(e) => setRuleFormName(e.target.value)} placeholder="Rule name" />
          <Typography.Text type="secondary">Tier</Typography.Text>
          <Select
            value={ruleFormTier}
            onChange={setRuleFormTier}
            options={[
              { value: 1, label: "Tier 1" },
              { value: 2, label: "Tier 2" },
              { value: 3, label: "Tier 3" },
            ]}
          />
          <Typography.Text type="secondary">Category</Typography.Text>
          <Select
            showSearch
            style={{ width: "100%" }}
            value={ruleFormDomain}
            options={categorySelectOptions}
            onChange={setRuleFormDomain}
            onSearch={setRuleFormDomainSearch}
            placeholder="Category (choose existing or type new)"
            filterOption={(input, option) => String(option?.label || "").toLowerCase().includes(String(input).toLowerCase())}
            onBlur={() => {
              const typed = String(ruleFormDomainSearch || "").trim();
              if (typed) setRuleFormDomain(typed);
            }}
          />
          <Typography.Text type="secondary">Severity</Typography.Text>
          <Select
            value={ruleFormSeverity}
            onChange={setRuleFormSeverity}
            options={[
              { value: "WARN", label: "WARN" },
              { value: "FAIL", label: "FAIL" },
            ]}
          />
          <Typography.Text type="secondary">Active</Typography.Text>
          <Select
            value={ruleFormActive}
            onChange={setRuleFormActive}
            options={[
              { value: "YES", label: "Enabled" },
              { value: "NO", label: "Disabled" },
            ]}
          />
          <Typography.Text type="secondary">Config (JSON)</Typography.Text>
          <Input.TextArea
            rows={6}
            value={ruleFormConfig}
            onChange={(e) => setRuleFormConfig(e.target.value)}
            placeholder='{"key":"value"}'
          />
        </Space>
      </Modal>
      <Modal
        title="Validation Rule Category Guide"
        open={ruleCategoryGuideOpen}
        onCancel={() => setRuleCategoryGuideOpen(false)}
        footer={null}
        width={860}
      >
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text type="secondary">
            JSON settings guide for validation rules. Categories are set via <code>domain</code>; additional fields are rule-specific. Update this list as rule options evolve.
          </Typography.Text>
          <Table
            size="small"
            pagination={false}
            rowKey="title"
            dataSource={VALIDATION_RULE_JSON_GUIDE}
            tableLayout="fixed"
            scroll={{ x: 980 }}
            columns={[
              { title: "Setting", dataIndex: "title", key: "title", width: 240 },
              { title: "Description", dataIndex: "description", key: "description" },
              {
                title: "JSON",
                dataIndex: "json",
                key: "json",
                width: 360,
                render: (v) => (
                  <code
                    style={{
                      display: "block",
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-word",
                      overflowWrap: "anywhere",
                    }}
                  >
                    {v}
                  </code>
                ),
              },
            ]}
          />
        </Space>
      </Modal>
      <Card title="Prerequisite Graph">
        <Space wrap={false} style={{ marginBottom: 10, width: "100%" }}>
          <AutoComplete
            allowClear
            options={prereqFromOptions}
            style={{ width: 360 }}
            placeholder="From course (select or type)"
            value={prereqFromQuery}
            onSearch={setPrereqFromQuery}
            onChange={setPrereqFromQuery}
            filterOption={(inputValue, option) =>
              String(option?.value || "").toLowerCase().includes(String(inputValue || "").toLowerCase())
            }
          />
          <AutoComplete
            allowClear
            options={prereqToOptions}
            style={{ width: 360 }}
            placeholder="To course (select or type)"
            value={prereqToQuery}
            onSearch={setPrereqToQuery}
            onChange={setPrereqToQuery}
            filterOption={(inputValue, option) =>
              String(option?.value || "").toLowerCase().includes(String(inputValue || "").toLowerCase())
            }
          />
        </Space>
        <Table
          size="small"
          pagination={{ pageSize: 8 }}
          rowKey="id"
          dataSource={prereqEdgesFiltered}
          columns={[
            {
              title: "From",
              dataIndex: "from_label",
              sorter: (a, b) => String(a.from_label || "").localeCompare(String(b.from_label || "")),
              render: (v) => renderCourseCodeWithHover(v),
            },
            {
              title: "To",
              dataIndex: "to_label",
              sorter: (a, b) => String(a.to_label || "").localeCompare(String(b.to_label || "")),
              render: (v) => renderCourseCodeWithHover(v),
            },
            {
              title: "Type",
              dataIndex: "relationship_type",
              render: (v) => formatPrereqType(v),
              filters: [
                { text: "Prerequisite", value: "PREREQUISITE" },
                { text: "Corequisite", value: "COREQUISITE" },
              ],
              onFilter: (value, row) => String(row.relationship_type || "").toUpperCase() === String(value || "").toUpperCase(),
            },
            { title: "Rule", dataIndex: "group_display" },
            { title: "Enforcement", dataIndex: "enforcement", render: (v) => formatEnforcement(v) },
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
          <Space>
            <AutoComplete
              allowClear
              style={{ width: 420 }}
              placeholder="Select course (dropdown or type to search)"
              value={courseSearchValue}
              options={courseAutoOptions}
              onChange={setCourseSearchValue}
              onSelect={(_, option) => {
                setSelectedCourseId(option.courseId);
                setCourseSearchValue(option.value);
              }}
              filterOption={(inputValue, option) =>
                String(option?.value || "").toLowerCase().includes(String(inputValue || "").toLowerCase())
              }
            />
            <Button type="primary" onClick={() => setNewCourseModalOpen(true)}>New Course</Button>
          </Space>
          <Tabs
            items={[
              {
                key: "general",
                label: "Overview",
                children: (
                  <Space direction="vertical" style={{ width: "100%" }}>
                    <div>
                      <Typography.Text type="secondary">Course Number</Typography.Text>
                      <Input
                        value={courseGeneralForm.course_number}
                        onChange={(e) =>
                          setCourseGeneralForm((s) => ({ ...s, course_number: e.target.value }))
                        }
                        placeholder="e.g., Astro 310"
                      />
                    </div>
                    <div>
                      <Typography.Text type="secondary">Short Title</Typography.Text>
                      <Input
                        value={courseGeneralForm.title}
                        onChange={(e) =>
                          setCourseGeneralForm((s) => ({ ...s, title: e.target.value }))
                        }
                        placeholder="e.g., Space Systems Design"
                      />
                    </div>
                    <Button type="primary" loading={courseSaveBusy} onClick={saveCourseDetail}>
                      Save Course Fields
                    </Button>
                  </Space>
                )
              },
              {
                key: "scheduling",
                label: "Scheduling & Offering",
                children: (
                  <Space direction="vertical" style={{ width: "100%" }}>
                    <div>
                      <Typography.Text type="secondary">Credit Hours</Typography.Text>
                      <Input
                        value={courseSchedulingForm.credit_hours}
                        onChange={(e) =>
                          setCourseSchedulingForm((s) => ({ ...s, credit_hours: e.target.value }))
                        }
                        placeholder="e.g., 3"
                      />
                    </div>
                    <div>
                      <Typography.Text type="secondary">Fixed Period (Optional)</Typography.Text>
                      <Select
                        allowClear
                        style={{ width: "100%" }}
                        value={courseSchedulingForm.designated_semester}
                        onChange={(v) =>
                          setCourseSchedulingForm((s) => ({ ...s, designated_semester: v ?? null }))
                        }
                        options={semesterOptions}
                      />
                    </div>
                    <div>
                      <Typography.Text type="secondary">Offered Periods JSON (Optional)</Typography.Text>
                      <Input
                        value={courseSchedulingForm.offered_periods_json}
                        onChange={(e) =>
                          setCourseSchedulingForm((s) => ({ ...s, offered_periods_json: e.target.value }))
                        }
                        placeholder='e.g., {"offered":[1,2,3]}'
                      />
                    </div>
                    <div>
                      <Typography.Text type="secondary">Standing Requirement (Optional)</Typography.Text>
                      <Input
                        value={courseSchedulingForm.standing_requirement}
                        onChange={(e) =>
                          setCourseSchedulingForm((s) => ({ ...s, standing_requirement: e.target.value }))
                        }
                        placeholder="e.g., C2C standing"
                      />
                    </div>
                    <div>
                      <Typography.Text type="secondary">Additional Requirements (Optional)</Typography.Text>
                      <Input.TextArea
                        rows={2}
                        value={courseSchedulingForm.additional_requirements_text}
                        onChange={(e) =>
                          setCourseSchedulingForm((s) => ({ ...s, additional_requirements_text: e.target.value }))
                        }
                        placeholder="e.g., Dept approval; restricted to major"
                      />
                    </div>
                    <div>
                      <Typography.Text type="secondary">Course Owner</Typography.Text>
                      <Select
                        style={{ width: "100%" }}
                        value={courseSchedulingForm.ownership_code || "DF"}
                        onChange={(v) =>
                          setCourseSchedulingForm((s) => ({ ...s, ownership_code: v || "DF" }))
                        }
                        options={COURSE_OWNERSHIP_OPTIONS}
                      />
                    </div>
                    <div>
                      <Typography.Text type="secondary">Minimum Section Size</Typography.Text>
                      <Input
                        value={courseSchedulingForm.min_section_size}
                        onChange={(e) =>
                          setCourseSchedulingForm((s) => ({ ...s, min_section_size: e.target.value }))
                        }
                        placeholder="e.g., 6"
                      />
                    </div>
                    <Button type="primary" loading={courseSaveBusy} onClick={saveCourseDetail}>
                      Save Scheduling Fields
                    </Button>
                  </Space>
                )
              },
              {
                key: "buckets",
                label: "Accreditation Buckets",
                children: (
                  <Space direction="vertical" style={{ width: "100%" }}>
                    <Space wrap>
                      <Select
                        style={{ width: 280 }}
                        value={newCourseBucketCode}
                        onChange={setNewCourseBucketCode}
                        options={[
                          { value: "ABET_MATH_BASIC_SCI", label: "ABET Math/Basic Science" },
                          { value: "ABET_ENGINEERING_TOPICS", label: "ABET Engineering Topics" },
                        ]}
                      />
                      <Input
                        style={{ width: 220 }}
                        value={newCourseBucketHours}
                        onChange={(e) => setNewCourseBucketHours(e.target.value)}
                        placeholder="Credit override (optional)"
                      />
                      <Button onClick={addCourseBucket}>Add/Update Bucket</Button>
                    </Space>
                    <Table
                      size="small"
                      pagination={false}
                      rowKey="id"
                      dataSource={courseBucketsQ.data || []}
                      columns={[
                        { title: "Bucket", dataIndex: "bucket_code" },
                        {
                          title: "Credits",
                          dataIndex: "credit_hours_override",
                          render: (v) => (v == null ? "Use course credit hours" : String(v)),
                        },
                        {
                          title: "Action",
                          render: (_, row) => (
                            <Button danger size="small" onClick={() => deleteCourseBucket(row.id)}>
                              Delete
                            </Button>
                          ),
                        },
                      ]}
                    />
                  </Space>
                ),
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
                          { value: "PREREQUISITE", label: "Prerequisite" },
                          { value: "COREQUISITE", label: "Corequisite" }
                        ]}
                      />
                      <Select
                        style={{ width: 140 }}
                        value={newPrereqEnforcement}
                        onChange={setNewPrereqEnforcement}
                        options={[
                          { value: "HARD", label: "Hard" },
                          { value: "SOFT", label: "Soft" }
                        ]}
                      />
                      <Input
                        style={{ width: 120 }}
                        placeholder="Group key (optional)"
                        value={newPrereqGroupKey}
                        onChange={(e) => setNewPrereqGroupKey(e.target.value)}
                      />
                      <Input
                        style={{ width: 150 }}
                        placeholder="Group label"
                        value={newPrereqGroupLabel}
                        onChange={(e) => setNewPrereqGroupLabel(e.target.value)}
                      />
                      <InputNumber
                        style={{ width: 110 }}
                        min={1}
                        value={Number(newPrereqGroupMinRequired || 1)}
                        onChange={(v) => setNewPrereqGroupMinRequired(String(v || 1))}
                        placeholder="Min in group"
                      />
                      <Button onClick={addPrerequisite}>Add Link</Button>
                    </Space>
                    <Table
                      size="small"
                      pagination={false}
                      rowKey="id"
                      dataSource={courseDetailQ.data?.prerequisites || []}
                      columns={[
                        {
                          title: "Required Course",
                          render: (_, row) => (
                            <Select
                              showSearch
                              optionFilterProp="label"
                              style={{ width: 300 }}
                              value={prereqEdits[row.id]?.required_course_id || row.required_course_id}
                              onChange={(v) =>
                                setPrereqEdits((s) => ({
                                  ...s,
                                  [row.id]: {
                                    ...(s[row.id] || {}),
                                    required_course_id: v,
                                    relationship_type: s[row.id]?.relationship_type || row.relationship_type || "PREREQUISITE",
                                    enforcement: s[row.id]?.enforcement || row.enforcement || "HARD",
                                  },
                                }))
                              }
                              options={(coursesQ.data || [])
                                .filter((c) => c.id !== selectedCourseId)
                                .map((c) => ({ value: c.id, label: `${c.course_number} - ${c.title}` }))}
                            />
                          ),
                        },
                        {
                          title: "Relationship",
                          render: (_, row) => (
                            <Select
                              style={{ width: 150 }}
                              value={prereqEdits[row.id]?.relationship_type || row.relationship_type || "PREREQUISITE"}
                              onChange={(v) =>
                                setPrereqEdits((s) => ({
                                  ...s,
                                  [row.id]: {
                                    ...(s[row.id] || {}),
                                    required_course_id: s[row.id]?.required_course_id || row.required_course_id,
                                    relationship_type: v,
                                    enforcement: s[row.id]?.enforcement || row.enforcement || "HARD",
                                  },
                                }))
                              }
                              options={[
                                { value: "PREREQUISITE", label: "Prerequisite" },
                                { value: "COREQUISITE", label: "Corequisite" },
                              ]}
                            />
                          ),
                        },
                        {
                          title: "Enforcement",
                          render: (_, row) => (
                            <Select
                              style={{ width: 120 }}
                              value={prereqEdits[row.id]?.enforcement || row.enforcement || "HARD"}
                              onChange={(v) =>
                                setPrereqEdits((s) => ({
                                  ...s,
                                  [row.id]: {
                                    ...(s[row.id] || {}),
                                    required_course_id: s[row.id]?.required_course_id || row.required_course_id,
                                    relationship_type: s[row.id]?.relationship_type || row.relationship_type || "PREREQUISITE",
                                    enforcement: v,
                                  },
                                }))
                              }
                              options={[
                                { value: "HARD", label: "Hard" },
                                { value: "SOFT", label: "Soft" },
                              ]}
                            />
                          ),
                        },
                        {
                          title: "Group",
                          render: (_, row) => (
                            <Space wrap>
                              <Input
                                style={{ width: 110 }}
                                placeholder="Group key"
                                value={prereqEdits[row.id]?.prerequisite_group_key ?? row.prerequisite_group_key ?? ""}
                                onChange={(e) =>
                                  setPrereqEdits((s) => ({
                                    ...s,
                                    [row.id]: {
                                      ...(s[row.id] || {}),
                                      required_course_id: s[row.id]?.required_course_id || row.required_course_id,
                                      relationship_type: s[row.id]?.relationship_type || row.relationship_type || "PREREQUISITE",
                                      enforcement: s[row.id]?.enforcement || row.enforcement || "HARD",
                                      prerequisite_group_key: e.target.value,
                                      group_label: s[row.id]?.group_label ?? row.group_label ?? "",
                                      group_min_required: s[row.id]?.group_min_required ?? String(row.group_min_required || 1),
                                    },
                                  }))
                                }
                              />
                              <Input
                                style={{ width: 130 }}
                                placeholder="Label"
                                value={prereqEdits[row.id]?.group_label ?? row.group_label ?? ""}
                                onChange={(e) =>
                                  setPrereqEdits((s) => ({
                                    ...s,
                                    [row.id]: {
                                      ...(s[row.id] || {}),
                                      required_course_id: s[row.id]?.required_course_id || row.required_course_id,
                                      relationship_type: s[row.id]?.relationship_type || row.relationship_type || "PREREQUISITE",
                                      enforcement: s[row.id]?.enforcement || row.enforcement || "HARD",
                                      prerequisite_group_key: s[row.id]?.prerequisite_group_key ?? row.prerequisite_group_key ?? "",
                                      group_label: e.target.value,
                                      group_min_required: s[row.id]?.group_min_required ?? String(row.group_min_required || 1),
                                    },
                                  }))
                                }
                              />
                              <InputNumber
                                style={{ width: 90 }}
                                min={1}
                                value={Number(prereqEdits[row.id]?.group_min_required ?? row.group_min_required ?? 1)}
                                onChange={(v) =>
                                  setPrereqEdits((s) => ({
                                    ...s,
                                    [row.id]: {
                                      ...(s[row.id] || {}),
                                      required_course_id: s[row.id]?.required_course_id || row.required_course_id,
                                      relationship_type: s[row.id]?.relationship_type || row.relationship_type || "PREREQUISITE",
                                      enforcement: s[row.id]?.enforcement || row.enforcement || "HARD",
                                      prerequisite_group_key: s[row.id]?.prerequisite_group_key ?? row.prerequisite_group_key ?? "",
                                      group_label: s[row.id]?.group_label ?? row.group_label ?? "",
                                      group_min_required: String(v || 1),
                                    },
                                  }))
                                }
                              />
                            </Space>
                          ),
                        },
                        {
                          title: "Action",
                          render: (_, row) => (
                            <Space>
                              <Button size="small" onClick={() => savePrerequisiteRow(row)}>Save</Button>
                              <Button size="small" danger onClick={() => deletePrerequisiteRow(row.id)}>Delete</Button>
                            </Space>
                          ),
                        },
                      ]}
                    />
                  </Space>
                )
              },
              {
                key: "requirements",
                label: "Requirement Links",
                children: (
                  <Space direction="vertical" style={{ width: "100%" }}>
                    <Space wrap>
                      <Select
                        style={{ width: 360 }}
                        placeholder="Requirement to link"
                        value={linkRequirementId}
                        onChange={setLinkRequirementId}
                        showSearch
                        optionFilterProp="label"
                        options={requirementLinkOptions}
                      />
                      <Button onClick={linkRequirementToCourse}>Link Requirement</Button>
                    </Space>
                    <Table
                      size="small"
                      pagination={false}
                      rowKey="id"
                      dataSource={courseDetailQ.data?.requirements || []}
                      columns={[
                        { title: "Requirement", dataIndex: "requirement_name", render: (v) => v || "-" },
                        { title: "Primary", dataIndex: "is_primary", render: (v) => (v ? "Yes" : "No") },
                        {
                          title: "Timing Rule",
                          render: (_, row) => {
                            if (row.required_semester != null) return `${periodShortLabel(row.required_semester)} fixed`;
                            const parts = [];
                            if (row.required_semester_min != null) parts.push(`>= ${periodShortLabel(row.required_semester_min)}`);
                            if (row.required_semester_max != null) parts.push(`<= ${periodShortLabel(row.required_semester_max)}`);
                            return parts.length ? parts.join(", ") : "None";
                          },
                        },
                      ]}
                    />
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
                    <Table
                      size="small"
                      pagination={false}
                      rowKey="id"
                      dataSource={substitutionsQ.data || []}
                      columns={[
                        {
                          title: "Original Course",
                          dataIndex: "original_course_id",
                          render: (v) => courseLabelById[v] || v,
                        },
                        {
                          title: "Substitute Course",
                          dataIndex: "substitute_course_id",
                          render: (v) => courseLabelById[v] || v,
                        },
                        { title: "Bidirectional", dataIndex: "is_bidirectional", render: (v) => (v ? "Yes" : "No") },
                        { title: "Approval", dataIndex: "requires_approval", render: (v) => (v ? "Required" : "Not required") },
                      ]}
                    />
                  </Space>
                )
              },
              {
                key: "resources",
                label: "Sections/Resources",
                children: (
                  <Table
                    size="small"
                    pagination={false}
                    rowKey="id"
                    dataSource={courseDetailQ.data?.resources || []}
                    columns={[
                      { title: "Semester", dataIndex: "semester_label" },
                      { title: "Max Enrollment", dataIndex: "max_enrollment" },
                      { title: "Instructor", dataIndex: "instructor_id", render: (v) => v || "-" },
                      { title: "Classroom", dataIndex: "classroom_id", render: (v) => v || "-" },
                    ]}
                  />
                )
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
          <Modal
            title="Create New Course"
            open={newCourseModalOpen}
            onCancel={() => setNewCourseModalOpen(false)}
            onOk={createCourseFromEditor}
            okText="Create Course"
            confirmLoading={newCourseBusy}
          >
            <Space direction="vertical" style={{ width: "100%" }}>
              <div>
                <Typography.Text type="secondary">Course Number</Typography.Text>
                <Input
                  value={newCourseForm.course_number}
                  onChange={(e) => setNewCourseForm((s) => ({ ...s, course_number: e.target.value }))}
                />
              </div>
              <div>
                <Typography.Text type="secondary">Short Title</Typography.Text>
                <Input
                  value={newCourseForm.title}
                  onChange={(e) => setNewCourseForm((s) => ({ ...s, title: e.target.value }))}
                />
              </div>
              <div>
                <Typography.Text type="secondary">Credit Hours</Typography.Text>
                <Input
                  value={newCourseForm.credit_hours}
                  onChange={(e) => setNewCourseForm((s) => ({ ...s, credit_hours: e.target.value }))}
                />
              </div>
              <div>
                <Typography.Text type="secondary">Fixed Period (Optional)</Typography.Text>
                <Select
                  allowClear
                  style={{ width: "100%" }}
                  value={newCourseForm.designated_semester}
                  onChange={(v) => setNewCourseForm((s) => ({ ...s, designated_semester: v ?? null }))}
                  options={semesterOptions}
                />
              </div>
              <div>
                <Typography.Text type="secondary">Offered Periods JSON (Optional)</Typography.Text>
                <Input
                  value={newCourseForm.offered_periods_json}
                  onChange={(e) => setNewCourseForm((s) => ({ ...s, offered_periods_json: e.target.value }))}
                />
              </div>
              <div>
                <Typography.Text type="secondary">Standing Requirement (Optional)</Typography.Text>
                <Input
                  value={newCourseForm.standing_requirement}
                  onChange={(e) => setNewCourseForm((s) => ({ ...s, standing_requirement: e.target.value }))}
                />
              </div>
              <div>
                <Typography.Text type="secondary">Additional Requirements (Optional)</Typography.Text>
                <Input.TextArea
                  rows={2}
                  value={newCourseForm.additional_requirements_text}
                  onChange={(e) => setNewCourseForm((s) => ({ ...s, additional_requirements_text: e.target.value }))}
                />
              </div>
              <div>
                <Typography.Text type="secondary">Course Owner</Typography.Text>
                <Select
                  style={{ width: "100%" }}
                  value={newCourseForm.ownership_code || "DF"}
                  onChange={(v) => setNewCourseForm((s) => ({ ...s, ownership_code: v || "DF" }))}
                  options={COURSE_OWNERSHIP_OPTIONS}
                />
              </div>
              <div>
                <Typography.Text type="secondary">Minimum Section Size</Typography.Text>
                <Input
                  value={newCourseForm.min_section_size}
                  onChange={(e) => setNewCourseForm((s) => ({ ...s, min_section_size: e.target.value }))}
                />
              </div>
            </Space>
          </Modal>
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
