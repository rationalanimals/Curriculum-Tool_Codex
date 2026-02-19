import { formatRequirementName } from "./designStudioUtils";

export function buildCoreRuleRequirementMeta(optionalCoreReqs, coreRuleChoiceOptionsByReq = {}) {
  return (optionalCoreReqs || []).map((req) => {
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
}

export function buildInitialCoreRuleRows(optionalCoreReqs, reqMeta) {
  const rows = [];
  for (const req of optionalCoreReqs || []) {
    const logic = String(req.logic_type || "ALL_REQUIRED").toUpperCase();
    const meta = (reqMeta || []).find((m) => m.requirement_id === req.id);
    const slotCount =
      logic === "PICK_N" || logic === "ANY_N"
        ? Math.max(1, Number(req.pick_n || 1))
        : 0;
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
  return rows;
}
