export function getCoreRuleSelectedIds(row, coreRuleChoiceOptionsByReq = {}) {
  const groupIds =
    (coreRuleChoiceOptionsByReq[row.requirement_id] || []).find((o) => o.value === row.primary_course_id)?.group_course_ids || [];
  const explicitSelected = groupIds.length > 1
    ? Array.from(new Set((row.substitute_course_ids || []).filter(Boolean))).filter((id) => groupIds.includes(id))
    : [];
  const selectedIds = explicitSelected.length
    ? explicitSelected
    : (groupIds.length > 1 ? groupIds : (row.primary_course_id ? [row.primary_course_id] : groupIds));
  return {
    groupIds,
    explicitSelected,
    selectedIds,
  };
}

export function buildCoreRuleGroupsFromRows(coreRulesRows, coreRuleChoiceOptionsByReq = {}, courseMapById = {}) {
  return (coreRulesRows || [])
    .filter((r) => r.primary_course_id)
    .map((r) => {
      const { selectedIds } = getCoreRuleSelectedIds(r, coreRuleChoiceOptionsByReq);
      const seen = new Set();
      const nums = selectedIds
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
}
