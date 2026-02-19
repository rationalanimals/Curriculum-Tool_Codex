export function resolveCourseIdFromToken(
  token,
  {
    courseMapById = {},
    courseIdByNumber = {},
    courseIdByNormalizedNumber = {},
    normalizeCourseNumber = (v) => String(v || "").trim(),
  } = {}
) {
  const raw = String(token || "").trim();
  if (!raw) return undefined;
  if (courseMapById[raw]) return raw;
  const byExact = courseIdByNumber[raw];
  if (byExact) return byExact;
  const byNorm = courseIdByNormalizedNumber[normalizeCourseNumber(raw)];
  if (byNorm) return byNorm;
  return undefined;
}

export function normalizeCoreRuleCourseNumbers(
  tokens,
  {
    courseMapById = {},
    courseIdByNumber = {},
    courseIdByNormalizedNumber = {},
    normalizeCourseNumber = (v) => String(v || "").trim(),
  } = {}
) {
  return (tokens || [])
    .map((t) => String(t || "").trim())
    .filter(Boolean)
    .map((t) => {
      const cid = resolveCourseIdFromToken(t, {
        courseMapById,
        courseIdByNumber,
        courseIdByNormalizedNumber,
        normalizeCourseNumber,
      });
      return cid ? (courseMapById[cid]?.course_number || t) : t;
    });
}

export function inferCoreRequirementIdFromCourseIds(courseIds, coreRuleChoiceOptionsByReq = {}) {
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

export function isFullChoiceGroupSelection(reqId, selectedIds, coreRuleChoiceOptionsByReq = {}) {
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
