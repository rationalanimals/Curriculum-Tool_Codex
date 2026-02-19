export function buildSubstituteGroupsFromRows(rows, courseIds, courseMapById = {}) {
  const idSet = new Set((courseIds || []).filter(Boolean));
  if (!idSet.size) return [];
  const filtered = (rows || []).filter(
    (s) => idSet.has(s.primary_course_id) && idSet.has(s.substitute_course_id)
  );
  if (!filtered.length) return [];
  const adj = {};
  for (const s of filtered) {
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

export function sanitizeBasketSubGroupRows(rows, validCourseIds) {
  const ids = Array.from(new Set((validCourseIds || []).filter(Boolean)));
  return (rows || [])
    .map((r) => {
      const primary = ids.includes(r.primary_course_id) ? r.primary_course_id : undefined;
      const subs = Array.from(
        new Set((r.substitute_course_ids || []).filter((x) => x && x !== primary && ids.includes(x)))
      );
      return { primary_course_id: primary, substitute_course_ids: subs };
    })
    .filter((r) => r.primary_course_id && (r.substitute_course_ids || []).length);
}

export function computeBasketValidationErrors({
  reqLinkKind,
  basketCourseIds,
  basketMinCount,
  basketSelectedId,
  basketName,
  requirementNodeMap,
  basketRequirementId,
  basketLinkId,
  basketSubGroupRows,
}) {
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
}

export function normalizeCoursePair(a, b) {
  return [a, b].sort().join("|");
}
