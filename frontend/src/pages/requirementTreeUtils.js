export function buildBasketDisplayGroups({
  basketId,
  courses = [],
  basketSubRows = [],
  fallbackReqSubRows = [],
  courseMapById = {},
} = {}) {
  const rows = [...(courses || [])];
  if (!rows.length) return [];
  const byCourseId = {};
  rows.forEach((bc) => {
    byCourseId[bc.course_id] = bc;
  });
  const setIds = new Set(rows.map((bc) => bc.course_id).filter(Boolean));
  const subRows = (basketSubRows || []).length ? (basketSubRows || []) : (fallbackReqSubRows || []);
  const adj = {};
  for (const s of subRows) {
    if (!setIds.has(s.primary_course_id) || !setIds.has(s.substitute_course_id)) continue;
    if (!adj[s.primary_course_id]) adj[s.primary_course_id] = new Set();
    if (!adj[s.substitute_course_id]) adj[s.substitute_course_id] = new Set();
    adj[s.primary_course_id].add(s.substitute_course_id);
    adj[s.substitute_course_id].add(s.primary_course_id);
  }
  const seen = new Set();
  const groups = [];
  for (const bc of rows) {
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
    const orderedIds = [...groupIds].sort((a, b) =>
      String(courseMapById[a]?.course_number || courseMapById[a]?.title || a)
        .localeCompare(String(courseMapById[b]?.course_number || courseMapById[b]?.title || b), undefined, { sensitivity: "base" })
    );
    const titleParts = orderedIds.map((cid) => {
      const item = byCourseId[cid];
      return item?.course_number || item?.course_title || courseMapById[cid]?.course_number || courseMapById[cid]?.title || "Missing course";
    });
    const firstItem = byCourseId[orderedIds[0]];
    return {
      key: `basket-course:${basketId}:${firstItem?.id || groupIdx}`,
      sortLabel: titleParts.join(" / "),
      titleParts,
    };
  });
  return displayRows.sort((a, b) =>
    String(a.sortLabel || "").localeCompare(String(b.sortLabel || ""), undefined, { sensitivity: "base" })
  );
}
