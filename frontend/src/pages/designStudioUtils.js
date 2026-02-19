export function withDot(code) {
  const t = String(code || "").trim();
  if (!t) return "";
  return t.endsWith(".") ? t : `${t}.`;
}

export function formatRequirementName(name, logicType, pickN, optionTotal) {
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

export function normalizeStatusToken(raw, fallback = "PASS") {
  const token = String(raw || "").trim().toUpperCase();
  if (!token) return fallback;
  if (token === "WARNING") return "WARN";
  return token;
}
