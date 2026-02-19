export function parseRuleConfig(rule) {
  try {
    return JSON.parse(rule?.config_json || "{}");
  } catch {
    return {};
  }
}

export function inferValidationDomain(rule) {
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

export function compareRuleOrder(a, b) {
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
