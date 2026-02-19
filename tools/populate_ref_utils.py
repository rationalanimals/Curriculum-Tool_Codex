import re
from typing import Callable


RAW_PREFIX_ALIASES: dict[str, str] = {
    "CREATIVE ART": "CREAT ART",
    "PHYS ": "PHYSICS ",
}


def _apply_raw_prefix_alias(raw: str) -> str:
    s = str(raw).strip()
    up = s.upper()
    for src, dst in RAW_PREFIX_ALIASES.items():
        if up.startswith(src):
            return dst + s[len(src) :]
    return s


def resolve_single_course_id(
    course_map: dict[str, str],
    raw: str,
    normalize_fn: Callable[[str], str],
) -> str | None:
    # First try exact normalization.
    key = normalize_fn(raw)
    cid = course_map.get(key)
    if cid:
        return cid

    # Try known raw prefix aliases (e.g., Creative Art -> Creat Art).
    alias_raw = _apply_raw_prefix_alias(raw)
    if alias_raw != raw:
        key = normalize_fn(alias_raw)
        cid = course_map.get(key)
        if cid:
            return cid

    # Try dropping one trailing suffix letter if the base course exists.
    m = re.match(r"^([A-Z]{2,20}\s)(\d{3})([A-Z])$", str(key))
    if m:
        base_key = f"{m.group(1)}{m.group(2)}"
        cid = course_map.get(base_key)
        if cid:
            return cid
    return None


def resolve_course_ids_strict(
    course_map: dict[str, str],
    numbers: list[str],
    normalize_fn: Callable[[str], str],
    label: str = "course refs",
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    missing: list[str] = []
    for raw in numbers:
        cid = resolve_single_course_id(course_map, raw, normalize_fn)
        if not cid:
            missing.append(raw)
            continue
        if cid in seen:
            continue
        seen.add(cid)
        out.append(cid)
    if missing:
        raise RuntimeError(f"Unresolved {label}: {missing}")
    return out

