import argparse
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCX_PATH = ROOT / "COI_2025-2026.docx"
OUT_DIR = ROOT / "docs" / "sequence-fragments"

NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


def para_text(el) -> str:
    return "".join((t.text or "") for t in el.findall(".//w:t", NS)).strip()


def tbl_rows(tbl):
    rows = []
    for tr in tbl.findall("./w:tr", NS):
        cells = []
        for tc in tr.findall("./w:tc", NS):
            paras = [para_text(p) for p in tc.findall("./w:p", NS)]
            paras = [p for p in paras if p]
            cells.append(paras)
        if cells:
            rows.append(cells)
    return rows


def flatten_table(rows):
    out = []
    for r in rows:
        line_cells = []
        for cell_paras in r:
            if not cell_paras:
                line_cells.append("")
            else:
                line_cells.append(" || ".join(cell_paras))
        out.append(" | ".join(line_cells))
    return out


def extract_section(children, major_heading: str):
    start = None
    for i, ch in enumerate(children):
        if ch.tag.endswith("}p"):
            txt = para_text(ch)
            if txt == major_heading:
                start = i
                break
    if start is None:
        raise RuntimeError(f"Major heading not found: {major_heading}")
    seq_start = None
    for i in range(start, len(children)):
        ch = children[i]
        if ch.tag.endswith("}p") and para_text(ch) == "SUGGESTED COURSE SEQUENCE":
            seq_start = i
            break
    if seq_start is None:
        raise RuntimeError(f"No suggested sequence marker for {major_heading}")
    seq_end = None
    for i in range(seq_start + 1, len(children)):
        ch = children[i]
        if ch.tag.endswith("}p"):
            txt = para_text(ch)
            if txt.startswith("Course Unit Summary") or txt in {"AEROSPACE MATERIALS MINOR", "ASTRONAUTICAL ENGINEERING"}:
                seq_end = i
                break
    if seq_end is None:
        seq_end = min(len(children), seq_start + 80)
    return seq_start, seq_end


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--major", required=True, help='Exact heading, e.g. "AERONAUTICAL ENGINEERING"')
    args = ap.parse_args()

    xml = zipfile.ZipFile(DOCX_PATH).read("word/document.xml")
    root = ET.fromstring(xml)
    body = root.find("w:body", NS)
    children = list(body)

    seq_start, seq_end = extract_section(children, args.major)
    lines = []
    lines.append(f"Major heading: {args.major}")
    lines.append(f"Doc child window: {seq_start}..{seq_end}")
    lines.append("")
    for i in range(seq_start, seq_end):
        ch = children[i]
        if ch.tag.endswith("}p"):
            txt = para_text(ch)
            if txt:
                lines.append(f"{i:05d} PAR: {txt}")
        elif ch.tag.endswith("}tbl"):
            rows = tbl_rows(ch)
            lines.append(f"{i:05d} TBL:")
            for row in flatten_table(rows):
                lines.append(f"      {row}")

    raw = "\n".join(lines).strip() + "\n"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", args.major.title()).strip("_")
    out_path = OUT_DIR / f"{safe}_suggested_sequence_fragments.txt"
    out_path.write_text(raw, encoding="utf-8")
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()

