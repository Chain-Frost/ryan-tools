# word_style_audit.py
"""
Word style audit (DOCX)

Produces:
1) A list of ALL styles defined in the document, with their explicit parameters.
   - Inherited properties (from basedOn) are NOT expanded; we only report explicit overrides.
2) A list of ALL styles APPLIED in the document (paragraph/character/table), with counts.

Usage:
  python word_style_audit.py path/to/document.docx
  python word_style_audit.py path/to/document.docx --out-dir reports

Outputs:
  - styles_defined.json
  - styles_applied.csv
  - styles_applied_summary.json
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree as ET

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
NS = {"w": W_NS}

# Optional local override: if set, this path takes precedence over CLI args.
HARDCODED_DOCX_PATH: Path | None = Path(
    r"C:\Users\Ryan.Brook\Downloads\N-0001894-5000-C-REP-10003_A ABH Constructability Report.docx"
)


def qn(tag: str) -> str:
    """Expand a namespace-prefixed tag like 'w:style' to '{...}style'."""
    prefix, local = tag.split(":")
    if prefix != "w":
        raise ValueError(f"Unsupported prefix: {prefix}")
    return f"{{{W_NS}}}{local}"


def first(el: ET.Element | None, path: str) -> ET.Element | None:
    if el is None:
        return None
    return el.find(path, NS)


def findall(el: ET.Element | None, path: str) -> list[ET.Element]:
    if el is None:
        return []
    return el.findall(path, NS)


def attr(el: ET.Element | None, name: str) -> str | None:
    if el is None:
        return None
    return el.get(qn(name) if ":" in name else name)


def text(el: ET.Element | None) -> str | None:
    if el is None:
        return None
    return el.text


def get_val(el: ET.Element | None) -> str | None:
    # In WordprocessingML many properties store values in w:val
    if el is None:
        return None
    return el.get(qn("w:val"))


def int_or_none(s: str | None) -> int | None:
    if s is None:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def read_docx_part(docx_path: Path, part_name: str) -> bytes:
    with zipfile.ZipFile(docx_path, "r") as zf:
        return zf.read(part_name)


def parse_xml_bytes(xml_bytes: bytes) -> ET.Element:
    return ET.fromstring(xml_bytes)


@dataclass(frozen=True)
class StyleId:
    style_id: str
    style_type: str  # paragraph, character, table, numbering, etc.


def _bool_prop_present(el: ET.Element | None, tag: str) -> bool:
    """Some boolean properties are represented by tag presence."""
    return first(el, f"./w:{tag}") is not None


def _extract_rpr(rpr: ET.Element | None) -> dict[str, object]:
    """Explicit run properties."""
    if rpr is None:
        return {}

    out: dict[str, object] = {}

    # Fonts (theme fonts exist too; we report explicit attributes if present)
    rfonts = first(rpr, "./w:rFonts")
    if rfonts is not None:
        out["rFonts"] = {
            k: v
            for k, v in {
                "ascii": rfonts.get(qn("w:ascii")),
                "hAnsi": rfonts.get(qn("w:hAnsi")),
                "eastAsia": rfonts.get(qn("w:eastAsia")),
                "cs": rfonts.get(qn("w:cs")),
                "asciiTheme": rfonts.get(qn("w:asciiTheme")),
                "hAnsiTheme": rfonts.get(qn("w:hAnsiTheme")),
                "eastAsiaTheme": rfonts.get(qn("w:eastAsiaTheme")),
                "csTheme": rfonts.get(qn("w:csTheme")),
            }.items()
            if v is not None
        }

    # Size: stored as half-points (e.g., 22 => 11pt)
    sz = get_val(first(rpr, "./w:sz"))
    szcs = get_val(first(rpr, "./w:szCs"))
    if sz is not None:
        out["sz_half_points"] = int_or_none(sz)
    if szcs is not None:
        out["szCs_half_points"] = int_or_none(szcs)

    # Bold/italic/underline/etc (presence means "on" unless w:val="0"/"false")
    def onoff(tag: str) -> bool | None:
        e = first(rpr, f"./w:{tag}")
        if e is None:
            return None
        v = e.get(qn("w:val"))
        if v is None:
            return True
        return v not in {"0", "false", "off"}

    for k in ["b", "bCs", "i", "iCs", "caps", "smallCaps", "strike", "dstrike"]:
        v = onoff(k)
        if v is not None:
            out[k] = v

    u = first(rpr, "./w:u")
    if u is not None:
        out["u"] = {
            k: v
            for k, v in {
                "val": u.get(qn("w:val")),
                "color": u.get(qn("w:color")),
            }.items()
            if v is not None
        }

    color = first(rpr, "./w:color")
    if color is not None:
        out["color"] = {k: v for k, v in {"val": color.get(qn("w:val"))}.items() if v}

    highlight = first(rpr, "./w:highlight")
    if highlight is not None:
        out["highlight"] = {k: v for k, v in {"val": highlight.get(qn("w:val"))}.items() if v}

    lang = first(rpr, "./w:lang")
    if lang is not None:
        out["lang"] = {
            k: v
            for k, v in {
                "val": lang.get(qn("w:val")),
                "eastAsia": lang.get(qn("w:eastAsia")),
                "bidi": lang.get(qn("w:bidi")),
            }.items()
            if v is not None
        }

    return out


def _extract_ppr(ppr: ET.Element | None) -> dict[str, object]:
    """Explicit paragraph properties."""
    if ppr is None:
        return {}

    out: dict[str, object] = {}

    jc = first(ppr, "./w:jc")
    if jc is not None:
        out["jc"] = jc.get(qn("w:val"))

    spacing = first(ppr, "./w:spacing")
    if spacing is not None:
        out["spacing"] = {
            k: v
            for k, v in {
                "before": spacing.get(qn("w:before")),
                "after": spacing.get(qn("w:after")),
                "line": spacing.get(qn("w:line")),
                "lineRule": spacing.get(qn("w:lineRule")),
                "beforeAutospacing": spacing.get(qn("w:beforeAutospacing")),
                "afterAutospacing": spacing.get(qn("w:afterAutospacing")),
            }.items()
            if v is not None
        }

    ind = first(ppr, "./w:ind")
    if ind is not None:
        out["ind"] = {
            k: v
            for k, v in {
                "left": ind.get(qn("w:left")),
                "right": ind.get(qn("w:right")),
                "firstLine": ind.get(qn("w:firstLine")),
                "hanging": ind.get(qn("w:hanging")),
            }.items()
            if v is not None
        }

    outline = first(ppr, "./w:outlineLvl")
    if outline is not None:
        out["outlineLvl"] = int_or_none(outline.get(qn("w:val")))

    keep_next = first(ppr, "./w:keepNext")
    if keep_next is not None:
        out["keepNext"] = True

    keep_lines = first(ppr, "./w:keepLines")
    if keep_lines is not None:
        out["keepLines"] = True

    wid_ctrl = first(ppr, "./w:widowControl")
    if wid_ctrl is not None:
        v = wid_ctrl.get(qn("w:val"))
        out["widowControl"] = (v not in {"0", "false", "off"}) if v is not None else True

    # Numbering linkage (style-level list definition linkage)
    numpr = first(ppr, "./w:numPr")
    if numpr is not None:
        out["numPr"] = {
            "ilvl": int_or_none(get_val(first(numpr, "./w:ilvl"))),
            "numId": int_or_none(get_val(first(numpr, "./w:numId"))),
        }

    return out


def _extract_tblpr(tblpr: ET.Element | None) -> dict[str, object]:
    """Explicit table style properties (minimal useful subset)."""
    if tblpr is None:
        return {}
    out: dict[str, object] = {}

    tblw = first(tblpr, "./w:tblW")
    if tblw is not None:
        out["tblW"] = {
            k: v
            for k, v in {
                "w": tblw.get(qn("w:w")),
                "type": tblw.get(qn("w:type")),
            }.items()
            if v is not None
        }

    tbl_borders = first(tblpr, "./w:tblBorders")
    if tbl_borders is not None:
        borders: dict[str, dict[str, str]] = {}
        for side in ["top", "left", "bottom", "right", "insideH", "insideV"]:
            s = first(tbl_borders, f"./w:{side}")
            if s is None:
                continue
            borders[side] = {
                k: v
                for k, v in {
                    "val": s.get(qn("w:val")),
                    "sz": s.get(qn("w:sz")),
                    "space": s.get(qn("w:space")),
                    "color": s.get(qn("w:color")),
                }.items()
                if v is not None
            }
        if borders:
            out["tblBorders"] = borders

    tbl_look = first(tblpr, "./w:tblLook")
    if tbl_look is not None:
        out["tblLook"] = {k: v for k, v in tbl_look.attrib.items()}

    return out


def extract_defined_styles(docx_path: Path) -> dict[str, object]:
    styles_xml = read_docx_part(docx_path, "word/styles.xml")
    root = parse_xml_bytes(styles_xml)

    styles: list[dict[str, object]] = []

    for style in findall(root, "./w:style"):
        style_id = style.get(qn("w:styleId"))
        style_type = style.get(qn("w:type"))
        if not style_id or not style_type:
            continue

        name_el = first(style, "./w:name")
        based_on_el = first(style, "./w:basedOn")
        next_el = first(style, "./w:next")
        link_el = first(style, "./w:link")

        entry: dict[str, object] = {
            "styleId": style_id,
            "type": style_type,
            "name": (name_el.get(qn("w:val")) if name_el is not None else None),
            "basedOn": (based_on_el.get(qn("w:val")) if based_on_el is not None else None),
            "next": (next_el.get(qn("w:val")) if next_el is not None else None),
            "link": (link_el.get(qn("w:val")) if link_el is not None else None),
            "uiPriority": int_or_none(get_val(first(style, "./w:uiPriority"))),
            "qFormat": first(style, "./w:qFormat") is not None,
            "semiHidden": first(style, "./w:semiHidden") is not None,
            "unhideWhenUsed": first(style, "./w:unhideWhenUsed") is not None,
            "customStyle": (style.get(qn("w:customStyle")) == "1"),
        }

        # Explicit property blocks (only what's overridden here)
        ppr = first(style, "./w:pPr")
        rpr = first(style, "./w:rPr")
        tblpr = first(style, "./w:tblPr")

        ppr_out = _extract_ppr(ppr)
        rpr_out = _extract_rpr(rpr)
        tblpr_out = _extract_tblpr(tblpr)

        if ppr_out:
            entry["pPr"] = ppr_out
        if rpr_out:
            entry["rPr"] = rpr_out
        if tblpr_out:
            entry["tblPr"] = tblpr_out

        styles.append(entry)

    # Also capture docDefaults (baseline defaults that styles inherit from)
    doc_defaults = {}
    dd = first(root, "./w:docDefaults")
    if dd is not None:
        rpr_default = first(dd, "./w:rPrDefault/w:rPr")
        ppr_default = first(dd, "./w:pPrDefault/w:pPr")
        rpr_out = _extract_rpr(rpr_default)
        ppr_out = _extract_ppr(ppr_default)
        if rpr_out:
            doc_defaults["rPrDefault"] = rpr_out
        if ppr_out:
            doc_defaults["pPrDefault"] = ppr_out

    return {"docx": str(docx_path), "docDefaults": doc_defaults, "styles": styles}


def extract_applied_styles(docx_path: Path) -> dict[str, object]:
    document_xml = read_docx_part(docx_path, "word/document.xml")
    root = parse_xml_bytes(document_xml)

    # Applied style counters
    applied_para: Counter[str] = Counter()
    applied_char: Counter[str] = Counter()
    applied_table: Counter[str] = Counter()

    # For reporting where styles appear (optional but useful)
    # We'll store first N occurrences for each style.
    occurrences: dict[str, list[dict[str, object]]] = defaultdict(list)
    max_occ = 20

    # Paragraphs
    paras = findall(root, ".//w:p")
    for i, p in enumerate(paras, start=1):
        ppr = first(p, "./w:pPr")
        pstyle = get_val(first(ppr, "./w:pStyle")) if ppr is not None else None
        if pstyle:
            applied_para[pstyle] += 1
            if len(occurrences[f"p:{pstyle}"]) < max_occ:
                occurrences[f"p:{pstyle}"].append({"paragraph_index": i})

        # Character styles in runs
        runs = findall(p, "./w:r")
        for r in runs:
            rpr = first(r, "./w:rPr")
            rstyle = get_val(first(rpr, "./w:rStyle")) if rpr is not None else None
            if rstyle:
                applied_char[rstyle] += 1

    # Tables (table style is on tblPr/tblStyle)
    tables = findall(root, ".//w:tbl")
    for t_i, tbl in enumerate(tables, start=1):
        tblpr = first(tbl, "./w:tblPr")
        tbl_style = get_val(first(tblpr, "./w:tblStyle")) if tblpr is not None else None
        if tbl_style:
            applied_table[tbl_style] += 1
            if len(occurrences[f"t:{tbl_style}"]) < max_occ:
                occurrences[f"t:{tbl_style}"].append({"table_index": t_i})

    def counter_to_sorted(c: Counter[str]) -> list[dict[str, object]]:
        return [{"styleId": k, "count": v} for k, v in sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))]

    return {
        "docx": str(docx_path),
        "applied": {
            "paragraph": counter_to_sorted(applied_para),
            "character": counter_to_sorted(applied_char),
            "table": counter_to_sorted(applied_table),
        },
        "occurrences_sample": occurrences,
    }


def write_outputs(
    out_dir: Path,
    defined: dict[str, object],
    applied: dict[str, object],
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "styles_defined.json").write_text(json.dumps(defined, indent=2), encoding="utf-8")
    (out_dir / "styles_applied_summary.json").write_text(json.dumps(applied, indent=2), encoding="utf-8")

    # Flatten applied styles into CSV
    rows: list[dict[str, object]] = []
    for kind in ["paragraph", "character", "table"]:
        for item in applied["applied"][kind]:
            rows.append({"kind": kind, "styleId": item["styleId"], "count": item["count"]})

    csv_path = out_dir / "styles_applied.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["kind", "styleId", "count"])
        w.writeheader()
        w.writerows(rows)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Audit Word DOCX styles (defined + applied).")
    ap.add_argument("docx", type=Path, nargs="?", help="Path to .docx file")
    ap.add_argument("--out-dir", type=Path, default=Path("style_audit_report"), help="Output directory")
    args = ap.parse_args(argv)
    docx_path = HARDCODED_DOCX_PATH if HARDCODED_DOCX_PATH is not None else args.docx
    if docx_path is None:
        ap.print_usage(sys.stderr)
        print("word_style_audit.py: error: docx is required unless HARDCODED_DOCX_PATH is set.", file=sys.stderr)
        return 2

    if not docx_path.exists():
        print(f"ERROR: File not found: {docx_path}", file=sys.stderr)
        return 2
    if docx_path.suffix.lower() != ".docx":
        print("ERROR: Input must be a .docx file (not .doc, not .dotx).", file=sys.stderr)
        return 2

    try:
        defined = extract_defined_styles(docx_path)
        applied = extract_applied_styles(docx_path)
        write_outputs(args.out_dir, defined, applied)
    except zipfile.BadZipFile:
        print("ERROR: Not a valid .docx (zip) file.", file=sys.stderr)
        return 2
    except KeyError as e:
        print(f"ERROR: Missing expected DOCX part: {e}", file=sys.stderr)
        return 2
    except ET.ParseError as e:
        print(f"ERROR: XML parse failed: {e}", file=sys.stderr)
        return 2

    print(f"Wrote reports to: {args.out_dir.resolve()}")
    print(" - styles_defined.json (all style definitions; explicit overrides only)")
    print(" - styles_applied.csv (applied style IDs + counts)")
    print(" - styles_applied_summary.json (same + sample occurrences)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
