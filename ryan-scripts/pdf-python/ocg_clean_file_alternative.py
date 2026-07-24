#!/usr/bin/env python3
"""
The cleaner removes page-local /Xi full-page junk XForms, matching /Xi optional
content blocks, JavaScript actions, protected-cover pages, and protected form
fields used by the digital editions.

Inputs can be files or folders. Folders expand to PDFs in that folder unless
--recursive is supplied.

Usage:
  python ocg_clean_file_alternative.py
  python ocg_clean_file_alternative.py input.pdf another.pdf
  python ocg_clean_file_alternative.py folder --output-dir folder\\cleaned
  python ocg_clean_file_alternative.py input.pdf --output output.pdf
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, cast

from pypdf import PdfReader, PdfWriter
from pypdf.generic import ArrayObject, ContentStream, NameObject

# --- User configuration -----------------------------------------------------

DEFAULT_INPUT_PDFS: Path | str | Sequence[Path | str] = Path(
    r"C:\folder\file.pdf"
)
DEFAULT_OUTPUT: Path | str | None = None
DEFAULT_OUTPUT_DIR: Path | str | None = None
DEFAULT_DECRYPT_PASSWORD = ""
DEFAULT_OUTPUT_SUFFIX = "_cleaned"
DEFAULT_RECURSIVE = False

# ---------------------------------------------------------------------------

PROCESS_FROM_PAGE = 2
PROTECTED_COVER_WARNING = "BROWSERSCANNOTOPENTHISCONTENT"
BOOM_FIELD_PREFIX = "boom"
PROTECTED_INFO_FIELD_NAME = "info"
PROTECTED_INFO_VALUE = "Please Login to View"

PathInput = Path | str
PathInputs = PathInput | Sequence[PathInput]


class _PageLike(Protocol):
    def get(self, key: Any, default: Any | None = None, /) -> Any: ...

    def __setitem__(self, key: Any, value: Any) -> Any: ...

    def __delitem__(self, key: Any, /) -> None: ...


@dataclass
class CleanSummary:
    source: Path
    destination: Path
    pages_processed: int = 0
    ocg_blocks_removed: int = 0
    bad_ocg_page_properties_removed: int = 0
    bad_xforms_removed: int = 0
    do_ops_stripped: int = 0
    js_annotations_removed: int = 0
    boom_widget_annotations_removed: int = 0
    protected_info_widget_annotations_removed: int = 0
    doc_level_js_entries_removed: int = 0
    acroform_js_entries_removed: int = 0
    boom_acroform_fields_removed: int = 0
    protected_info_acroform_fields_removed: int = 0
    doc_level_ocproperties_removed: bool = False
    protected_cover_page_removed: bool = False
    decrypt_status: str = "not encrypted"
    warnings: list[str] = field(default_factory=lambda: [])
    errors: list[str] = field(default_factory=lambda: [])
    page_notes: list[str] = field(default_factory=lambda: [])


def _writer_add_object(writer: PdfWriter, obj: Any) -> Any:
    return cast(Any, writer)._add_object(obj)


def _name_to_key(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("latin1")
        except Exception:
            return repr(value)
    try:
        text = str(value)
    except Exception:
        return None
    return text if text else None


def _resolve_dict(value: Any) -> dict[Any, Any]:
    if value is None:
        return {}
    if hasattr(value, "get_object"):
        try:
            value = value.get_object()
        except Exception:
            return {}
    if isinstance(value, dict):
        return cast(dict[Any, Any], value)
    return {}


def _compact_text(text: str) -> str:
    return "".join(char for char in text.upper() if char.isalnum())


def _normalize_ocg_label(label: str | None) -> str | None:
    if label is None:
        return None
    text = label.strip()
    if text.startswith("/"):
        text = text[1:]
    return text if text else None


def _xi_number(value: Any) -> int | None:
    normalized = _normalize_ocg_label(_name_to_key(value))
    if not normalized or not normalized.startswith("Xi"):
        return None
    suffix = normalized[2:]
    if not suffix.isdecimal():
        return None
    return int(suffix)


def _is_name_like(value: Any, prefix: str) -> bool:
    key = _normalize_ocg_label(_name_to_key(value))
    return bool(key and key.startswith(prefix))


def _is_full_page_form_xobject(name: Any, xobj_dict: dict[Any, Any]) -> bool:
    if not _is_name_like(name, "Xi"):
        return False
    if _name_to_key(xobj_dict.get("/Subtype")) != "/Form":
        return False
    bbox = str(xobj_dict.get("/BBox"))
    return bbox in {
        "[0, 0, 595.28, 841.89]",
        "[0.0, 0.0, 595.28, 841.89]",
        "[0, 0, 612, 792]",
    }


def _clean_bad_xobjects(resources: dict[Any, Any]) -> tuple[set[str], set[str], int]:
    removed_names: set[str] = set()
    bad_property_names: set[str] = set()
    xobjects = resources.get("/XObject")
    if not xobjects:
        return removed_names, bad_property_names, 0
    xobjects_obj = _resolve_dict(xobjects)
    if not xobjects_obj:
        return removed_names, bad_property_names, 0

    for name, ref in list(xobjects_obj.items()):
        xobj = ref
        if hasattr(xobj, "get_object"):
            try:
                xobj = xobj.get_object()
            except Exception:
                continue
        if not isinstance(xobj, dict):
            continue
        xobj_dict = cast(dict[Any, Any], xobj)
        key = _name_to_key(name)
        if not _is_full_page_form_xobject(name, xobj_dict):
            continue
        xi_num = _xi_number(name)
        if xi_num is not None:
            bad_property_names.add(f"/Xi{xi_num + 1}")
        if key:
            removed_names.add(key)
        try:
            del xobjects_obj[name]
        except Exception:
            pass

    if not xobjects_obj:
        try:
            resources.pop("/XObject")
        except Exception:
            pass

    return removed_names, bad_property_names, len(removed_names)


def _remove_bad_ocg_properties(resources: dict[Any, Any], bad_property_names: set[str]) -> int:
    props = resources.get("/Properties")
    if not props:
        return 0
    props_dict = _resolve_dict(props)
    if not props_dict:
        return 0

    removed = 0
    for name in list(props_dict.keys()):
        key = _name_to_key(name)
        norm = _normalize_ocg_label(key)
        if key not in bad_property_names and (not norm or f"/{norm}" not in bad_property_names):
            continue
        try:
            del props_dict[name]
        except Exception:
            pass
        removed += 1

    if not props_dict:
        try:
            del resources["/Properties"]
        except Exception:
            pass
    return removed


def _is_oc_tag(operands: list[Any]) -> bool:
    if not operands:
        return False
    return _name_to_key(operands[0]) == "/OC"


def _content_prop_key(props: Any) -> str | None:
    if hasattr(props, "get_object"):
        try:
            props = props.get_object()
        except Exception:
            return None
    if isinstance(props, dict):
        return None
    return _name_to_key(props)


def _strip_content(
    content_obj: Any,
    reader: PdfReader,
    removed_xobject_names: set[str],
    bad_property_names: set[str],
) -> tuple[ContentStream, int, int, set[str]]:
    content_stream = ContentStream(content_obj, reader)
    new_ops: list[tuple[list[Any], bytes]] = []

    remove_props: set[str] = set()
    for operands, operator in content_stream.operations:
        if operator != b"BDC" or not _is_oc_tag(operands):
            continue
        props = operands[1] if len(operands) > 1 else None
        props_key = _content_prop_key(props)
        norm = _normalize_ocg_label(props_key) if props_key else None
        if props_key and (props_key in bad_property_names or (norm and f"/{norm}" in bad_property_names)):
            remove_props.add(props_key)

    remove_props_norm = {_normalize_ocg_label(name) for name in remove_props if name}
    removed_blocks = 0
    remove_depth = 0
    do_removed = 0

    for operands, operator in content_stream.operations:
        if remove_depth > 0:
            if operator in {b"BDC", b"BMC"}:
                remove_depth += 1
            elif operator == b"EMC":
                remove_depth -= 1
            continue

        if operator == b"BDC" and _is_oc_tag(operands) and remove_props:
            props = operands[1] if len(operands) > 1 else None
            props_key = _content_prop_key(props)
            norm = _normalize_ocg_label(props_key) if props_key else None
            if props_key in remove_props or (norm and norm in remove_props_norm):
                removed_blocks += 1
                remove_depth = 1
            if remove_depth > 0:
                continue

        if operator == b"Do" and operands:
            name = _name_to_key(operands[0])
            if name and name in removed_xobject_names:
                do_removed += 1
                continue

        new_ops.append((operands, operator))

    content_stream.operations = new_ops
    content_stream.get_data()
    return content_stream, removed_blocks, do_removed, remove_props


def default_output_path(input_path: Path, suffix: str = DEFAULT_OUTPUT_SUFFIX) -> Path:
    return input_path.with_name(f"{input_path.stem}{suffix}{input_path.suffix or '.pdf'}")


def _first_page_has_protected_cover(reader: PdfReader) -> bool:
    if not reader.pages:
        return False
    text = reader.pages[0].extract_text() or ""
    return _compact_text(text).count(PROTECTED_COVER_WARNING) >= 2


def _remove_page(writer: PdfWriter, page_index: int) -> bool:
    remove_page = getattr(writer, "remove_page", None)
    if callable(remove_page):
        remove_page(page_index)
        return True

    pages = getattr(writer, "pages", None)
    if pages is not None:
        try:
            del pages[page_index]
            return True
        except Exception:
            return False
    return False


def _strip_js_from_action(action: Any) -> bool:
    action_dict = _resolve_dict(action)
    if not action_dict:
        return False
    if _name_to_key(action_dict.get("/S")) == "/JavaScript":
        return True
    if "/Next" in action_dict:
        next_obj = action_dict.get("/Next")
        if isinstance(next_obj, list):
            next_list = list(cast(list[Any], next_obj))
        elif next_obj is not None:
            next_list = [next_obj]
        else:
            next_list = []
        for item in list(next_list):
            if _strip_js_from_action(item):
                try:
                    next_list.remove(item)
                except Exception:
                    pass
        if not next_list:
            try:
                del action_dict["/Next"]
            except Exception:
                pass
        else:
            action_dict[NameObject("/Next")] = ArrayObject(next_list)
    return False


def _remove_js_entries(container: dict[Any, Any]) -> int:
    removed = 0
    action = container.get("/A")
    if action and _strip_js_from_action(action):
        try:
            del container["/A"]
        except Exception:
            pass
        removed += 1

    aa = container.get("/AA")
    if aa:
        aa_dict = _resolve_dict(aa)
        if aa_dict:
            for key in list(aa_dict.keys()):
                if _strip_js_from_action(aa_dict.get(key)):
                    try:
                        del aa_dict[key]
                    except Exception:
                        pass
                    removed += 1
            if not aa_dict:
                try:
                    del container["/AA"]
                except Exception:
                    pass
    return removed


def _remove_js_from_annotations(page: _PageLike) -> int:
    annotations = page.get("/Annots")
    if not annotations:
        return 0
    annots_obj = annotations
    if hasattr(annots_obj, "get_object"):
        try:
            annots_obj = annots_obj.get_object()
        except Exception:
            return 0
    if not isinstance(annots_obj, list):
        return 0

    removed = 0
    for annot_ref in cast(list[Any], annots_obj):
        annot = annot_ref
        if hasattr(annot, "get_object"):
            try:
                annot = annot.get_object()
            except Exception:
                continue
        if isinstance(annot, dict):
            removed += _remove_js_entries(cast(dict[Any, Any], annot))
    return removed


def _field_name(value: Any, seen: set[int] | None = None) -> str | None:
    field = _resolve_dict(value)
    if not field:
        return None
    if seen is None:
        seen = set()
    marker = id(field)
    if marker in seen:
        return None
    seen.add(marker)

    name = _name_to_key(field.get("/T"))
    if name:
        return name
    parent = field.get("/Parent")
    if parent is None:
        return None
    return _field_name(parent, seen)


def _is_boom_field(value: Any) -> bool:
    name = _field_name(value)
    return bool(name and name.startswith(BOOM_FIELD_PREFIX))


def _is_protected_info_field(value: Any) -> bool:
    field = _resolve_dict(value)
    if not field:
        return False
    name = _field_name(value)
    if name != PROTECTED_INFO_FIELD_NAME:
        return False
    if _name_to_key(field.get("/Subtype")) == "/Widget" and str(field.get("/Rect")) != "[0, 0, 595.28, 841.89]":
        return False
    parent = _resolve_dict(field.get("/Parent"))
    value = _name_to_key(field.get("/V")) or _name_to_key(field.get("/DV"))
    parent_value = _name_to_key(parent.get("/V")) or _name_to_key(parent.get("/DV"))
    return value == PROTECTED_INFO_VALUE or parent_value == PROTECTED_INFO_VALUE


def _remove_target_annotations(page: _PageLike) -> tuple[int, int]:
    annotations = page.get("/Annots")
    if not annotations:
        return 0, 0
    annots_obj = annotations
    if hasattr(annots_obj, "get_object"):
        try:
            annots_obj = annots_obj.get_object()
        except Exception:
            return 0, 0
    if not isinstance(annots_obj, list):
        return 0, 0

    kept: list[Any] = []
    boom_removed = 0
    protected_info_removed = 0
    for annot_ref in cast(list[Any], annots_obj):
        if _is_boom_field(annot_ref):
            boom_removed += 1
            continue
        if _is_protected_info_field(annot_ref):
            protected_info_removed += 1
            continue
        kept.append(annot_ref)

    if kept:
        page[NameObject("/Annots")] = ArrayObject(kept)
    else:
        try:
            del page["/Annots"]
        except Exception:
            pass
    return boom_removed, protected_info_removed


def _remove_js_from_field(field_ref: Any) -> int:
    field = _resolve_dict(field_ref)
    if not field:
        return 0

    removed = _remove_js_entries(field)
    kids_obj = field.get("/Kids", [])
    if isinstance(kids_obj, list):
        for kid in cast(list[Any], kids_obj):
            removed += _remove_js_from_field(kid)
    return removed


def _remove_js_from_acroform(writer: PdfWriter) -> int:
    root_obj = cast(Any, writer)._root_object
    if not isinstance(root_obj, dict):
        return 0
    acroform = _resolve_dict(cast(dict[Any, Any], root_obj).get("/AcroForm"))
    if not acroform:
        return 0

    removed = _remove_js_entries(acroform)
    fields_obj = acroform.get("/Fields", [])
    if isinstance(fields_obj, list):
        for field_ref in cast(list[Any], fields_obj):
            removed += _remove_js_from_field(field_ref)
    return removed


def _remove_boom_acroform_fields(writer: PdfWriter) -> int:
    return _remove_acroform_fields(writer, _is_boom_field)


def _remove_protected_info_acroform_fields(writer: PdfWriter) -> int:
    return _remove_acroform_fields(writer, _is_protected_info_field)


def _remove_acroform_fields(writer: PdfWriter, predicate: Any) -> int:
    root_obj = cast(Any, writer)._root_object
    if not isinstance(root_obj, dict):
        return 0
    acroform = _resolve_dict(cast(dict[Any, Any], root_obj).get("/AcroForm"))
    if not acroform:
        return 0

    fields_obj = acroform.get("/Fields", [])
    if not isinstance(fields_obj, list):
        return 0

    kept: list[Any] = []
    removed = 0
    for field_ref in cast(list[Any], fields_obj):
        if predicate(field_ref):
            removed += 1
            continue
        kept.append(field_ref)

    acroform[NameObject("/Fields")] = ArrayObject(kept)
    return removed


def _remove_doc_ocproperties(writer: PdfWriter) -> bool:
    root_obj = cast(Any, writer)._root_object
    if not isinstance(root_obj, dict):
        return False
    root = cast(dict[Any, Any], root_obj)
    if "/OCProperties" not in root:
        return False
    try:
        del root["/OCProperties"]
    except Exception:
        return False
    return True


def _remove_doc_level_js(writer: PdfWriter) -> int:
    root_obj = cast(Any, writer)._root_object
    if not isinstance(root_obj, dict):
        return 0
    root = cast(dict[Any, Any], root_obj)
    removed = 0
    names_dict = _resolve_dict(root.get("/Names"))
    if names_dict and "/JavaScript" in names_dict:
        try:
            del names_dict["/JavaScript"]
            removed += 1
        except Exception:
            pass
        if not names_dict:
            try:
                del root["/Names"]
            except Exception:
                pass
    open_action = root.get("/OpenAction")
    if open_action and _strip_js_from_action(open_action):
        try:
            del root["/OpenAction"]
        except Exception:
            pass
        removed += 1
    removed += _remove_js_entries(root)
    return removed


def _print_summary(summary: CleanSummary) -> None:
    if summary.page_notes:
        print("\nPer-page notes")
        print("-" * 40)
        for note in summary.page_notes:
            print(f"- {note}")
    if summary.warnings:
        print("\nWarnings")
        print("-" * 40)
        for warning in summary.warnings:
            print(f"- {warning}")
    if summary.errors:
        print("\nErrors")
        print("-" * 40)
        for error in summary.errors:
            print(f"- {error}")

    print("\nSummary")
    print("-" * 40)
    print(f"Source: {summary.source}")
    print(f"Pages processed: {summary.pages_processed}")
    print(f"OCG blocks removed: {summary.ocg_blocks_removed}")
    print(f"Bad OCG page properties removed: {summary.bad_ocg_page_properties_removed}")
    print(f"Bad XForm objects removed: {summary.bad_xforms_removed}")
    print(f"Do ops stripped: {summary.do_ops_stripped}")
    print(f"JS annotations removed: {summary.js_annotations_removed}")
    print(f"Boom widget annotations removed: {summary.boom_widget_annotations_removed}")
    print(f"Protected info widget annotations removed: {summary.protected_info_widget_annotations_removed}")
    print(f"Doc-level JS entries removed: {summary.doc_level_js_entries_removed}")
    print(f"AcroForm JS entries removed: {summary.acroform_js_entries_removed}")
    print(f"Boom AcroForm fields removed: {summary.boom_acroform_fields_removed}")
    print(f"Protected info AcroForm fields removed: {summary.protected_info_acroform_fields_removed}")
    print(f"Doc-level OCProperties removed: {summary.doc_level_ocproperties_removed}")
    print(f"Protected cover page removed: {summary.protected_cover_page_removed}")
    print(f"Decrypt status: {summary.decrypt_status}")
    print(f"Warnings: {len(summary.warnings)}")
    print(f"Errors: {len(summary.errors)}")
    print(f"Wrote: {summary.destination}")


def clean_pdf_file(
    src: PathInput,
    dst: PathInput | None = None,
    password: str = DEFAULT_DECRYPT_PASSWORD,
    show_progress: bool = True,
) -> CleanSummary:
    source_path = Path(src)
    destination_path = Path(dst) if dst is not None else default_output_path(source_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {source_path}")
    if source_path.resolve() == destination_path.resolve():
        raise ValueError(f"Refusing to overwrite input PDF: {source_path}")

    reader = PdfReader(str(source_path))
    writer = PdfWriter()
    summary = CleanSummary(source=source_path, destination=destination_path)

    if reader.is_encrypted:
        try:
            result = reader.decrypt(password)
        except Exception as exc:
            raise RuntimeError(f"Decrypt failed for {source_path}: {type(exc).__name__}: {exc}") from exc
        if result == 0:
            raise RuntimeError(f"Decrypt failed for {source_path}: invalid or missing password.")
        summary.decrypt_status = f"decrypted (result={result})"
        summary.warnings.append("Input was encrypted; permissions removed by decrypt before processing.")

    def process_page(page: _PageLike, page_num: int) -> None:
        def record_error(stage: str, exc: Exception) -> None:
            summary.errors.append(f"Page {page_num} [{stage}]: {type(exc).__name__}: {exc}")

        try:
            summary.js_annotations_removed += _remove_js_from_annotations(page)
        except Exception as exc:
            record_error("js_annots", exc)

        try:
            boom_annots, protected_info_annots = _remove_target_annotations(page)
            summary.boom_widget_annotations_removed += boom_annots
            summary.protected_info_widget_annotations_removed += protected_info_annots
        except Exception as exc:
            record_error("target_annots", exc)

        if page_num < PROCESS_FROM_PAGE:
            summary.page_notes.append(f"Page {page_num}: skipped")
            return

        try:
            resources = _resolve_dict(page.get("/Resources"))
        except Exception as exc:
            record_error("resources", exc)
            resources = {}

        try:
            removed_xobject_names, bad_property_names, xforms_removed = _clean_bad_xobjects(resources)
            summary.bad_xforms_removed += xforms_removed
        except Exception as exc:
            record_error("xobject", exc)
            removed_xobject_names: set[str] = set()
            bad_property_names: set[str] = set()

        try:
            content_obj = page.get("/Contents")
            if content_obj:
                content_stream, removed_blocks, do_removed, removed_props = _strip_content(
                    content_obj, reader, removed_xobject_names, bad_property_names
                )
                summary.ocg_blocks_removed += removed_blocks
                summary.do_ops_stripped += do_removed
                page[NameObject("/Contents")] = _writer_add_object(writer, content_stream)
                if removed_props:
                    removed_list = ", ".join(sorted(removed_props))
                    summary.page_notes.append(f"Page {page_num}: removed OCG blocks {removed_list}")
            else:
                summary.warnings.append(f"Page {page_num}: missing /Contents")
        except Exception as exc:
            record_error("content", exc)

        try:
            summary.bad_ocg_page_properties_removed += _remove_bad_ocg_properties(resources, bad_property_names)
        except Exception as exc:
            record_error("properties", exc)

    total_pages = len(reader.pages)

    def process_page_with_progress(page: _PageLike) -> None:
        summary.pages_processed += 1
        if show_progress:
            print(f"Processing {source_path.name}: page {summary.pages_processed}/{total_pages}")
        process_page(page, summary.pages_processed)

    remove_first_page = _first_page_has_protected_cover(reader)
    writer.clone_document_from_reader(reader, after_page_append=process_page_with_progress)
    if remove_first_page:
        summary.protected_cover_page_removed = _remove_page(writer, 0)
        if summary.protected_cover_page_removed:
            summary.page_notes.append("Page 1: deleted protected cover page")
        else:
            summary.warnings.append("Page 1 matched protected cover pattern but could not be deleted.")

    summary.doc_level_js_entries_removed = _remove_doc_level_js(writer)
    summary.acroform_js_entries_removed = _remove_js_from_acroform(writer)
    summary.boom_acroform_fields_removed = _remove_boom_acroform_fields(writer)
    summary.protected_info_acroform_fields_removed = _remove_protected_info_acroform_fields(writer)
    summary.doc_level_ocproperties_removed = _remove_doc_ocproperties(writer)

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("wb") as handle:
        writer.write(handle)

    return summary


def normalize_inputs(inputs: PathInputs | None, recursive: bool = DEFAULT_RECURSIVE) -> list[Path]:
    if inputs is None:
        return []

    raw_paths = [Path(inputs)] if isinstance(inputs, Path | str) else [Path(input_path) for input_path in inputs]
    pdf_paths: list[Path] = []
    for path in raw_paths:
        if path.is_dir():
            pattern = "**/*.pdf" if recursive else "*.pdf"
            pdf_paths.extend(sorted(path.glob(pattern)))
        else:
            pdf_paths.append(path)
    return pdf_paths


def resolve_output_path(src: Path, output: Path | None, output_dir: Path | None, input_count: int) -> Path:
    if output_dir is not None:
        return output_dir / default_output_path(src).name

    if output is None:
        return default_output_path(src)

    if input_count == 1 and output.suffix:
        return output

    return output / default_output_path(src).name


def clean_pdfs(
    inputs: PathInputs,
    output: PathInput | None = None,
    output_dir: PathInput | None = None,
    password: str = DEFAULT_DECRYPT_PASSWORD,
    recursive: bool = DEFAULT_RECURSIVE,
    show_progress: bool = True,
) -> list[CleanSummary]:
    input_paths = normalize_inputs(inputs, recursive=recursive)
    if not input_paths:
        raise ValueError("No input PDFs were supplied.")

    output_path = Path(output) if output is not None else None
    output_dir_path = Path(output_dir) if output_dir is not None else None
    if output_path is not None and output_dir_path is not None:
        raise ValueError("Use either output or output_dir, not both.")
    if output_path is not None and len(input_paths) > 1 and output_path.suffix:
        raise ValueError("For multiple inputs, output must be a folder. Use output_dir for clarity.")

    summaries: list[CleanSummary] = []
    for src in input_paths:
        dst = resolve_output_path(src=src, output=output_path, output_dir=output_dir_path, input_count=len(input_paths))
        summaries.append(clean_pdf_file(src=src, dst=dst, password=password, show_progress=show_progress))
    return summaries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean Vitrium/ProtectedPDF noise from ANCOLD PDFs.")
    parser.add_argument("input_pdfs", nargs="*", type=Path, help="Input PDF path(s) or folder(s).")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output PDF for one input, or output folder for multiple inputs.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output folder. Each PDF is written with the default cleaned filename.",
    )
    parser.add_argument("--password", default=DEFAULT_DECRYPT_PASSWORD)
    parser.add_argument("--recursive", action="store_true", help="Expand PDFs in input folders recursively.")
    parser.add_argument("--quiet", action="store_true", help="Only print the final one-line result per PDF.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    inputs = args.input_pdfs if args.input_pdfs else DEFAULT_INPUT_PDFS
    output = args.output if args.output is not None else DEFAULT_OUTPUT
    output_dir = args.output_dir if args.output_dir is not None else DEFAULT_OUTPUT_DIR
    recursive = args.recursive or DEFAULT_RECURSIVE

    try:
        summaries = clean_pdfs(
            inputs=inputs,
            output=output,
            output_dir=output_dir,
            password=args.password,
            recursive=recursive,
            show_progress=not args.quiet,
        )
    except (FileNotFoundError, OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    for summary in summaries:
        if args.quiet:
            print(f"Wrote: {summary.destination} ({len(summary.errors)} errors)")
        else:
            _print_summary(summary)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
