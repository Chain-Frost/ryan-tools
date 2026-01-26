# ryan-scripts\misc-python\ocg_clean_file.py

"""
Remove the smaller-numbered OCG marked-content block per page, drop XForm /OC
objects, strip JavaScript actions, and remove /Widget annotations.

Usage:
  python3 ocg_clean_file.py input.pdf -o output.pdf
  or set the DEFAULT_INPUT_PDF
  python3 ocg_clean_file.py
"""

from __future__ import annotations

import os
import re
import sys
from typing import Any

from pypdf import PdfReader, PdfWriter
from pypdf.generic import ArrayObject, ContentStream, NameObject

DEFAULT_INPUT_PDF = r"folder/file.pdf"
DEFAULT_OUTPUT_PDF = None
DEFAULT_DECRYPT_PASSWORD = ""


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
    return value if isinstance(value, dict) else {}


def _normalize_ocg_label(label: str | None) -> str | None:
    if label is None:
        return None
    text = label.strip()
    if text.startswith("/"):
        text = text[1:]
    return text if text else None


def _ocg_numeric_suffix(label: str | None) -> int | None:
    normalized = _normalize_ocg_label(label)
    if not normalized:
        return None
    if "-" in normalized:
        return None
    match = re.search(r"(\d+)$", normalized)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _resolve_ocg_name(ocg_obj: Any) -> str | None:
    if ocg_obj is None:
        return None
    if hasattr(ocg_obj, "get_object"):
        try:
            ocg_obj = ocg_obj.get_object()
        except Exception:
            return None
    if isinstance(ocg_obj, dict):
        if "/Name" in ocg_obj:
            return _name_to_key(ocg_obj.get("/Name"))
        if "/OCGs" in ocg_obj:
            names: list[str] = []
            for item in ocg_obj.get("/OCGs", []):
                entry = item
                if hasattr(entry, "get_object"):
                    try:
                        entry = entry.get_object()
                    except Exception:
                        continue
                if isinstance(entry, dict) and "/Name" in entry:
                    name = _name_to_key(entry.get("/Name"))
                    if name:
                        names.append(name)
            if names:
                return ", ".join(names)
    return None


def _resolve_props(props: Any, props_dict: dict[Any, Any]) -> tuple[str | None, str | None]:
    props_obj = props
    if hasattr(props_obj, "get_object"):
        try:
            props_obj = props_obj.get_object()
        except Exception:
            return None, None
    if isinstance(props_obj, dict):
        return None, _resolve_ocg_name(props_obj)
    props_key = _name_to_key(props_obj)
    if not props_key or not props_dict:
        return props_key, None
    ocg_obj = None
    if props_obj in props_dict:
        ocg_obj = props_dict.get(props_obj)
    else:
        try:
            ocg_obj = props_dict.get(NameObject(props_key))
        except Exception:
            ocg_obj = None
    return props_key, _resolve_ocg_name(ocg_obj)


def _remove_ocg_xobjects(resources: dict[Any, Any]) -> tuple[set[str], int]:
    ocg_names: set[str] = set()
    xobjects = resources.get("/XObject")
    if not xobjects:
        return ocg_names, 0
    xobjects_obj = _resolve_dict(xobjects)
    if not xobjects_obj:
        return ocg_names, 0

    for name, ref in list(xobjects_obj.items()):
        xobj = ref
        if hasattr(xobj, "get_object"):
            try:
                xobj = xobj.get_object()
            except Exception:
                continue
        if not isinstance(xobj, dict):
            continue
        subtype = _name_to_key(xobj.get("/Subtype"))
        if subtype == "/Form" and "/OC" in xobj:
            key = _name_to_key(name)
            if key:
                ocg_names.add(key)
            try:
                del xobjects_obj[name]
            except Exception:
                pass

    if not xobjects_obj:
        try:
            resources.pop("/XObject")
        except Exception:
            pass

    return ocg_names, len(ocg_names)


def _is_oc_tag(operands: list[Any]) -> bool:
    if not operands:
        return False
    tag = _name_to_key(operands[0])
    return tag == "/OC"


def _strip_content(
    content_obj: Any,
    reader: PdfReader,
    ocg_xobject_names: set[str],
    props_dict: dict[Any, Any],
) -> tuple[ContentStream, int, int, set[str]]:
    content_stream = ContentStream(content_obj, reader)
    new_ops: list[tuple[list[Any], bytes]] = []

    ocg_entries: list[dict[str, Any]] = []
    for operands, operator in content_stream.operations:
        if operator == b"BDC" and _is_oc_tag(operands):
            props = operands[1] if len(operands) > 1 else None
            props_key, ocg_name = _resolve_props(props, props_dict)
            num = _ocg_numeric_suffix(props_key)
            if num is None:
                num = _ocg_numeric_suffix(ocg_name)
            if num is not None:
                ocg_entries.append(
                    {
                        "num": num,
                        "props_key": props_key,
                        "ocg_name": ocg_name,
                        "primary": ocg_name or props_key,
                    }
                )

    remove_props: set[str] = set()
    if ocg_entries:
        min_num = min(entry["num"] for entry in ocg_entries)
        for entry in ocg_entries:
            if entry["num"] == min_num:
                if entry["props_key"]:
                    remove_props.add(entry["props_key"])

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
            props_key, ocg_name = _resolve_props(props, props_dict)
            norm = _normalize_ocg_label(props_key) if props_key else None
            if props_key in remove_props or (norm and norm in remove_props_norm):
                removed_blocks += 1
                remove_depth = 1
            if remove_depth > 0:
                continue

        if operator == b"Do" and operands:
            name = _name_to_key(operands[0])
            if name and name in ocg_xobject_names:
                do_removed += 1
                continue

        new_ops.append((operands, operator))

    content_stream.operations = new_ops
    content_stream.get_data()
    return content_stream, removed_blocks, do_removed, remove_props


def _default_output_path(input_path: str) -> str:
    root, ext = os.path.splitext(input_path)
    return f"{root}_cleaned{ext or '.pdf'}"


def _strip_js_from_action(action: Any) -> bool:
    action_dict = _resolve_dict(action)
    if not action_dict:
        return False
    if _name_to_key(action_dict.get("/S")) == "/JavaScript":
        return True
    if "/Next" in action_dict:
        next_obj = action_dict.get("/Next")
        if isinstance(next_obj, list):
            next_list = list(next_obj)
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
            action_dict[NameObject("/Next")] = next_list
    return False


def _remove_js_from_annotations(page) -> int:
    removed = 0
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
    for annot_ref in annots_obj:
        annot = annot_ref
        if hasattr(annot, "get_object"):
            try:
                annot = annot.get_object()
            except Exception:
                continue
        if not isinstance(annot, dict):
            continue
        action = annot.get("/A")
        if action and _strip_js_from_action(action):
            try:
                del annot["/A"]
            except Exception:
                pass
            removed += 1
        aa = annot.get("/AA")
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
                        del annot["/AA"]
                    except Exception:
                        pass
    return removed


def _remove_widget_annotations(page) -> int:
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
    kept = []
    removed = 0
    for annot_ref in annots_obj:
        annot = annot_ref
        if hasattr(annot, "get_object"):
            try:
                annot = annot.get_object()
            except Exception:
                kept.append(annot_ref)
                continue
        subtype = annot.get("/Subtype") if isinstance(annot, dict) else None
        if str(subtype) == "/Widget":
            removed += 1
            continue
        kept.append(annot_ref)
    if kept:
        page[NameObject("/Annots")] = ArrayObject(kept)
    else:
        try:
            del page["/Annots"]
        except Exception:
            pass
    return removed


def _remove_doc_level_js(writer: PdfWriter) -> bool:
    # Run after cloning so we can safely mutate writer's root object.
    root = writer._root_object
    if not root:
        return False
    removed = False
    names = root.get("/Names")
    if names:
        names_dict = _resolve_dict(names)
        if names_dict and "/JavaScript" in names_dict:
            try:
                del names_dict["/JavaScript"]
                removed = True
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
        removed = True
    aa = root.get("/AA")
    if aa:
        aa_dict = _resolve_dict(aa)
        if aa_dict:
            for key in list(aa_dict.keys()):
                if _strip_js_from_action(aa_dict.get(key)):
                    try:
                        del aa_dict[key]
                    except Exception:
                        pass
                    removed = True
            if not aa_dict:
                try:
                    del root["/AA"]
                except Exception:
                    pass
    return removed


def main() -> int:
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
        output_path = sys.argv[2] if len(sys.argv) > 2 else _default_output_path(pdf_path)
    else:
        pdf_path = DEFAULT_INPUT_PDF
        output_path = DEFAULT_OUTPUT_PDF or _default_output_path(pdf_path)

    if not pdf_path:
        raise ValueError("Set DEFAULT_INPUT_PDF or pass a PDF path on the command line.")

    reader = PdfReader(pdf_path)
    writer = PdfWriter()

    total_ocg_blocks = 0
    total_xforms = 0
    total_do_removed = 0
    total_js_annots = 0
    total_widgets_removed = 0
    warnings: list[str] = []
    errors: list[str] = []
    page_notes: list[str] = []

    decrypt_status = "not_encrypted"
    if reader.is_encrypted:
        try:
            result = reader.decrypt(DEFAULT_DECRYPT_PASSWORD)
        except Exception as exc:
            raise RuntimeError(f"Decrypt failed: {type(exc).__name__}: {exc}") from exc
        if result == 0:
            raise RuntimeError("Decrypt failed: invalid or missing password.")
        decrypt_status = f"decrypted (result={result})"
        warnings.append("Input was encrypted; permissions removed by decrypt before processing.")

    def process_page(page, page_num: int) -> None:
        nonlocal total_ocg_blocks, total_xforms, total_do_removed, total_js_annots, total_widgets_removed

        def record_error(stage: str, exc: Exception) -> None:
            errors.append(f"Page {page_num} [{stage}]: {type(exc).__name__}: {exc}")

        resources = {}
        props_dict = {}
        ocg_xobject_names: set[str] = set()

        try:
            resources = _resolve_dict(page.get("/Resources"))
            props_dict = _resolve_dict(resources.get("/Properties"))
        except Exception as exc:
            record_error("resources", exc)

        try:
            ocg_xobject_names, xforms_removed = _remove_ocg_xobjects(resources)
            total_xforms += xforms_removed
        except Exception as exc:
            record_error("xobject", exc)

        try:
            total_js_annots += _remove_js_from_annotations(page)
        except Exception as exc:
            record_error("js_annots", exc)

        try:
            total_widgets_removed += _remove_widget_annotations(page)
        except Exception as exc:
            record_error("widgets", exc)

        try:
            content_obj = page.get("/Contents")
            if content_obj:
                content_stream, removed_blocks, do_removed, removed_props = _strip_content(
                    content_obj, reader, ocg_xobject_names, props_dict
                )
                total_ocg_blocks += removed_blocks
                total_do_removed += do_removed
                page[NameObject("/Contents")] = writer._add_object(content_stream)
                if removed_props:
                    removed_list = ", ".join(sorted(removed_props))
                    page_notes.append(f"Page {page_num}: removed OCG blocks {removed_list}")
            else:
                warnings.append(f"Page {page_num}: missing /Contents")
        except Exception as exc:
            record_error("content", exc)

    total_pages = len(reader.pages)
    processed_pages = {"value": 0}

    def process_page_with_progress(page) -> None:
        processed_pages["value"] += 1
        page_num = processed_pages["value"]
        print(f"Processing page {page_num}/{total_pages}")
        process_page(page, page_num)

    writer.clone_document_from_reader(reader, after_page_append=process_page_with_progress)
    doc_js_removed = _remove_doc_level_js(writer)

    with open(output_path, "wb") as handle:
        writer.write(handle)

    if page_notes:
        print("\nPer-page notes")
        print("-" * 40)
        for note in page_notes:
            print(f"- {note}")
    if warnings:
        print("\nWarnings")
        print("-" * 40)
        for warning in warnings:
            print(f"- {warning}")
    if errors:
        print("\nErrors")
        print("-" * 40)
        for error in errors:
            print(f"- {error}")
    print("\nSummary")
    print("-" * 40)
    print(f"Pages processed: {processed_pages['value']}")
    print(f"OCG blocks removed: {total_ocg_blocks}")
    print(f"XForm /OC objects removed: {total_xforms}")
    print(f"Do ops stripped: {total_do_removed}")
    print(f"JS annotations removed: {total_js_annots}")
    print(f"Widget annotations removed: {total_widgets_removed}")
    print(f"Doc-level JS removed: {doc_js_removed}")
    print(f"Decrypt status: {decrypt_status}")
    print(f"Warnings: {len(warnings)}")
    print(f"Errors: {len(errors)}")

    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
