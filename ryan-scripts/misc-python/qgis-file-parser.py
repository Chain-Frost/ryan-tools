# ryan-scripts\misc-python\qgis-file-parser.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
import re
from typing import Literal
from urllib.parse import ParseResult, unquote, urlparse
from urllib.request import url2pathname
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile

QgisSourceStatus = Literal["exists", "missing", "skipped"]
QgisPathKind = Literal["abs", "rel"]

# ---------------------------------------------------------------------------
# Hard-coded defaults
# ---------------------------------------------------------------------------
#
# These values are used when the script is run without matching command-line
# arguments. CLI arguments still override them.
#
# Set HARD_CODED_PROJECT_FILE to None if you want the script to prompt for a
# project file when no path is supplied on the command line.
HARD_CODED_PROJECT_FILE: Path | None = Path(
    r"P:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO"
    r"\4 ENGINEERING\11 HYDROLOGY\Report\RP20181.498_Figures_LG.qgs"
)
HARD_CODED_MISSING_ONLY: bool = False
HARD_CODED_SUMMARY_ONLY: bool = False
HARD_CODED_UNIQUE_FILES: bool = False
HARD_CODED_FAIL_ON_MISSING: bool = False

_DRIVE_PATH_PATTERN: re.Pattern[str] = re.compile(pattern=r"^[A-Za-z]:[\\/]")
_KEY_VALUE_PATH_PATTERN: re.Pattern[str] = re.compile(
    pattern=r"(?:^|\s)(?:dbname|filename|file)=(?P<quote>['\"]?)(?P<value>.*?)(?P=quote)(?:\s|$)",
    flags=re.IGNORECASE,
)
_REMOTE_URL_PATTERN: re.Pattern[str] = re.compile(
    pattern=r"(?:^|\s)url=(?P<quote>['\"]?)(?P<url>https?://.*?)(?P=quote)(?:\s|$)", flags=re.IGNORECASE
)


@dataclass(frozen=True, slots=True)
class QgisLayerSource:
    layer_name: str
    provider: str
    raw_datasource: str
    status: QgisSourceStatus
    reason: str
    datasource_path_text: str | None
    path_kind: QgisPathKind | None
    file_path: Path | None


@dataclass(frozen=True, slots=True)
class QgisProjectReport:
    project_file: Path
    project_home: Path
    layers: tuple[QgisLayerSource, ...]

    @property
    def existing_count(self) -> int:
        return sum(1 for layer in self.layers if layer.status == "exists")

    @property
    def missing_count(self) -> int:
        return sum(1 for layer in self.layers if layer.status == "missing")

    @property
    def skipped_count(self) -> int:
        return sum(1 for layer in self.layers if layer.status == "skipped")


def main() -> None:
    args = parse_args()
    project_file = args.project_file or prompt_for_project_file()

    report: QgisProjectReport = inspect_qgis_project(project_file=project_file)
    print(
        format_qgis_project_report(
            report=report,
            include_missing=not args.summary_only,
            include_existing=not args.missing_only and not args.summary_only,
            include_skipped=not args.missing_only and not args.summary_only,
            unique_files=args.unique_files,
        )
    )

    if args.fail_on_missing and report.missing_count:
        raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Open a QGIS .qgs or .qgz project and report whether its local datasource files exist.",
    )
    parser.add_argument(
        "project_file",
        nargs="?",
        type=Path,
        default=HARD_CODED_PROJECT_FILE,
        help="QGIS project file to inspect. Overrides HARD_CODED_PROJECT_FILE when supplied.",
    )
    parser.add_argument(
        "--missing-only",
        action=argparse.BooleanOptionalAction,
        default=HARD_CODED_MISSING_ONLY,
        help="Only list missing files after the summary. Use --no-missing-only to override a hard-coded True value.",
    )
    parser.add_argument(
        "--summary-only",
        action=argparse.BooleanOptionalAction,
        default=HARD_CODED_SUMMARY_ONLY,
        help="Only print the project summary counts. Use --no-summary-only to override a hard-coded True value.",
    )
    parser.add_argument(
        "--unique-files",
        action=argparse.BooleanOptionalAction,
        default=HARD_CODED_UNIQUE_FILES,
        help="List each resolved file once. Use --no-unique-files to override a hard-coded True value.",
    )
    parser.add_argument(
        "--fail-on-missing",
        action=argparse.BooleanOptionalAction,
        default=HARD_CODED_FAIL_ON_MISSING,
        help="Return exit code 1 when files are missing. Use --no-fail-on-missing to override a hard-coded True value.",
    )
    return parser.parse_args()


def prompt_for_project_file() -> Path:
    raw_path = input("QGIS project file (.qgs or .qgz): ").strip().strip('"')
    if not raw_path:
        raise SystemExit("No project file supplied.")
    return Path(raw_path)


def inspect_qgis_project(*, project_file: Path) -> QgisProjectReport:
    project_file = project_file.expanduser()
    if not project_file.exists():
        raise FileNotFoundError(f"QGIS project does not exist: {project_file}")

    root: ElementTree.Element = ElementTree.fromstring(read_project_xml(project_file=project_file))
    project_home: Path = get_project_home(project_file=project_file, root=root)

    layers: list[QgisLayerSource] = []
    project_layers: ElementTree.Element | None = root.find("projectlayers")
    if project_layers is None:
        return QgisProjectReport(project_file=project_file, project_home=project_home, layers=())

    for map_layer in project_layers.findall("maplayer"):
        layer_name: str = direct_child_text(element=map_layer, tag="layername") or "<unnamed layer>"
        provider: str = direct_child_text(element=map_layer, tag="provider") or ""
        datasource: str = direct_child_text(element=map_layer, tag="datasource") or ""
        layers.append(
            inspect_datasource(
                layer_name=layer_name,
                provider=provider,
                datasource=datasource,
                project_home=project_home,
            )
        )

    return QgisProjectReport(project_file=project_file, project_home=project_home, layers=tuple(layers))


def read_project_xml(*, project_file: Path) -> bytes:
    suffix: str = project_file.suffix.lower()
    if suffix == ".qgs":
        return project_file.read_bytes()
    if suffix != ".qgz":
        raise ValueError(f"Expected a .qgs or .qgz file: {project_file}")

    try:
        with ZipFile(project_file) as archive:
            qgs_names: list[str] = [name for name in archive.namelist() if name.lower().endswith(".qgs")]
            if not qgs_names:
                raise ValueError(f"No .qgs project XML found inside: {project_file}")
            with archive.open(qgs_names[0]) as qgs_file:
                return qgs_file.read()
    except BadZipFile as exc:
        raise ValueError(f"QGZ file is not a readable zip archive: {project_file}") from exc


def get_project_home(*, project_file: Path, root: ElementTree.Element) -> Path:
    project_dir: Path = absolute_normalized(path=project_file.parent)
    home_element: ElementTree.Element | None = root.find("homePath")
    home_path_text: str = ""
    if home_element is not None:
        home_path_text = (home_element.get("path") or "").strip()

    if not home_path_text:
        return project_dir

    home_path = Path(normalize_slashes(path_text=home_path_text))
    if is_absolute_path(path_text=home_path_text) or home_path.is_absolute():
        return absolute_normalized(path=home_path)
    return absolute_normalized(path=project_dir / home_path)


def direct_child_text(*, element: ElementTree.Element, tag: str) -> str | None:
    for child in element:
        if child.tag == tag:
            return child.text.strip() if child.text else ""
    return None


def inspect_datasource(
    *,
    layer_name: str,
    provider: str,
    datasource: str,
    project_home: Path,
) -> QgisLayerSource:
    file_text, skip_reason = extract_file_path_text(datasource=datasource, provider=provider)
    if file_text is None:
        return QgisLayerSource(
            layer_name=layer_name,
            provider=provider,
            raw_datasource=datasource,
            status="skipped",
            reason=skip_reason,
            datasource_path_text=None,
            path_kind=None,
            file_path=None,
        )

    path_kind = get_path_kind(path_text=file_text)
    file_path = resolve_datasource_path(path_text=file_text, project_home=project_home)
    if file_path.exists():
        return QgisLayerSource(
            layer_name=layer_name,
            provider=provider,
            raw_datasource=datasource,
            status="exists",
            reason="file exists",
            datasource_path_text=file_text,
            path_kind=path_kind,
            file_path=file_path,
        )

    return QgisLayerSource(
        layer_name=layer_name,
        provider=provider,
        raw_datasource=datasource,
        status="missing",
        reason="file not found",
        datasource_path_text=file_text,
        path_kind=path_kind,
        file_path=file_path,
    )


def extract_file_path_text(*, datasource: str, provider: str) -> tuple[str | None, str]:
    source: str = datasource.strip()
    if not source:
        return None, "empty datasource"

    if _REMOTE_URL_PATTERN.search(source) or source.lower().startswith(("http://", "https://")):
        return None, "remote URL"

    key_value_path = extract_key_value_path(source=source)
    if key_value_path:
        return key_value_path, "file datasource"

    provider_lower: str = provider.lower()
    if provider_lower in {"arcgismapserver", "arcgisfeatureserver", "wms", "wfs", "postgres", "mssql", "oracle"}:
        return None, f"{provider or 'non-file'} datasource"

    path_text: str = source.split("|", maxsplit=1)[0].strip()
    if not path_text:
        return None, "empty datasource path"

    if path_text.lower().startswith("file:"):
        return file_url_to_path_text(url_text=path_text), "file URL"

    if "://" in path_text:
        return None, "non-file URI"

    if not looks_like_path(path_text=path_text):
        return None, "not a local file path"

    return path_text, "file datasource"


def extract_key_value_path(*, source: str) -> str | None:
    match: re.Match[str] | None = _KEY_VALUE_PATH_PATTERN.search(source)
    if match is None:
        return None

    value = match.group("value").strip()
    if not value:
        return None
    if "://" in value and not value.lower().startswith("file:"):
        return None

    if value.lower().startswith("file:"):
        return file_url_to_path_text(url_text=value)

    return value if looks_like_path(path_text=value) else None


def file_url_to_path_text(*, url_text: str) -> str:
    parsed: ParseResult = urlparse(url_text)
    local_path: str = url2pathname(unquote(parsed.path))
    if parsed.netloc:
        return f"//{parsed.netloc}{local_path}"
    return local_path


def resolve_datasource_path(*, path_text: str, project_home: Path) -> Path:
    normalized_path_text: str = normalize_slashes(path_text=path_text.strip().strip("'\""))
    path = Path(normalized_path_text)
    if is_absolute_path(path_text=normalized_path_text) or path.is_absolute():
        return absolute_normalized(path=path)
    return absolute_normalized(path=project_home / path)


def absolute_normalized(*, path: Path) -> Path:
    return Path(os.path.abspath(path))


def normalize_slashes(*, path_text: str) -> str:
    if path_text.startswith("//"):
        return "\\\\" + path_text[2:].replace("/", "\\")
    return path_text.replace("/", "\\")


def looks_like_path(*, path_text: str) -> bool:
    if is_absolute_path(path_text=path_text):
        return True
    if path_text.startswith((".", "..")):
        return True
    if "\\" in path_text or "/" in path_text:
        return True
    return bool(Path(path_text).suffix)


def is_absolute_path(*, path_text: str) -> bool:
    return bool(_DRIVE_PATH_PATTERN.match(string=path_text)) or path_text.startswith(("\\\\", "//"))


def get_path_kind(*, path_text: str) -> QgisPathKind:
    normalized_path_text: str = normalize_slashes(path_text=path_text.strip().strip("'\""))
    path = Path(normalized_path_text)
    if is_absolute_path(path_text=normalized_path_text) or path.is_absolute():
        return "abs"
    return "rel"


def format_qgis_project_report(
    *,
    report: QgisProjectReport,
    include_missing: bool,
    include_existing: bool,
    include_skipped: bool,
    unique_files: bool,
) -> str:
    lines: list[str] = [
        f"QGIS project: {report.project_file}",
        f"Project home: {report.project_home}",
        (
            f"Layers checked: {len(report.layers)} | "
            f"existing: {report.existing_count} | "
            f"missing: {report.missing_count} | "
            f"skipped: {report.skipped_count}"
        ),
        "",
    ]

    layers: tuple[QgisLayerSource, ...] = unique_file_layers(layers=report.layers) if unique_files else report.layers

    for status, title in (("missing", "Missing files"), ("exists", "Existing files"), ("skipped", "Skipped sources")):
        if status == "missing" and not include_missing:
            continue
        if status == "exists" and not include_existing:
            continue
        if status == "skipped" and not include_skipped:
            continue

        section_layers: list[QgisLayerSource] = [layer for layer in layers if layer.status == status]
        lines.append(f"{title}: {len(section_layers)}")
        for layer in section_layers:
            lines.append(format_layer_source(layer=layer))
        lines.append("")

    return "\n".join(lines).rstrip()


def unique_file_layers(*, layers: tuple[QgisLayerSource, ...]) -> tuple[QgisLayerSource, ...]:
    unique_layers: list[QgisLayerSource] = []
    seen_files: set[str] = set()

    for layer in layers:
        if layer.file_path is None:
            unique_layers.append(layer)
            continue

        key: str = str(layer.file_path).casefold()
        if key in seen_files:
            continue
        seen_files.add(key)
        unique_layers.append(layer)

    return tuple(unique_layers)


def format_layer_source(*, layer: QgisLayerSource) -> str:
    status_text = layer.status.upper()
    provider_text: str = layer.provider or "unknown provider"
    if layer.file_path is not None:
        path_kind_text: str = (layer.path_kind or "unknown").upper()
        if layer.path_kind == "rel" and layer.datasource_path_text is not None:
            return (
                f"[{status_text}][{path_kind_text}] {layer.layer_name} ({provider_text}) -> "
                f"source: {layer.datasource_path_text} | resolved: {layer.file_path}"
            )
        return f"[{status_text}][{path_kind_text}] {layer.layer_name} ({provider_text}) -> {layer.file_path}"
    return f"[{status_text}] {layer.layer_name} ({provider_text}) -> {layer.reason}"


if __name__ == "__main__":
    main()
