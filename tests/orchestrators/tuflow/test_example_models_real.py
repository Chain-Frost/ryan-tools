"""Replay the complete public example archive through the current consumer API.

BAT files are read as fixture text, never executed. This is dependency validation,
not hydraulic acceptance or an eager/deferred comparator.
"""

import csv
import hashlib
import json
import os
import sys
import zipfile
from collections import Counter
from dataclasses import asdict, replace
from pathlib import Path
from typing import cast

import pytest

from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun, parse_command_plan
from ryan_library.functions.tuflow.pytuflow_compatibility import require_validation_api
from ryan_library.orchestrators.tuflow.command_preflight import check_command_plan

DATA = Path(__file__).absolute().parents[2] / "test_data/tuflow/example_input_preflight"
REQUIREMENT = Path(__file__).absolute().parents[3] / "repo-scripts/pytuflow-requirement.txt"
ARCHIVE_SHA256 = "e374c8e072679fc6c4dc11e63b270caa1101b6aebb84d70060872cdd1b0aa7d6"
pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="Example BAT plans require native Windows paths")

# Temporary, pinned-PyTUFLOW 05e35476 findings. See upstream PR #60 and
# ryan-tools #181. Match the entire observed finding, never just a TCF name:
# a changed/additional issue or an unexpected recovery must fail this test.
_ONE_D_SCHEMA_GAP = "Unsupported GIS BC/SA/RF layer schema"
_EXPECTED_VALIDATOR_GAPS: dict[str, tuple[str, str, str]] = {
    "EG02_013.tcf": ("Read GIS BC", "1d_bc_EG14_001_P", _ONE_D_SCHEMA_GAP),
    "EG03_009.tcf": (
        "Read GIS RF Triangle",
        "2d_rf_EG03_009_R",
        "Cannot inspect GIS attributes: Missing or unexpanded BC Name in 2d_rf_EG03_009_R: ''",
    ),
    "EG04_004.tcf": ("Read GIS BC", "1d_bc_EG04_004_P", _ONE_D_SCHEMA_GAP),
    "EG04_005.tcf": ("Read GIS BC", "1d_bc_EG14_001_P", _ONE_D_SCHEMA_GAP),
    "EG04_006.tcf": ("Read GIS BC", "1d_bc_EG04_006_P", _ONE_D_SCHEMA_GAP),
    "EG14_001.tcf": ("Read GIS BC", "1d_bc_EG14_001_P", _ONE_D_SCHEMA_GAP),
    "EG14_002.tcf": ("Read GIS BC", "1d_bc_EG14_002_R", _ONE_D_SCHEMA_GAP),
    "EG14_003.tcf": ("Read GIS BC", "1d_bc_EG14_003_R", _ONE_D_SCHEMA_GAP),
}


@pytest.fixture(scope="module")
def example_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Verify every original member and extract once, outside the data submodule."""
    archive = DATA / "examples.zip"
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == ARCHIVE_SHA256
    manifest = cast(dict[str, object], json.loads((DATA / "manifest.json").read_text(encoding="utf-8")))
    entries = cast(list[dict[str, str | int]], manifest["files"])
    expected = {str(entry["path"]): entry for entry in entries}
    root = tmp_path_factory.mktemp("tuflow-examples")
    with zipfile.ZipFile(archive) as source:
        assert len(source.namelist()) == len(expected) == 1464
        assert set(source.namelist()) == set(expected)
        for member in source.infolist():
            # Resolve deliberately: archive extraction must remain inside its temporary root across links.
            target = (root / member.filename).resolve()
            assert target.is_relative_to(root.resolve()), member.filename
            content = source.read(member)
            entry = expected[member.filename]
            assert len(content) == entry["size"], member.filename
            assert hashlib.sha256(content).hexdigest() == entry["sha256"], member.filename
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
    return root


def _fixture_batch_plan(path: Path) -> CommandPlan:
    """Extract declared launches, bypassing only the aggregators' executable guard.

    The pinned Run_All/Run_GPKG fixtures contain a fixed IF/EXIT executable
    guard unsupported by the production planner. Do not generalise shell handling:
    assert its exact non-launch statements and preserve physical line numbers.
    """
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    if path.name in {"Run_All_Examples.bat", "Run_GPKG_Examples.bat"}:
        guard = {
            "setlocal",
            'if not exist "%tuflow_exe%" (',
            'echo error: "%tuflow_exe%" not found. update run_all_examples.bat with the correct path.',
            "exit /b 1",
            ")",
            "echo.",
            "echo all tuflow example runs completed.",
        }
        lines = ["" if line.strip().casefold() in guard else line for line in lines]
    return parse_command_plan(
        "\n".join(lines), source=path, initial_directory=path.parent, environment={}, syntax="batch"
    )


@pytest.fixture(scope="module")
def example_plan(example_root: Path) -> CommandPlan:
    """Derive unique model contexts from every simulation BAT, plus unlisted TCFs."""
    runs: dict[tuple[str, tuple[tuple[str, str], ...]], PlannedRun] = {}
    batches = sorted(example_root.rglob("*.bat"))
    assert len(batches) == 22
    for batch in batches:
        # This is an empty-GIS generation utility, not a simulation; no TCF is supplied.
        if batch.name == "Create_empties_run.bat":
            assert "Create_Empties" in batch.read_text(encoding="utf-8-sig")
            continue
        for run in _fixture_batch_plan(batch).runs:
            relative = Path(str(run.tcf)).relative_to(example_root).as_posix()
            key = (relative, tuple(sorted(run.flag_pairs)))
            runs.setdefault(key, run)
    listed = {key[0] for key in runs}
    for tcf in sorted(example_root.rglob("*.tcf")):
        relative = tcf.relative_to(example_root).as_posix()
        if relative not in listed:
            # Retain explicit no-flag coverage for controls absent from every batch.
            plan = parse_command_plan(
                f'"{tcf.name}"', source=tcf, initial_directory=tcf.parent, environment={}, syntax="commands"
            )
            runs[(relative, ())] = plan.runs[0]
    actual = Counter(key[0] for key in runs)
    with (DATA / "baseline.csv").open(newline="", encoding="utf-8-sig") as source:
        expected = {row["tcf"]: int(row["runs"]) for row in csv.DictReader(source)}
    assert actual == expected, f"BAT-derived coverage differs: actual={actual}, expected={expected}"
    assert len(actual) == 161
    assert sum(actual.values()) == 174
    ordered = tuple(replace(run, index=index) for index, run in enumerate(runs.values(), 1))
    return CommandPlan(batches[0], ordered, (), 0, "mixed", tuple(batches))


def test_example_batch_inventory(example_plan: CommandPlan) -> None:
    """No omitted model, extra context or duplicate aggregate launch goes unnoticed."""
    assert len(example_plan.runs) == 174


@pytest.mark.gdal
def test_all_example_models_public_validation(example_plan: CommandPlan, tmp_path: Path) -> None:
    """Check all models; permit only exact known upstream defects in the pinned validator."""
    diagnostic = require_validation_api().diagnostic
    expected_source = REQUIREMENT.read_text(encoding="utf-8").strip().split("@", 1)[1].strip()
    assert expected_source in diagnostic.source, diagnostic.message
    output = Path(os.environ.get("TUFLOW_EXAMPLE_REPORT_DIR", str(tmp_path))).absolute()
    output.mkdir(parents=True, exist_ok=True)
    groups: dict[Path, list[PlannedRun]] = {}
    for run in example_plan.runs:
        groups.setdefault(Path(str(run.tcf)), []).append(run)
    rows: list[dict[str, object]] = []
    observed_known: list[str] = []
    unexpected_failures: list[str] = []
    unexpectedly_passing: list[str] = []
    for tcf, runs in groups.items():
        plan = replace(example_plan, runs=tuple(runs))
        report = check_command_plan(plan)
        report.write_csv(output / f"{tcf.stem}-issues.csv")

        expected = _EXPECTED_VALIDATOR_GAPS.get(tcf.name)
        if expected is None:
            if not report.passed or report.completed_runs != len(runs):
                unexpected_failures.append(
                    f"{tcf.name}: {len(report.issues)} unexpected issue(s), "
                    f"{report.completed_runs}/{len(runs)} runs checked"
                )
        elif report.passed:
            unexpectedly_passing.append(tcf.name)
        elif len(runs) != 1 or report.completed_runs != 1 or len(report.issues) != 1:
            unexpected_failures.append(
                f"{tcf.name}: expected exactly one attributed unresolved finding, "
                f"got {len(report.issues)} finding(s), {report.completed_runs}/{len(runs)} runs checked"
            )
        else:
            issue = report.issues[0]
            finding = issue.finding
            actual = (finding.origin, finding.layer, finding.detail)
            if (
                issue.run.index != runs[0].index
                or finding.status != "unresolved"
                or finding.kind != "attribute"
                or finding.path is None
                or finding.path.name != f"{finding.layer}.shp"
                or actual != expected
            ):
                unexpected_failures.append(f"{tcf.name}: changed pinned-validator finding: {actual!r}")
            else:
                observed_known.append(tcf.name)

        rows.append(
            {
                "tcf": tcf.name,
                "runs": len(runs),
                "passed": report.passed,
                "completed_runs": report.completed_runs,
                "groups_checked": report.groups_checked,
                "checked_paths": len(report.checked_paths),
                "checked_path_inventory": sorted(report.checked_paths),
                "commands": plan.manifest(),
                "issues": [asdict(issue) for issue in report.issues],
            }
        )
    summary = {
        "consumer_revision": os.environ.get("GITHUB_SHA", "local working tree; record Git HEAD alongside this report"),
        "parser": asdict(diagnostic),
        "archive_sha256": ARCHIVE_SHA256,
        "tcfs": len(rows),
        "runs": sum(len(runs) for runs in groups.values()),
        "passed_tcfs": sum(row["passed"] is True for row in rows),
        "passed_runs": sum(len(groups[tcf]) for tcf, row in zip(groups, rows, strict=True) if row["passed"] is True),
        "results": rows,
        "expected_known_gaps": sorted(_EXPECTED_VALIDATOR_GAPS),
        "verified_known_gaps": sorted(observed_known),
        "unexpected_failures": unexpected_failures,
        "unexpectedly_passing": unexpectedly_passing,
    }
    (output / "results.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    assert not unexpected_failures, f"Unexpected or changed model findings: {unexpected_failures}; reports: {output}"
    assert not unexpectedly_passing, (
        f"Pinned PyTUFLOW gap unexpectedly fixed for {unexpectedly_passing}; "
        "remove its temporary allowance (ryan-tools #181)"
    )
    assert set(observed_known) == set(_EXPECTED_VALIDATOR_GAPS), (
        f"Expected exactly the eight recorded upstream defects; verified {observed_known}"
    )
