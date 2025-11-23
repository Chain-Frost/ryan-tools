"""Tests for logging helper utilities."""

import logging


import pytest

# from pathlib import Path
# import sys
# TODO this appears deprecated - we use loguru now. we should find out what actually still uses normal logging so we can remove from maintenance.
# # sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ...ryan_library.functions.logging_helpers import ConditionalFormatter


@pytest.fixture()
def formatter() -> ConditionalFormatter:
    return ConditionalFormatter(
        detailed_fmt="DETAIL-%(levelname)s-%(message)s",
        simple_fmt="SIMPLE-%(message)s",
    )


def _make_record(message: str, simple: bool = False) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=42,
        msg=message,
        args=(),
        exc_info=None,
    )
    if simple:
        record.simple_format = True
    return record


def test_conditional_formatter_uses_detailed_format_by_default(
    formatter: ConditionalFormatter,
) -> None:
    record = _make_record("default formatting")

    rendered = formatter.format(record)

    assert rendered == "DETAIL-INFO-default formatting"


def test_conditional_formatter_uses_simple_format_when_flagged(
    formatter: ConditionalFormatter,
) -> None:
    record = _make_record("simple formatting", simple=True)

    rendered = formatter.format(record)

    assert rendered == "SIMPLE-simple formatting"


def test_conditional_formatter_retains_detailed_format_between_calls(
    formatter: ConditionalFormatter,
) -> None:
    simple_record = _make_record("first simple", simple=True)
    detailed_record = _make_record("second detailed")

    simple_rendered = formatter.format(simple_record)
    detailed_rendered = formatter.format(detailed_record)

    assert simple_rendered == "SIMPLE-first simple"
    assert detailed_rendered == "DETAIL-INFO-second detailed"
    assert formatter.detailed_fmt == "DETAIL-%(levelname)s-%(message)s"
