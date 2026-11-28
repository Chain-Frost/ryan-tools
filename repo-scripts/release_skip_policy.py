"""Select the explicit release opt-out for a verified pull-request merge."""

import json
import re
import sys
from typing import cast

_SKIP_MARKER = re.compile(r"\[skip release\]", re.IGNORECASE)


def should_skip_release(labels: list[str], commit_message: str) -> bool:
    """Match the exact PR label or the immutable merge commit keyword."""
    return "skip-release" in {label.casefold() for label in labels} or bool(_SKIP_MARKER.search(commit_message))


def main() -> int:
    """Return skip/publish on stdout, failing closed for invalid input."""
    record: object = json.load(sys.stdin)
    if not isinstance(record, dict):
        message = "Expected a JSON object."
        raise TypeError(message)
    fields = cast("dict[str, object]", record)
    labels = fields.get("labels")
    message = fields.get("commit_message")
    if not isinstance(labels, list):
        error_message = "labels must be list[str]."
        raise TypeError(error_message)
    values = cast("list[object]", labels)
    if any(not isinstance(label, str) for label in values):
        error_message = "labels must be list[str]."
        raise TypeError(error_message)
    if not isinstance(message, str):
        error_message = "commit_message must be a string."
        raise TypeError(error_message)
    print("skip" if should_skip_release(cast("list[str]", values), message) else "publish")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
