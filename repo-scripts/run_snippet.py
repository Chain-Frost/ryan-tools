from __future__ import annotations

import sys


def main() -> None:
    code = sys.stdin.read()
    exec(compile(code, "<stdin>", "exec"), {})  # noqa: S102 - executing stdin is this helper's sole purpose


if __name__ == "__main__":
    main()
