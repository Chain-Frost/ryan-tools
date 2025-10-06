from __future__ import annotations

import sys


def main() -> None:
    code = sys.stdin.read()
    exec(compile(code, "<stdin>", "exec"), {})


if __name__ == "__main__":
    main()
