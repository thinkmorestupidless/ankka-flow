"""Keeps README.md's included samples fresh, the way `docs sync` keeps the pages'.

The README is not a documentation page, so the docs tool does not read it, but it shows the same
tested code the pages show, between the same `<!-- include: path#name -->` markers. This runs the
tool's own include processing over it:

    uv run --project tools/docs python tools/docs/readme_includes.py          # rewrite
    uv run --project tools/docs python tools/docs/readme_includes.py --check  # fail when stale
"""

from __future__ import annotations

import sys
from pathlib import Path

from ankka_docs import snippets

ROOT = Path(__file__).resolve().parent.parent.parent
README = ROOT / "README.md"


def main() -> int:
    check = "--check" in sys.argv[1:]
    snippets.Problem.prefix = ""
    before = README.read_text()
    after, problems = snippets.process(ROOT, "README.md", before)
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        return 1
    if after == before:
        print("README.md: included samples are current")
        return 0
    if check:
        print("README.md: an included sample has drifted from its source; run `just readme-sync`", file=sys.stderr)
        return 1
    README.write_text(after)
    print("README.md: included samples refreshed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
