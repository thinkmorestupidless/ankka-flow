"""`uv run descriptor`: write flow/descriptor.json from the project's streamlet.

The streamlet is `module:attr`, from $FLOW_STREAMLET or `[tool.ankka-flow] streamlet` in
pyproject.toml. `--check` exits 1 when the file on disk differs.
"""

from __future__ import annotations

import argparse
import importlib
import os
import pathlib
import sys
import tomllib

from .descriptor import spec_for, validate, write
from .streamlet import Streamlet


def _reference(root: pathlib.Path) -> str:
    ref = os.environ.get("FLOW_STREAMLET")
    if ref:
        return ref
    pyproject = root / "pyproject.toml"
    if pyproject.exists():
        table = tomllib.loads(pyproject.read_text()).get("tool", {}).get("ankka-flow", {})
        if isinstance(table, dict) and isinstance(table.get("streamlet"), str):
            return str(table["streamlet"])
    raise SystemExit(
        "no streamlet: set FLOW_STREAMLET=module:Class or [tool.ankka-flow] streamlet in pyproject.toml"
    )


def load(ref: str) -> type[Streamlet]:
    module, _, attr = ref.partition(":")
    if not attr:
        raise SystemExit(f"'{ref}' is not module:attr")
    obj = getattr(importlib.import_module(module), attr)
    cls = obj if isinstance(obj, type) else type(obj)
    if not issubclass(cls, Streamlet):
        raise SystemExit(f"{ref} is not a Streamlet")
    return cls


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="descriptor", description=__doc__)
    parser.add_argument("--out", default="flow/descriptor.json")
    parser.add_argument("--check", action="store_true", help="exit 1 if the file differs")
    args = parser.parse_args(argv)
    root = pathlib.Path.cwd()
    if (root / "src").is_dir():
        sys.path.insert(0, str(root / "src"))
    spec = spec_for(load(_reference(root)))
    problems = validate(spec)
    if problems:
        for p in problems:
            print(f"descriptor: {p}", file=sys.stderr)
        return 1
    text = write(spec)
    out = pathlib.Path(args.out)
    if args.check:
        if not out.exists() or out.read_text(encoding="utf-8") != text:
            print(f"{out} is out of date; run `uv run descriptor`", file=sys.stderr)
            return 1
        print(f"{out} is up to date")
        return 0
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
