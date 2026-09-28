#!/usr/bin/env python3
"""Checks every relative Markdown link in docs/ and README.md points at a file that exists."""

import re
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
files = [root / "README.md", *sorted((root / "docs").rglob("*.md"))]
link = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
broken = []
for f in files:
    text = re.sub(r"```.*?```", "", f.read_text(encoding="utf-8"), flags=re.S)
    for target in link.findall(text):
        if re.match(r"^[a-z]+:", target) or target.startswith("#"):
            continue
        path = (f.parent / target.split("#", 1)[0]).resolve()
        if not path.exists():
            broken.append(f"{f.relative_to(root)}: {target}")
print("\n".join(broken) if broken else f"{len(files)} files, every relative link resolves")
sys.exit(1 if broken else 0)
