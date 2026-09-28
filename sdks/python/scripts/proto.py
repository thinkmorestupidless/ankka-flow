"""Copy protocol/ into proto/ (committed) and generate src/ankka_flow/_proto/ (ignored).

The copy is what CI diffs against protocol/ (FR-027); the generated code is rebuilt from the copy,
so an SDK checkout builds without the rest of the repository.
"""

from __future__ import annotations

import pathlib
import re
import shutil
import sys

HERE = pathlib.Path(__file__).resolve().parent.parent
PROTOCOL = HERE.parent.parent / "protocol"
PROTO = HERE / "proto"
OUT = HERE / "src" / "ankka_flow" / "_proto"


def copy() -> None:
    if not PROTOCOL.is_dir():
        print(f"no {PROTOCOL}; using the committed copy in {PROTO}")
        return
    if PROTO.exists():
        shutil.rmtree(PROTO)
    PROTO.mkdir()
    shutil.copytree(PROTOCOL / "src" / "main" / "protobuf", PROTO / "src" / "main" / "protobuf")
    shutil.copytree(PROTOCOL / "fixtures", PROTO / "fixtures")
    for name in ("DESCRIPTOR.md", "README.md"):
        shutil.copy2(PROTOCOL / name, PROTO / name)


def generate() -> None:
    from grpc_tools import protoc  # type: ignore[import-untyped]

    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    src = PROTO / "src" / "main" / "protobuf"
    files = sorted(str(p) for p in src.rglob("*.proto"))
    include = pathlib.Path(protoc.__file__).parent / "_proto"
    rc = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{src}",
            f"-I{include}",
            f"--python_out={OUT}",
            f"--pyi_out={OUT}",
            f"--grpc_python_out={OUT}",
            *files,
        ]
    )
    if rc != 0:
        sys.exit(rc)
    pattern = re.compile(r"^from ankka\.flow\.v1 import", re.MULTILINE)
    for path in OUT.rglob("*.py*"):
        text = path.read_text()
        new = pattern.sub("from ankka_flow._proto.ankka.flow.v1 import", text)
        if new != text:
            path.write_text(new)
    for d in [OUT, *[p for p in OUT.rglob("*") if p.is_dir()]]:
        (d / "__init__.py").touch()


if __name__ == "__main__":
    copy()
    generate()
    print(f"protocol copied to {PROTO} and generated into {OUT}")
