"""`uv run conformance`: serve the reference streamlet and run the sidecar's conformance suite against it.

The reference is the `conformance` declaration of `proto/fixtures/declarations/conformance.md`,
behaving by each record's key as the protocol's conformance contract says. The suite lives in the
ankka-flow repository (`sidecar/src/test/.../ConformanceSuite.scala`) and needs sbt and a JDK.

Environment:
  FLOW_PROCESS_PORT            port to serve on (default 9010)
  ANKKA_FLOW_CONFORMANCE_ONLY  run only the cases whose names start with this
  ANKKA_FLOW_BREAK             break one SDK rule on purpose (`ack-first`, `keyless-empty-key`)
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Iterable
from pathlib import Path

from ankka_flow import Batch, Emit, IntegerParameter, JsonInlet, JsonOutlet, Record, StringParameter, Streamlet, serve


class Conformance(Streamlet):
    """The reference streamlet every SDK ships for the conformance suite."""

    name = "conformance"
    description = "The conformance reference streamlet."
    inlet = JsonInlet("in", schema_name="conformance.v1")
    side = JsonInlet("side", schema_name="conformance-side.v1")
    out = JsonOutlet("out", schema_name="conformance.v1")
    other = JsonOutlet("other", schema_name="conformance-other.v1")
    mode = StringParameter("mode", default="echo", description="Unused by the suite; proves a string parameter arrives.")
    factor = IntegerParameter("factor", default=1, description="How many times the multiply key emits.")

    def process(self, batch: Batch) -> Iterable[Emit]:
        for record in batch:
            key = (record.key or b"").decode()
            if key == "echo":
                yield self.out.emit(record)
            elif key == "fan":
                yield self.out.emit(record)
                yield self.other.emit(record)
            elif key == "skip":
                continue
            elif key == "fail":
                raise RuntimeError("the record said fail")
            elif key == "late":
                time.sleep(0.3)
                yield self.out.emit(record)
            elif key == "rogue-outlet":
                yield Emit("nope", record)
            elif key == "multiply":
                for i in range(1, self.config[self.factor] + 1):
                    yield self.out.emit(record, headers=[*record.headers, ("n", str(i).encode())])
            elif key == "unkeyed":
                yield self.out.emit(record, key=None)
            elif key == "header-echo":
                yield self.out.emit(record, headers=list(reversed(record.headers)))
            else:
                yield self.out.emit(record)


def reference_streamlet() -> Conformance:
    return Conformance()


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "build.sbt").exists() and (parent / "protocol").is_dir():
            return parent
    raise SystemExit("conformance: run from inside the ankka-flow repository (the suite is its sbt project)")


def main() -> int:
    port = int(os.environ.get("FLOW_PROCESS_PORT", "9010"))
    server = serve(reference_streamlet(), port=port, block=False)
    try:
        cmd = ["sbt", "-batch", f"-Dflow.conformance.target=127.0.0.1:{port}", "-Dflow.cluster.tests=off"]
        only = os.environ.get("ANKKA_FLOW_CONFORMANCE_ONLY")
        if only:
            cmd.append(f"-Dflow.conformance.only={only}")
        cmd.append("sidecar/testOnly *ConformanceSuite")
        print(f"conformance: serving the Python reference on 127.0.0.1:{port}; running {' '.join(cmd)}", flush=True)
        return subprocess.call(cmd, cwd=_repo_root())
    finally:
        server.stop()


if __name__ == "__main__":
    sys.exit(main())
