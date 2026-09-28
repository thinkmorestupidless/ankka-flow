"""JSON over bytes. The SDK decodes nothing on its own; these are for the streamlet's author."""

from __future__ import annotations

import json as _json
from typing import Any


def loads(data: bytes) -> Any:
    return _json.loads(data)


def dumps(obj: Any) -> bytes:
    return _json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
