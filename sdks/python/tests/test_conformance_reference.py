"""The Python conformance reference behaves as the contract says, through the harness."""

import pytest

from ankka_flow._conformance import reference_streamlet
from ankka_flow.testkit import Harness


def run(key: str, config: dict[str, object] | None = None, headers: list[tuple[str, bytes]] | None = None) -> Harness:
    h = Harness(reference_streamlet(), config=config or {})
    h.inlet("in").put(key=key.encode(), value=b"{}", headers=headers or [])
    h.run()
    return h


def test_echo_and_fan() -> None:
    assert len(run("echo").outlet("out").records) == 1
    h = run("fan")
    assert len(h.outlet("out").records) == 1 and len(h.outlet("other").records) == 1


def test_skip_and_fail() -> None:
    assert run("skip").outlet("out").records == []
    assert run("fail").failures


def test_multiply_uses_the_factor() -> None:
    assert len(run("multiply", {"factor": 4}).outlet("out").records) == 4


def test_unkeyed_and_header_echo() -> None:
    assert run("unkeyed").outlet("out").records[0].key is None
    h = run("header-echo", headers=[("a", b"1"), ("b", b"2")])
    assert [k for k, _ in h.outlet("out").records[0].headers] == ["b", "a"]


def test_rogue_outlet_is_refused() -> None:
    assert run("rogue-outlet").failures


@pytest.mark.parametrize("key", ["echo", "unknown"])
def test_default_is_echo(key: str) -> None:
    assert len(run(key).outlet("out").records) == 1
