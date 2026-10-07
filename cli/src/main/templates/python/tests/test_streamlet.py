"""The streamlet through the SDK's harness: no Kafka, no sidecar."""

import json

import pytest

from ankka_flow.testkit import Harness

from {{module}}.streamlet import {{class}}


def test_adds_the_greeting_and_keeps_the_key_and_headers() -> None:
    h = Harness({{class}}())
    h.inlet("in").put(key=b"k-1", value=b'{"id": 1}', headers=[("ce-id", b"e-1")])
    h.run()
    [record] = h.outlet("out").records
    assert json.loads(record.value) == {"id": 1, "greeting": "hello, ankka-flow"}
    assert record.key == b"k-1"
    assert record.headers == [("ce-id", b"e-1")]


def test_the_greeting_is_the_parameters_deploy_time_value() -> None:
    h = Harness({{class}}(), config={"greeting": "hej"})
    h.inlet("in").put(value=b'{"id": 1}')
    h.run()
    assert json.loads(h.outlet("out").records[0].value)["greeting"] == "hej"


@pytest.mark.parametrize("value", [b"not json", b"[1, 2]"])
def test_a_value_that_is_not_a_json_object_fails_the_batch(value: bytes) -> None:
    h = Harness({{class}}())
    h.inlet("in").put(value=value)
    h.run()
    assert len(h.failures) == 1
    assert h.outlet("out").records == []
