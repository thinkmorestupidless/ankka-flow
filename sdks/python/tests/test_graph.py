"""Graph deltas built by the SDK carry their element keys, and nothing lets an author choose one."""

from __future__ import annotations

import inspect
import json as stdjson
import pathlib
from collections.abc import Iterable
from typing import Any

import pytest

from ankka_flow import Batch, Emit, GraphDeltaOutlet, JsonInlet, JsonOutlet, Record, Streamlet, graph, json
from ankka_flow.descriptor import spec_for, write
from ankka_flow.testkit import Harness

KEYS = pathlib.Path(__file__).resolve().parent.parent / "proto" / "fixtures" / "graph-deltas" / "keys.json"
ROWS: list[dict[str, Any]] = stdjson.loads(KEYS.read_text(encoding="utf-8"))

DELTAS: list[dict[str, Any]] = stdjson.loads((KEYS.parent / "deltas.json").read_text(encoding="utf-8"))

OUT = GraphDeltaOutlet("deltas")


def build(delta: dict[str, Any], record: Record | None = None) -> Emit:
    """The fixture's delta, built the way a mapper would build it."""
    if delta["kind"] == "node":
        return OUT.node(
            record,
            id=delta["id"],
            version=delta["version"],
            labels=delta.get("labels", []),
            properties=delta.get("properties"),
        )
    if delta["kind"] == "edge":
        return OUT.edge(
            record,
            id=delta["id"],
            version=delta["version"],
            type=delta["type"],
            from_id=delta["from"],
            to_id=delta["to"],
            properties=delta.get("properties"),
        )
    if delta["element"] == "node":
        return OUT.tombstone_node(record, id=delta["id"], version=delta["version"])
    return OUT.tombstone_edge(
        record,
        id=delta["id"],
        version=delta["version"],
        type=delta["type"],
        from_id=delta["from"],
        to_id=delta["to"],
    )


# ── the shared fixture: the SDK and the sink agree on every key ─────────────────────────────────


def test_the_fixture_covers_each_kind_and_the_awkward_ids() -> None:
    assert len(ROWS) >= 8
    kinds = {(r["delta"]["kind"], r["delta"].get("element")) for r in ROWS}
    assert {("node", None), ("edge", None), ("tombstone", "node"), ("tombstone", "edge")} <= kinds
    assert any(not r["key"].isascii() for r in ROWS)


@pytest.mark.parametrize("row", ROWS, ids=[f"{i}:{r['key']}" for i, r in enumerate(ROWS)])
def test_a_delta_built_by_the_outlet_has_the_fixtures_key_and_value(row: dict[str, Any]) -> None:
    emit = build(row["delta"])
    assert emit.outlet == "deltas"
    assert emit.record.key == row["key"].encode("utf-8")
    assert json.loads(emit.record.value) == row["delta"]
    assert graph.read(emit.record).key == row["key"].encode("utf-8")


def test_the_fixture_of_deltas_covers_every_kind_of_property() -> None:
    assert len(DELTAS) >= 12
    kinds = {kind for r in DELTAS for kind in r["reads"].values()}
    assert kinds == {k for s in ("string", "integer", "float", "boolean") for k in (s, f"list:{s}")}


@pytest.mark.parametrize("row", DELTAS, ids=[f"{i}:{r['key']}" for i, r in enumerate(DELTAS)])
def test_a_delta_of_the_fixture_built_by_the_outlet_reads_back_as_the_fixtures(row: dict[str, Any]) -> None:
    emit = build(row["delta"])
    key = row["key"].encode("utf-8")
    assert emit.record.key == key
    # What the outlet wrote and what the fixture holds read as the same delta: the outlet always
    # writes labels and properties, and a whole-number float is the integer the sink stores.
    written = graph.read(emit.record)
    given = graph.read(Record(key=key, value=stdjson.dumps(row["delta"]).encode("utf-8")))
    assert written == given
    assert written.key == key
    assert set(written.properties) == set(row["reads"])


def test_a_node_and_an_edge_with_one_id_have_different_keys() -> None:
    assert graph.node_key("same-id") == b"node:same-id"
    assert graph.edge_key("same-id") == b"edge:same-id"
    node = OUT.node(None, id="same-id", version=1)
    edge = OUT.edge(None, id="same-id", version=1, type="LINKS", from_id="a", to_id="b")
    assert node.record.key != edge.record.key


def test_a_tombstone_has_the_key_of_the_element_it_marks() -> None:
    assert OUT.tombstone_node(None, id="cart:1", version=2).record.key == graph.node_key("cart:1")
    tomb = OUT.tombstone_edge(None, id="e:1", version=2, type="LINKS", from_id="a", to_id="b")
    assert tomb.record.key == graph.edge_key("e:1")


# ── no key can be chosen ────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("method", ["node", "edge", "tombstone_node", "tombstone_edge"])
def test_no_method_takes_a_key(method: str) -> None:
    parameters = inspect.signature(getattr(GraphDeltaOutlet, method)).parameters
    assert "key" not in parameters
    assert not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values())
    with pytest.raises(TypeError):
        getattr(OUT, method)(None, id="x", version=1, key=b"mine")


# ── what the sink would refuse is refused here, naming the argument ─────────────────────────────


def node(**overrides: Any) -> Emit:
    args: dict[str, Any] = {"id": "n", "version": 1, "labels": ["Thing"], "properties": {}}
    args.update(overrides)
    return OUT.node(None, **args)


def edge(**overrides: Any) -> Emit:
    args: dict[str, Any] = {"id": "e", "version": 1, "type": "LINKS", "from_id": "a", "to_id": "b"}
    args.update(overrides)
    return OUT.edge(None, **args)


@pytest.mark.parametrize(
    ("overrides", "names"),
    [
        ({"id": ""}, "id"),
        ({"id": 7}, "id"),
        ({"version": -1}, "version"),
        ({"version": 1.5}, "version"),
        ({"version": "1"}, "version"),
        ({"version": True}, "version"),
        ({"version": 2**63}, "version"),
        ({"labels": ["has space"]}, "label"),
        ({"labels": ["1st"]}, "label"),
        ({"labels": "Cart"}, "labels"),
        ({"properties": {"id": "x"}}, "'id' is reserved"),
        ({"properties": {"_version": 3}}, "'_version' is reserved"),
        ({"properties": {"_deleted": True}}, "'_deleted' is reserved"),
        ({"properties": {"a": None}}, "property 'a'"),
        ({"properties": {"a": {"b": 1}}}, "property 'a'"),
        ({"properties": {"a": []}}, "property 'a'"),
        ({"properties": {"a": [1, "x"]}}, "property 'a'"),
        ({"properties": {"a": [1, 1.5]}}, "property 'a'"),
        ({"properties": {"a": [True, 1]}}, "property 'a'"),
        ({"properties": {"a": [[1]]}}, "property 'a'"),
        ({"properties": {"a": float("nan")}}, "property 'a'"),
        ({"properties": {"a": 2**63}}, "property 'a'"),
        ({"properties": {1: "x"}}, "property name"),
        ({"properties": [("a", 1)]}, "properties"),
    ],
)
def test_a_node_the_sink_would_refuse_raises(overrides: dict[str, Any], names: str) -> None:
    with pytest.raises(ValueError, match=names):
        node(**overrides)


@pytest.mark.parametrize(
    ("overrides", "names"),
    [
        ({"type": "checked out"}, "type"),
        ({"type": ""}, "type"),
        ({"from_id": ""}, "from_id"),
        ({"to_id": ""}, "to_id"),
        ({"version": True}, "version"),
        ({"properties": {"a": object()}}, "property 'a'"),
    ],
)
def test_an_edge_the_sink_would_refuse_raises(overrides: dict[str, Any], names: str) -> None:
    with pytest.raises(ValueError, match=names):
        edge(**overrides)


def test_tombstones_validate_too() -> None:
    with pytest.raises(ValueError, match="version"):
        OUT.tombstone_node(None, id="n", version=-1)
    with pytest.raises(ValueError, match="id"):
        OUT.tombstone_node(None, id="", version=1)
    with pytest.raises(ValueError, match="type"):
        OUT.tombstone_edge(None, id="e", version=1, type="not ok", from_id="a", to_id="b")
    with pytest.raises(ValueError, match="to_id"):
        OUT.tombstone_edge(None, id="e", version=1, type="LINKS", from_id="a", to_id="")


def test_values_the_sink_accepts_are_accepted() -> None:
    emit = node(
        labels=[],
        properties={"s": "x", "i": 9007199254740993, "f": 1.25, "b": False, "xs": ["a", "b"], "ns": (1, 2)},
    )
    assert json.loads(emit.record.value)["properties"] == {
        "s": "x",
        "i": 9007199254740993,
        "f": 1.25,
        "b": False,
        "xs": ["a", "b"],
        "ns": [1, 2],
    }
    assert json.loads(node(labels=()).record.value)["labels"] == []
    assert json.loads(OUT.node(None, id="n", version=0).record.value) == {
        "kind": "node",
        "id": "n",
        "version": 0,
        "labels": [],
        "properties": {},
    }


# ── read ────────────────────────────────────────────────────────────────────────────────────────


def test_read_round_trips_each_kind() -> None:
    n = graph.read(OUT.node(None, id="cart:1", version=7, labels=["Cart"], properties={"cartId": "1"}).record)
    assert n == graph.Delta(
        kind="node",
        element="node",
        id="cart:1",
        version=7,
        labels=("Cart",),
        properties={"cartId": "1"},
        key=b"node:cart:1",
    )
    e = graph.read(
        OUT.edge(None, id="e:1", version=7, type="LINKS", from_id="a", to_id="b", properties={"w": 2}).record
    )
    assert (e.kind, e.element, e.type, e.from_id, e.to_id, e.properties, e.key) == (
        "edge",
        "edge",
        "LINKS",
        "a",
        "b",
        {"w": 2},
        b"edge:e:1",
    )
    tn = graph.read(OUT.tombstone_node(None, id="cart:1", version=8).record)
    assert (tn.kind, tn.element, tn.id, tn.version, tn.key) == ("tombstone", "node", "cart:1", 8, b"node:cart:1")
    te = graph.read(OUT.tombstone_edge(None, id="e:1", version=8, type="LINKS", from_id="a", to_id="b").record)
    assert (te.kind, te.element, te.type, te.from_id, te.to_id, te.key) == (
        "tombstone",
        "edge",
        "LINKS",
        "a",
        "b",
        b"edge:e:1",
    )


def test_read_refuses_a_wrongly_keyed_or_keyless_delta() -> None:
    value = OUT.node(None, id="cart:cart-1", version=1).record.value
    with pytest.raises(ValueError, match="key 'cart-1' is not this delta's element key 'node:cart:cart-1'"):
        graph.read(Record(value=value, key=b"cart-1"))
    with pytest.raises(ValueError, match="key 'edge:cart:cart-1' is not this delta's element key"):
        graph.read(Record(value=value, key=b"edge:cart:cart-1"))
    with pytest.raises(ValueError, match="no key; this delta's element key is 'node:cart:cart-1'"):
        graph.read(Record(value=value, key=None))


@pytest.mark.parametrize(
    ("value", "says"),
    [
        (b"not json", "not a JSON object"),
        (b"[1, 2]", "not a JSON object"),
        (b"", "not a JSON object"),
        (b'{"id": "n", "version": 1}', "kind missing"),
        (b'{"kind": "nod", "id": "n", "version": 1}', "unknown kind 'nod'"),
        (b'{"kind": "node", "id": "", "version": 1}', "id"),
        (b'{"kind": "node", "id": "n", "version": -1}', "version"),
        (b'{"kind": "node", "id": "n", "version": 1.5}', "version"),
        (b'{"kind": "edge", "id": "e", "version": 1, "from": "a", "to": "b"}', "type"),
        (b'{"kind": "tombstone", "id": "n", "version": 1}', "tombstone needs element"),
        (b'{"kind": "node", "id": "n", "version": 1, "properties": {"a": null}}', "property 'a'"),
    ],
)
def test_read_refuses_what_is_not_a_delta(value: bytes, says: str) -> None:
    with pytest.raises(ValueError, match=says):
        graph.read(Record(value=value, key=b"node:n"))


def test_read_takes_a_whole_number_written_with_an_exponent_as_the_sink_does() -> None:
    delta = graph.read(Record(value=b'{"kind": "node", "id": "n", "version": 1e3}', key=b"node:n"))
    assert delta.version == 1000


# ── the outlet is an ordinary outlet of the delta contract ──────────────────────────────────────


class WithHelper(Streamlet):
    name = "mapper"
    notices = JsonInlet("in", schema_name="notices.v1")
    deltas = GraphDeltaOutlet("deltas")

    def process(self, batch: Batch) -> Iterable[Emit]:
        for record in batch:
            body = json.loads(record.value)
            yield self.deltas.node(record, id=f"thing:{body['id']}", version=body["v"], labels=["Thing"])


class ByHand(Streamlet):
    name = "mapper"
    notices = JsonInlet("in", schema_name="notices.v1")
    deltas = JsonOutlet("deltas", schema_name="ankka.graph-delta.v1")

    def process(self, batch: Batch) -> Iterable[Emit]:
        return []


def test_the_descriptor_is_the_one_a_json_outlet_of_the_contract_writes() -> None:
    assert OUT.schema_name == graph.SCHEMA_NAME == "ankka.graph-delta.v1"
    assert write(spec_for(WithHelper)) == write(spec_for(ByHand))


def test_a_delta_built_from_an_input_record_carries_its_headers_and_is_not_skipped() -> None:
    h = Harness(WithHelper())
    h.inlet("in").put(key=b"a", value=b'{"id": "a", "v": 3}', headers=[("ce-id", b"n-1")])
    h.inlet("in").put(key=b"b", value=b'{"id": "b", "v": 4}')
    h.run()
    assert h.failures == []
    assert h.skipped == []
    records = h.outlet("deltas").records
    assert [graph.read(r).key for r in records] == [b"node:thing:a", b"node:thing:b"]
    assert records[0].headers == [("ce-id", b"n-1")]
    assert [r.offset for r in records] == [0, 1]


def test_a_delta_built_from_nothing_has_no_headers() -> None:
    record = OUT.node(None, id="n", version=1).record
    assert record.headers == []
    assert record.key == b"node:n"
