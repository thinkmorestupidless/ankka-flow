# Contract: `ankka_flow.graph`

How a Python mapper writes graph deltas. The module builds the record, key included; the author
never chooses a key.

## Declaring

```python
from ankka_flow import GraphDeltaOutlet, JsonInlet, Streamlet

class CheckoutGraph(Streamlet):
    name = "checkout-graph"
    notices = JsonInlet("in", schema_name="ankka.checkout-notice.v1")
    deltas = GraphDeltaOutlet("deltas")          # the contract is the outlet's, not the author's
```

`GraphDeltaOutlet` is a `JsonOutlet` with `schema_name` fixed to `ankka.graph-delta.v1`; the
descriptor it yields is the one `JsonOutlet("deltas", schema_name="ankka.graph-delta.v1")` yields.

## Emitting

```python
yield self.deltas.node(record, id=f"cart:{cart}", version=at, labels=["Cart"], properties={"cartId": cart})
yield self.deltas.edge(record, id=f"checked-out:{cart}:{at}", version=at, type="CHECKED_OUT",
                       from_id=f"cart:{cart}", to_id=f"checkout:{cart}:{at}")
yield self.deltas.tombstone_node(record, id=f"cart:{cart}", version=at)
yield self.deltas.tombstone_edge(record, id=…, version=at, type="CHECKED_OUT", from_id=…, to_id=…)
```

| method | key of the record | value |
|---|---|---|
| `node(record, *, id, version, labels=(), properties=None)` | `node:<id>` | `{"kind": "node", …}` |
| `edge(record, *, id, version, type, from_id, to_id, properties=None)` | `edge:<id>` | `{"kind": "edge", "from": from_id, "to": to_id, …}` |
| `tombstone_node(record, *, id, version)` | `node:<id>` | `{"kind": "tombstone", "element": "node", …}` |
| `tombstone_edge(record, *, id, version, type, from_id, to_id)` | `edge:<id>` | `{"kind": "tombstone", "element": "edge", …}` |

`record` is the input the delta was derived from: its headers and offset are carried, so the
harness sees it was not skipped. `None` builds a record with no headers. No method takes a key.
The inherited `emit` still exists (it is how any outlet forwards a record); a delta emitted through
it with another key is refused by the sink, and the documentation says to use the four methods.

Each method raises `ValueError`, naming the argument, for: an empty `id`; a `version` that is not a
non-negative `int` (a `bool` is not an `int` here); a label or `type` that is not an identifier
(`[A-Za-z_][A-Za-z0-9_]*`); an empty `from_id` or `to_id`; a property named `id`, `_version` or
`_deleted`; a property value that is not a `str`, `int`, `float`, `bool` or a non-empty list of one
of those types.

## Testing a mapper

```python
from ankka_flow import graph

deltas = [graph.read(r) for r in h.outlet("deltas").records]
assert [(d.kind, d.key) for d in deltas] == [("node", b"node:cart:cart-1"), …]
```

`graph.read(record) -> Delta` parses an emitted record and raises `ValueError` when the record is
not a delta or its key is not its element's. `Delta` has `kind`, `element` (`"node"` or `"edge"`),
`id`, `version`, `labels`, `type`, `from_id`, `to_id`, `properties`, `key`. `graph.node_key(id)` and
`graph.edge_key(id)` give the key bytes, for a script that writes deltas with a plain Kafka client.
