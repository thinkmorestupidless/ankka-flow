from ankka_flow.testkit import Harness

from {{module}}.streamlet import Echo


def test_forwards_every_record() -> None:
    h = Harness(Echo())
    h.inlet("in").put(key=b"k", value=b'{"n": 1}', headers=[("ce_type", b"Thing")])
    h.run()
    assert [r.value for r in h.outlet("out").records] == [b'{"n": 1}']
    assert h.outlet("out").records[0].headers == [("ce_type", b"Thing")]
