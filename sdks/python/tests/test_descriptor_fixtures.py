"""FR-002: the same declaration produces the same bytes in every language."""

from __future__ import annotations

import pathlib

import pytest

from ankka_flow.descriptor import spec_for, validate, write
from fixture_streamlets import FIXTURES

DESCRIPTORS = pathlib.Path(__file__).resolve().parent.parent / "proto" / "fixtures" / "descriptors"


def test_every_fixture_file_has_a_declaration() -> None:
    assert sorted(p.stem for p in DESCRIPTORS.glob("*.json")) == sorted(FIXTURES)


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_fixture_bytes(name: str) -> None:
    spec = spec_for(FIXTURES[name], sdk_name="fixture", sdk_version="0.0.0")
    assert validate(spec) == []
    expected = (DESCRIPTORS / f"{name}.json").read_bytes()
    assert write(spec).encode("utf-8") == expected


def test_outside_fixtures_the_sdk_names_itself() -> None:
    text = write(spec_for(FIXTURES["minimal"]))
    assert '"name": "ankka-flow-python"' in text
