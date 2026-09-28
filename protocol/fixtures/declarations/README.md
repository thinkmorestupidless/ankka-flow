# Fixture declarations

Each file describes one streamlet declaration. Every SDK declares each streamlet in its own
language, writes its descriptor with `sdk` pinned to `{"name": "fixture", "version": "0.0.0"}`,
and asserts the bytes equal `../descriptors/<name>.json`. Declaration order in the files below is
deliberately not sorted where sorting is what the fixture tests.
