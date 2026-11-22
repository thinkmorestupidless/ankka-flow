# Fixture declarations

Each file describes one streamlet declaration and the engine it is written for. The SDK of that
engine declares the streamlet in its own language, writes its descriptor with `sdk` pinned to
`{"name": "fixture", "version": "0.0.0"}`, and asserts the bytes equal
`../descriptors/<name>.json`. Declaration order in the files below is deliberately not sorted where
sorting is what the fixture tests. A contract with a schema is fingerprinted from the file named;
one without names its contract only.
