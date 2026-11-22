# Contract fixtures

`fingerprints.json` is ankka's `protocol/fixtures/contracts/fingerprints.json` at tag `v0.18.0`,
copied unchanged. Each element is a contract's name, a schema document, and the fingerprint ankka
computes for that document: `sha256:` and the lowercase hex SHA-256 of the document rendered under
RFC 8785.

`descriptor`'s `CanonicalSuite` and the Flink SDK's `CanonicalTest` fingerprint every document and
compare with the value stated here, so a fingerprint either SDK writes is the one ankka declares.
Refresh it from a later ankka tag only by copying the file again and naming the tag above.
