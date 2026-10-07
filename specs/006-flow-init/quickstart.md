# Quickstart: `flow init`

## Tier 1 — the command

```bash
sbt 'cli/testOnly *InitSuite'
cli/target/universal/stage/bin/flow init order-greeter -l python --dir /tmp/x && ls -a /tmp/x
```

Expected: every refusal refused with nothing written; both languages render with no token left.

## Tier 2 — the projects, built

```bash
sbt 'cli/testOnly *TemplateSuite'                    # both languages; -Dflow.template.tests=python for one
```

Expected: for each language the project's tests, descriptor check and image build pass, and
`flow verify` accepts its blueprint.

## Tier 3 — the laptop loop, by hand (SC-002)

With a released `flow`: `flow init greeter`, then the README's commands — compose up, the streamlet
on the host, a console-produced `{"id": 1}` read back from the output topic as
`{"greeting": "hello, ankka-flow", "id": 1}`. Both languages.

## Tier 4 — the native binary

```bash
just cli-native            # the smoke script now runs init for both languages and cmp -r's them against the JVM build
```

## Reviewer's checklist

- [ ] nothing in the SDKs, the sidecar, the operator or the protocol changed
- [ ] each template's files equal its index; the build fails on a path written twice
- [ ] the project depends on the SDK and the sidecar image at the CLI's version
- [ ] no `{{` survives rendering
- [ ] the template suite builds both languages against this repository's SDK
- [ ] the native binary writes byte-identical projects
- [ ] the tutorial starts from `flow init`; the CLI reference documents it
