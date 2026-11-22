# Review: 009 engine streamlets (origin/main...HEAD, branch `009-engine-streamlets`)

**Verdict:** Request changes
**What it does:** Replaces version one (the sidecar, its protocol, the Scala and Python SDKs written
against it, the built-in Neo4j stage, four samples and `flow init`) with engine streamlets: a Pekko
Streams SDK in Scala and a Flink SDK and testkit in Java, which write one descriptor format byte for
byte. The operator runs a Pekko Streams streamlet as a Deployment and a Flink streamlet as a
`FlinkDeployment` with its state in a configured store. Blueprints can name ankka topics, which
`flow verify` checks against an exported declarations file, and `flow local` runs a pipeline on a
laptop.
**Should it exist:** Yes. The spec explains why version one has no future beside ankka's consumer
(feature 037) and what the engines add: windows, event time and large state. The seam is in the
right place, because blueprints, contracts, lag and reset do not depend on the engine, and the
SC-001 record shows a stage moving engines without touching the blueprint.
**Read:** In full: `spec.md`, `tasks.md`, the parts of `research.md` on commit-after-write and on
verification (V6, T065 to T067, Tier 3), `CLAUDE.md`, `docs/contributing/documentation.md`,
`docs/reference/limitations.md`, `docs/get-started/first-streamlet.md`; `sdks/pekko-streams/…/Runner.scala`,
`CommitAfterWrite.scala`, `Stage.scala`, the tail of `PekkoStreamlet.scala`; `sdks/flink/…/Connectors.java`,
`FlinkStreamlets.java`; `operator/…/FlinkRendering.scala`, `StateStore.scala`, `Grant.scala`;
`cli/…/AnkkaDeclarations.scala`; `blueprint/…/AnkkaTopics.scala`; the two sample streamlets; the
`release.yml` diff; `RunnerKafkaSuite` (commit cases). Skimmed: every docs page for its fences,
includes and `languages` facet; the rest of `plan.md` and `research.md`; the remaining operator, CLI,
descriptor and Flink SDK files, by structure and grep. Not reviewed line by line: the deletions of
version one (read only for references left behind: SC-005 grep), the rendered marketplace copies,
`uv.lock`, `reachability-metadata.json`, `operator/src/test/resources/flink-operator.yaml` (rendered).
Opened outside the diff: Pekko 1.7.0 `StatefulMap` (`stream/impl/fusing/Ops.scala`),
pekko-connectors-kafka 1.2.0 `CommitCollectorStage` and `Committer`, ankka's docs tool `check.py`
(`samples`, `tested`) in `tools/docs/.venv`, ankka's `mkdocs.yml` `extra.docs.samples`. Needs
another reviewer: the Flink operator wiring (HA, `last-state` upgrades, RBAC for the Flink service
account), whose behaviour only the k3s suite can show.
**Size:** 680 files, +47,844 −31,649 hand-written lines (the summary's count, generated files and
lockfiles excluded). Most of the deletion is version one, and the 52 commits follow the task phases
one by one, so the change can be reviewed commit by commit. Even so, it is far past the size at
which a reviewer still finds defects reliably. The split to accept next time: (1) the spec and
design, (2) the descriptor module and the two SDKs, (3) the operator and CLI, (4) ankka topics,
(5) the removal of version one.

## Blocking

- `sdks/pekko-streams/src/main/scala/com/thinkmorestupidless/ankka/flow/pekko/CommitAfterWrite.scala:116-128`
  — issue (blocking): when the stream fails, the held offsets are handed to the committer, and the
  committer commits them, so a record whose emits were only partly confirmed can be committed and
  its remaining emits lost.
  The design says held offsets are released "when the stream completes (never on failure)"
  (`research.md` line 673, and the docstring at line 101: "what was held back is not committed
  either"). Pekko 1.7.0 does not work that way. `StatefulMap.onUpstreamFailure` calls
  `closeStateAndFail`, which runs `onComplete(state)` and emits its result before failing
  (`Ops.scala`, `closeStateAndFail`). So line 125 emits `CommittableOffsetBatch(held.values)` on
  failure too. The committer's `CommitCollectorStage.onUpstreamFailure` then calls
  `offsetBatch.tellCommitEmergency()` on the batch in progress.
  The case that loses data: `Stage` allows `mapConcat` (`Stage.scala:18`), so one record at offset
  H can become several elements. If the first element's write is confirmed, H is held. If the
  second element's write fails (a broker error, or a grant revoked on that outlet), the failure
  reaches `statefulMap`, H is emitted, and the committer commits it as an emergency. The restart
  then reads from H+1, and the unwritten emit is never written. This breaks FR-007 ("never skip")
  and the CLAUDE.md rule "Commit after the write".
  Whether the commit lands depends on a race with the consumer actor stopping at once
  (`withStopTimeout(Duration.Zero)`, `Runner.scala:214`). That is why no suite has caught it, and
  why it cannot be relied on in either direction. `RunnerKafkaSuite`'s "logic throws" case maps
  one element per record, so it cannot show the problem. The finding is confirmed by reading the
  library sources; it has not been reproduced in a run.
  Suggestion: release held offsets only on normal completion, not in `onComplete`. One way is to
  concatenate a flush marker before the `statefulMap`, since `concat` emits only after the upstream
  completes successfully:
  ```scala
  .mapAsync(parallelism)(…)                       // as now, emitting Some(offset)
  .concat(Source.single(None))                    // only on normal completion
  .statefulMap(() => Map.empty: Held)(
    (held, o) => o match
      case Some(offset) => /* as now */
      case None         => (Map.empty, CommittableOffsetBatch(held.values.toSeq)),
    _ => None                                     // never release on failure
  )
  ```
  Add a `RunnerKafkaSuite` case to go with it: a `mapConcat` stage that writes two emits per record,
  the second to an outlet whose write fails. Assert that the record's offset is not committed and
  that both emits appear after the failure clears. Add this case to the set `mutationCheck` guards
  too.
  Resolved: the hold stage is `holdUntilNext`, which releases on a marker concatenated after the writes and nothing in `onComplete`; `HoldUntilNextSuite` fails with the old release (checked by reverting it), and a `RunnerKafkaSuite` case writes two emits per record, the second past the broker's size limit; `pekkoSdk/test` 47 green, `mutationCheck` kills the mutation, 9dd5f8f. The Kafka case passed with the defect too (the emergency commit races the stop), which research.md records; the mutation set is unchanged, as `commit-first` mutates the ordering of write and commit, which the new unit case does not exercise.

## Should fix

- `docs/contributing/documentation.md:47-49` — issue (non-blocking): this page states the
  repository's language rule, and the rule still describes version one. It says the `languages`
  facet takes "`scala` and `python`" and that a page with both shows "`/// tab | Scala` and
  `/// tab | Python` blocks, Scala first, each sample included from tested code". The facet is now
  `[scala, java]` (`mkdocs.yml`). Suggestion: restate it for Scala and Java, and say how a sample
  bound to one engine, and so to one language, is shown: in an engine's own section, with the
  docs tool's `<!-- languages: scala; … -->` comment giving the reason.
  Resolved: the rule restated for Scala and Java, with how a one-engine sample gives its reason, and that the check enforces it, 9a80d39.
- `mkdocs.yml` (`extra.docs`) — issue (non-blocking): nothing enforces the rule above. The ankka
  docs tool pinned here (v0.18.0) has a check that shows every sample in each language a page
  names, or records why not, and that each tab is included from tested code. ankka turns it on with
  `extra.docs.samples: {facet: languages, tested: true}`; this repository does not. On a scratch
  copy with it turned on, `docs check` reports 8 problems that the default run (`35 pages, no
  problems`) does not:
  - `docs/build/images.md:14`, `docs/build/testing.md:22`, `docs/build/testing.md:80`: a Scala or
    Java block on a `[scala, java]` page with no reason given for the other language.
  - `docs/build/flink-streamlet.md:19`: an sbt (`scala`) block on a `[java]` page.
  - `docs/get-started/install.md:91,97`: Scala blocks on a page that names no `languages`.
  - `docs/concepts/contracts.md:16,25`: the Scala and Java tabs are written in the page, not
    included from tested code (see the next finding).
  No sample is missing a language its engine supports: every Pekko Streams sample is in Scala and
  every Flink sample is in Java, which is what the spec decides. The repository's rule does not
  call a missing language blocking, so this is should-fix. Suggestion: turn the check on, and
  answer each problem with an include or a `<!-- languages: …; <reason> -->` /
  `<!-- untested: <reason> -->` comment.
  Resolved: `extra.docs.samples` on with `tested: true`; each of the eight problems answered by an include or a `languages:` comment; `docs check` 35 pages, no problems, 9a80d39.
- `docs/concepts/contracts.md:23-37` — issue (non-blocking): the only tab set in the docs holds code
  written in the page. It matches `Cleaner.scala` and `CartCounter.java` today, but nothing fails
  when they change. That breaks the page's own rule ("each sample included from tested code") and
  the "Samples come from tested code" section. Suggestion: mark `// docs:start ports` regions
  around the port declarations in both samples, and use
  `<!-- include: samples/cart-pipeline/cleaner/src/main/scala/cart/cleaner/Cleaner.scala#ports -->`
  and the Java equivalent.
  Resolved: `ports` regions in `Cleaner.scala` and `CartCounter.java`, included by both tabs; the samples' tests and descriptor checks green, 9a80d39.
- `docs/reference/limitations.md` (Engines) — issue (non-blocking): the page names the language
  limit for Flink only ("Flink streamlets in Java … Scala on Flink's Java API may work and is
  neither documented nor tested"). It says nothing about the matching limit for Pekko Streams: a
  Pekko Streams streamlet is written in Scala 3.3 or later only. Pekko's `javadsl` has no SDK, and
  Scala 2.13 has no artifact. A Java reader of `concepts/engines.md` who picks Pekko Streams for a
  stateless stage first learns this from the build page. Suggestion: add
  "**Pekko Streams streamlets in Scala.** The SDK is Scala 3.3 or later; there is no Java API for
  it." beside the Flink entry.
  Resolved: "Pekko Streams streamlets in Scala" added beside the Flink entry, 9a80d39.
- `features/cli/install.feature:9` — issue (non-blocking): "`flow version` prints the release's
  version and the protocol version" describes the protocol that this change removes. `flow version`
  now prints `flow <v>, descriptor <n>` (`Main.scala:182`). SC-005 says no scenario describes the
  protocol, and the checker passes only because "protocol" is still in the glossary for MCP.
  Suggestion: "… and the descriptor version".
  Resolved: "… and the descriptor version"; `just features` 0 findings, 5dc8533.
- `cli/src/main/resources/META-INF/native-image/…/reachability-metadata.json` — todo: according to
  commit `300430a`, this metadata was edited by hand (the new resource types registered, the
  templates dropped) without the tracing agent, and `just cli-native` was not run ("CI's cli-native
  job is the first to build the binary"). CLAUDE.md says the metadata is generated, then pruned,
  and regenerated "on … a new command", and `flow local` is a new command. Suggestion: before
  merge, show a green `cli-native` run with `cli/native-smoke.sh` on at least one platform, or
  regenerate with `-Dflow.cli.agent=on` and prune.
  Answered: `ci.yml`'s `cli-native` job runs on every pull request: it builds the binary, runs `cli/test` through it and byte-compares `native-smoke.sh` with the JVM build. The pull request is marked ready only once that check is green; GraalVM is not on the machine this was built on, so that run is the first.

## Nits

- `docs/build/flink-streamlet.md:24-31` — nitpick (non-blocking): the Flink SDK is a plain Java
  library, but its only build sample is sbt. A Java reader most often has Maven or Gradle; one
  Maven `<dependency>` block with `provided` scope for Flink would serve them. (`install.md:103`
  has the same sbt-only line.)
  Resolved: a Maven `<dependencies>` block beside the sbt one, Flink `provided`, 9a80d39. `install.md` keeps its one sbt line, which now says the Maven build is on the Flink page.
- `docs/get-started/first-streamlet.md:5` — nitpick (non-blocking): the page is titled "Your first
  streamlet" and declares `languages: [scala, java]`, but it shows no streamlet code, only the
  blueprint and commands. Either drop the facet, or include the cleaner's and the counter's
  declarations (the `ports` regions suggested above would serve).
  Resolved: the facet dropped, as the tutorial shows no streamlet code, 9a80d39.

## Questions

- `operator/src/main/scala/com/thinkmorestupidless/ankka/flow/operator/FlinkRendering.scala:63` —
  question: `store.flinkConfig ++ own` lets flow's keys override the store's, so a state store
  cannot set `restart-strategy.type` or the checkpoint interval for an installation. Is that the
  intent? If so, `deploy/install.md` should say which keys the `flink-config` entry cannot change.
  Answered: yes, intended: those keys carry the streamlet's own directories in the store, which state epochs and resets rest on, and the delivery the docs promise. `deploy/install.md` now lists every key the operator sets over `flink-config`, bce28c9.
- `cli/src/main/scala/com/thinkmorestupidless/ankka/flow/cli/AnkkaDeclarations.scala:42-46` —
  question: a topic whose `contract` object has no string `name` is read as declaring no contract,
  and is then reported as `AnkkaTopicWithoutContract` rather than as a file it cannot read. Is
  ankka's `-o json` guaranteed never to print such an object? If not, refusing it would name the
  real problem.
  Resolved: nothing guarantees it, so a contract that is not absent, `null`, or an object with a string `name` is refused as unreadable, naming the topic, with its test, e1b25bb.
- thought: research.md records that a Pekko Streams streamlet keeps a quiet partition's last record
  uncommitted, so lag stays at 1 per quiet partition, and the observe page says so. That is honest.
  It does mean "lag 0" never means caught up for this engine, which the engines page could say
  where readers compare the two.
  Resolved: the engines page says what each engine's lag rests at, linking to the observe page, bce28c9.

## Praise

- `descriptor/fixtures/` with `DescriptorFixturesSuite` and `DescriptorFixturesTest` — praise: one
  set of fixtures, written byte for byte by a Scala SDK and a Java SDK with no shared code. This
  holds the "one descriptor" rule by construction rather than by review.
- `specs/009-engine-streamlets/research.md` (V6, T065, Tier 3) — praise: the verification record
  says what was not run and why (Tier 3, the native build). It also records where an assumption
  failed (a `FlinkDeployment` that cannot restore its state stays `DEPLOYED`, so the condition
  comes from the `JobException` event). That makes the gaps visible to the next person.
- `operator/…/StreamletFiles.scala:84,106` — praise: `read_committed` on readers of managed topics
  and `transaction.timeout.ms=900000` on exactly-once outlets. These are the two settings Flink's
  exactly-once Kafka sink needs and most often lacks.
- `docs/reference/limitations.md` (Delivery) — praise: each engine's delivery promise is stated
  per destination, including "at least once into ankka" and the one-checkpoint delay downstream of
  Flink, which FR-027 asks for.

## Out of scope

- note: `ankka-brain/family.md` says the one Pekko line is 1.7.0, while commit `75bebc6` moves this
  repository to Pekko 1.7.1. T068 updates the brain in its own checkout. Confirm that it did,
  rather than changing it here.
  Answered: done at T068, in ankka-brain's own checkout (local commit 4385433): the Pekko line note says the Pekko Streams SDK is at Pekko 1.7.1 and sbt 1.13.1, Flink on its own line.

## Checks applied

General groups applied: Correctness, Design and scope, Error handling, Concurrency and state,
Security (credentials reaching pods, the state-store `env` Secret, CI publishing), Data and
migrations (stored formats: the descriptor and Flink state, the reset epoch), Tests, Observability
(conditions for grants and the state store), API and compatibility (the breaking removal,
`BREAKING CHANGE:` footer present), Dependencies and build (Flink line, BSL check V9, release jobs),
Documentation (the language rule, the sample includes, limitations), Commit and PR hygiene
(Conventional Commits: all 52 headers parse; the breaking commit carries `!` and the footer).
Skipped: Performance beyond the in-flight bound, because there is no new hot path to measure from
the diff.
Language skills: `pr-review-scala` (concurrency and streams, types and totality, error handling,
tests, build); `pr-review-java` (error handling and resources, serialization, API, build). There is
no repository-specific language skill and no `REVIEW.md`.
Project rules read: `CLAUDE.md` (Rules table, Documentation, Living features), the workspace
`CLAUDE.md` and brain principles, `docs/contributing/documentation.md`, ankka's docs tool rules for
`samples` and `tested`.
Enforced by tooling and not reviewed: scalafmt (hook), `-Wunused:all`, `--release 17` on the Java
modules, `docs check` as configured (it passes: `35 pages, no problems`).
Tests run: `sbt -Dflow.cluster.tests=off descriptor/test blueprint/test`, which passed (58 and 63).
`docs check` was run as configured, and again on a scratch copy with `extra.docs.samples` turned on
(8 problems, listed above). Not run: the Pekko SDK's Kafka suites and the operator suites (Docker
and k3s, outside this review's budget), and the native build (no GraalVM run was asked for). The
blocking finding comes from reading the Pekko and pekko-connectors-kafka sources and needs the
suggested Kafka case to reproduce it.
