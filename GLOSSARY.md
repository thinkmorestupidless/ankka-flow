# Glossary

The words this project's features use, each in exactly one sense. A term marked has
still to be settled by `/speckit-clarify`. Where `docs/reference/glossary.md` defines a word for the
people who build on ankka-flow, the term here means the same.

## The platform

### pipeline
A set of streamlets wired by a blueprint over topics, deployed to a cluster as one resource.

### streamlet
One stage of a pipeline: a process with typed inlets and outlets, or a stage built into the
sidecar.

### blueprint
The file that names a pipeline's streamlets and wires their inlets and outlets over topics.

### descriptor
The file a streamlet's SDK writes, saying what the streamlet is: its inlets, outlets and
parameters.

Avoid: manifest, spec

### deploy-time configuration
Settings supplied when a pipeline is deployed rather than written in its blueprint: replicas, a
streamlet's parameters, a Secret's name.

Avoid: config, settings

### resource
The `AnkkaFlow` resource `flow` writes for a pipeline, which a cluster runs.

### reset
A request that a pipeline, or one of its streamlets, read its topics again from the start.

### cluster
The Kubernetes cluster a pipeline is deployed to.

### protocol version
The version of the streamlet protocol `flow` writes into a resource.

### note
A line `flow` prints beside a verification saying what it decided, such as that a topic will be
compacted.

### refusal
An answer from `flow` that it will not do what was asked, saying why.

## The CLI and its release

### ankka
The platform ankka-flow runs beside, whose CLI ships through the same tap.

### ankka-flow
This project: the pipelines, the sidecar, the operator and `flow`.

### CLI
A command a person runs on their own machine: `flow`, or ankka's.

### flow
The command a person runs to verify a blueprint, generate a resource, reset a pipeline
and print its version.

### JVM build
`flow` as it is built from source and run on a Java virtual machine.

### native binary
`flow` as one executable for one platform, run with no Java virtual machine.

Avoid: executable alone

### platform
An operating system and processor a native binary is built for: macOS on Apple
silicon, macOS on Intel, Linux on x64, Linux on arm64.

Avoid: target, architecture

### release
What a tagged version of ankka-flow publishes: images, the Python SDK, the plugin, and
the archives.

Avoid: drop, cut

### archive
A compressed file on a release holding one native binary, named for the version and the
platform.

### checksum
The digest published beside an archive, by which the archive is verified.

### tap
The Homebrew repository `thinkmorestupidless/homebrew-tap`, through which ankka's CLI
and `flow` are installed.

Avoid: repository alone

### formula
A tap's definition of one installable: its version, its archives and their checksums.

Avoid: recipe, package

### smoke script
A script that runs a native binary and asks it for each thing a native build can
silently lose, failing with the name of the first thing missing.

### suite
The CLI's own tests, which run against the JVM build or the native binary.

### checkout
A copy of the repository on a machine, at one commit.

### dirty tree
A checkout with changes not committed; a release refuses to build from one.

### build task
The command that builds the JVM build from source.

### install page
The documentation page that says how to get `flow`.

### guide
A documentation page that walks a reader through doing something with ankka-flow.

### contributing page
The documentation page for people changing ankka-flow itself.

### skill
A rendering of the documentation for a coding agent, published with the plugin.

## The SDKs

### SDK
A library a streamlet is written against, which writes the streamlet's descriptor,
serves it to the sidecar and ships a harness. One per language.

Avoid: library, client

### Python SDK
The SDK for streamlets written in Python, published to PyPI.

### Scala SDK
The SDK for streamlets written in Scala, published to Maven Central.

### streamlet author
A person writing a streamlet against an SDK.

Avoid: developer, user

### process
The method of a streamlet that takes one batch and returns the emits to make.

Avoid: handler, callback

### batch
The records of one inlet partition that a streamlet processes in one call, in offset
order.

Avoid: chunk, micro-batch

### record
What a topic holds and a streamlet sees: bytes, an optional key, and ordered headers.

Avoid: message

### emit
A record a streamlet sends to one of its outlets while processing a batch.

Avoid: produce, publish

### inlet
A port a streamlet reads from.

Avoid: port alone, upstream

### outlet
A port a streamlet writes to.

Avoid: port alone, destination

### parameter
A typed setting a streamlet declares, given a value by the deploy-time configuration or
its default.

Avoid: option, knob

### partition
One of the ordered parts of a topic; records with one key are in one partition.

Avoid: shard

### harness
An SDK's stand-in for Kafka and the sidecar in a test: it batches and partitions
records as the sidecar does and records what a streamlet emits, fails or skips.

Avoid: test harness, testkit

### sidecar
The container the platform runs beside every streamlet process, owning everything
Kafka and driving the streamlet through the protocol.

Avoid: proxy, ambassador

### protocol
The conversation between the sidecar and a streamlet process: discovery, start,
batches, emits, acknowledgements, failures and stop.

Avoid: gRPC, API

### discovery
The sidecar's first question to a process: what streamlet it is, answered with the
descriptor.

### process port
The loopback port a streamlet process serves on, given to it by the platform.

### descriptor fixtures
The six declared streamlets whose descriptor bytes every SDK must reproduce.

### fixture streamlet
One of the six streamlets the descriptor fixtures describe.

### conformance suite
The repository's suite that drives a served streamlet through every conversation the
sidecar can have with it.

Avoid: compliance tests

### reference streamlet
The streamlet the conformance suite drives, behaving by each record's key.

### cart router
The sample streamlet: one inlet of cart events, two outlets, one parameter.

### cart event
A record the cart router reads, keyed by cart id, carrying a total.

### laptop walkthrough
The first-streamlet tutorial's run: Kafka and the sidecar in containers, the streamlet
on the host, events produced and verified.

### Maven Central
The registry Scala and Java artifacts are published to and resolved from by version.

### registry
Where a release publishes images.

### pre-release tag
A tag with a hyphen in it, whose release publishes only the release page, the native
binaries and the formula.

### release tag
A tag without a hyphen, whose release publishes everything.

### CI
The checks that run on every change to the repository.

### docs build
The command that checks every documentation page and builds the site.

### coding agent
A model writing code with the skills as its documentation.

## Everyday words

a, an, the, and, or, of, on, for, to, from, with, without, in, into, by, at, as, is, are, was,
were, has, have, had, does, do, not, no, one, two, four, each, every, every, same, other, both,
person, machine, reader, contributor, maintainer, installed, installs, install, installing,
upgrades, upgraded, runs, run, running, prints, printed, says, said, offers, offered, holds,
downloads, downloaded, unpacks, unpacked, verifies, verified, checks, checked, matches, attached,
attaches, replaced, duplicated, updated, touched, untouched, builds, built, fails, failed, passes,
passed, skipped, missing, names, naming, named, identical, byte, bytes, output, command, flag,
flags, arguments, usage, text, message, messages, error, errors, path, version, tagged, tag, day,
later, again, first, before, after, then, when, given, that, which, it, its, their, this, these,
those, there, here, still, also, any, all, none, nothing, something, only, exactly, already, side,
beside, where, how, what, who, whole, works, working, mention, mentioned, sentence, instruction,
instructions, offered, listed, lists, refuses, refused, differs, different, wrong, correct,
running, stays, stay, ships, shipped, shipping, published, attach, pushed, pushes, leg, step,
source, Homebrew, macOS, Linux, Apple, silicon, Intel, x64, arm64, Java, virtual, cluster,
Kubernetes, YAML, API, double, images, SDK, plugin, Python, sample, samples, README, repository,
pages, page, documentation, docs, describe, described, describes, describing, begin, begins,
began, assume, assumes, assumed, tells, told, tell, look, looks, looking, find, finds, found,
reaches, reached, gets, get, got, within, minute, minutes, second, seconds, started, starting,
start, count, cases, case, applied, apply, using, use, uses, used, read, reads, reading, written,
write, writes, writing, work, done, does, did, do, task, rendered, against, executable, through, whose, included, requested, requests, draws,
needs, time, asks, failure, replacement, anything, waits, ankka's, declared, declares, declaring, declaration, implements, tests, test, Scala, Kafka, author,
key, keys, headers, value, values, bytes, unchanged, held, put, chosen, order, count, size, more,
than, default, configured, typed, came, emits, emitted, emitting, fails, failing, failed, recorded,
records, problem, equals, equal, served, serve, serving, port, suite, deployed, starts, beside,
reach, compares, compared, copy, two, shared, gives, give, containers, host, produced, topic, input,
wires, wired, routed, affect, round, trip, manual, fresh, project, naming, compiles, compile,
uploaded, upload, rest, equivalent, coordinates, named, looks, pass, change, build, drift,
lines, line, written, writes, write, reads, read, shows, shown, show, learn, kept, sends, sent,
once, twice, again, nothing, neither, nor, page, pages, registry, empty, schema, difference, reports, committed, keeps, sets, setting, traced, directory, itself, part, else, depends, own,
