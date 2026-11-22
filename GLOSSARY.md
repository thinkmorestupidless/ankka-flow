# Glossary

The words this project's features use, each in exactly one sense. Where `docs/reference/glossary.md` defines a word for the
people who build on ankka-flow, the term here means the same.

## The platform

### pipeline
A set of streamlets wired by a blueprint over topics, deployed to a cluster as one resource.

### streamlet
One stage of a pipeline, with typed inlets and outlets: logic written against one engine's own
API and run by that engine.

### blueprint
The file that names a pipeline's streamlets and wires their inlets and outlets over topics.

### descriptor
The file a streamlet's SDK writes, saying what the streamlet is: its inlets, outlets and
parameters.

Avoid: manifest, spec

### deploy-time configuration
Settings supplied when a pipeline is deployed rather than written in its blueprint: replicas, a
streamlet's parameters, a Secret's name.

Avoid: config

### resource
The `AnkkaFlow` resource `flow` writes for a pipeline, which a cluster runs.

### reset
A request that a pipeline, or one of its streamlets, read its topics again from the start.

### cluster
The Kubernetes cluster a pipeline is deployed to.

### note
A line `flow` prints beside a verification saying what it decided, such as that a topic will be
compacted.

### refusal
An answer from `flow` that it will not do what was asked, saying why.

## The CLI and its release

### ankka
The platform ankka-flow runs beside, whose CLI ships through the same tap.

### ankka-flow
This project: the pipelines, the SDKs, the operator and `flow`.

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
What a tagged version of ankka-flow publishes: images, the SDKs, the plugin, and
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

Avoid: recipe

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
A library a streamlet is written against, which writes the streamlet's descriptor, gives its
logic its inlets and outlets as the engine's own sources and sinks, and ships a harness. One per
engine.

Avoid: library

### streamlet author
A person writing a streamlet against an SDK.

Avoid: developer, user

### record
What a topic holds and a streamlet sees: bytes, an optional key, and ordered headers.

Avoid: message

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
An SDK's stand-in for Kafka and a cluster in a test: it feeds a streamlet's inlets and records
what the streamlet writes to its outlets.

Avoid: test harness, testkit

### cart event
A record the sample pipeline reads, keyed by cart id, carrying a total and the time it happened.

### laptop walkthrough
The first-streamlet tutorial's run: Kafka in a container, each streamlet a command on the
host, records written and verified, with no cluster.

### CI
The checks that run on every change to the repository.

### coding agent
A model writing code with the skills as its documentation.

### project
The directory a pipeline's code lives in, holding its streamlets, its blueprint and its project
file. (An ankka project is its own term.)

Avoid: scaffold, skeleton

### image
The container image a streamlet runs from, holding its code, its SDK and what its engine needs.

Avoid: docker image

### tutorial
A documentation page that takes a reader through a first task from the start.

Avoid: walkthrough

## The server

### server
The Model Context Protocol server "flow mcp" runs, serving the documentation and flow's abilities to
one coding agent over standard input and output.

Avoid: daemon

### tool
One operation the server offers a coding agent, with a description, an input schema and whether it
changes a cluster.

Avoid: function, action

### resource
In the server, one documentation page addressed by its path. (A pipeline's Kubernetes resource is
the resource of the platform section.)

### client
The coding agent's side of the Model Context Protocol: what connects to the server, such as Claude
Code or Claude Desktop.

### named cluster
The Kubernetes context and namespace the project file names: the only cluster "flow mcp"'s tools
touch.

### project file
"flow.toml" in a project's root, naming the cluster the project's tools may touch.

## Engines and ankka

### engine
What runs a streamlet's logic: Flink or Pekko Streams. A descriptor names its
streamlet's engine.

Avoid: runtime, runner, backend

### Flink streamlet
A streamlet written against Flink's own API and run as a Flink job; the engine for a
stage that needs windows or state.

### Pekko Streams streamlet
A streamlet written as a Pekko Streams flow; the engine for a stage that needs
neither windows nor state.

### Pekko Streams SDK
The SDK for Pekko Streams streamlets, written against from Scala.

### Flink SDK
The SDK for Flink streamlets, written against from Java.

### stage
One step of a pipeline's work as a person thinks of it, before it is a streamlet or
an ankka consumer.

### state
What a Flink streamlet remembers between records, kept by Flink in the cluster's
state store.

Avoid: checkpoint, savepoint

### state store
The durable storage a cluster gives Flink streamlets for their state.

### window
A span of time into which a Flink streamlet groups records, by the time each record
carries.

### Flink operator
The Flink Kubernetes Operator, which a cluster must have before it runs a Flink
streamlet. Not ankka-flow's own operator.

### condition
A line of a pipeline's status saying one thing that is true of it, such as what it
lacks.

### Ready
The phase of a deployed pipeline whose every streamlet is running and reading.

### lag
How far a streamlet's reading of a topic is behind what has been written to it.

### contract
What a topic carries: a name and, where given, the fingerprint of a schema document.
An inlet, an outlet and an ankka topic each declare one, and the sides of a topic must declare
the same.

Avoid: message shape

### managed
Of a topic: created by the pipeline that names it.

### ankka project
A project in ankka: the unit that declares topics.

Avoid: ankka application

### ankka topic
A topic an ankka project declares, which a blueprint names by the project and the
declared name, such as "checkout/cart-checkouts". A pipeline reads or writes it and never creates
it.

Avoid: ankka stream, service topic

### ankka consumer
A component of a service in an ankka project that reads one topic; what a stage is
when it is one service reacting to one topic and not a pipeline.

### declarations
An ankka project's topics and their contracts, as a file a person exports from ankka
and gives to flow.

### machine
The identity a pipeline has to ankka: a registered machine of an ankka organization,
named by the pipeline's deploy-time configuration.

Avoid: service account

### grant
An ankka project's permission for a machine to read or write one topic. Made in
ankka, never by flow.

### right
What a grant permits: reading or writing.

### engines page
The documentation page that says which engine to choose for a stage.

### limitations page
The documentation page that says what ankka-flow does not do.

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
once, twice, again, nothing, neither, nor, page, pages, registry, empty, schema, difference, reports, committed, keeps, sets, setting, traced, directory, itself, part, else, depends, own, capital, letter, tests, edited, language, differs, visible, input, output, problem, sixty-three, characters, holds, files, package, takes, opened, exposes, port, way, through, goes, see, longer, follows, object, set, JSON, starting, digit, may, id, client, initializes, initialized, session, newest, offered, schema, answers, answer, among, Markdown, continues, reached, phase, conditions, recent, container, asked, consumer, lag, created, updated, sent, draws, marked, read-only, settings, entry, dry, run, Claude, Code, touch, Protocol, Model, Context, abilities, kubectl, apply, applied, verified, verifies, calls, namespace, searches, title, exist, result, logs, lines, Desktop, connects, connected, speaks, description, generate, verify, reset, images, list, get, sets, up, kind,
never, made, makes, registered, lacks, binds, method, carried, carries, delivery, between, pod, pods, knows, new, discarded,
whatever, stopped, laptop, remembers,
