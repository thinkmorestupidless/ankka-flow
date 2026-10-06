# Glossary

The words this project's features use, each in exactly one sense. A term marked *Proposed.* has
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

### deploy-time configuration
Settings supplied when a pipeline is deployed rather than written in its blueprint: replicas, a
streamlet's parameters, a Secret's name.

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
*Proposed.* The platform ankka-flow runs beside, whose CLI ships through the same tap.

### ankka-flow
*Proposed.* This project: the pipelines, the sidecar, the operator and `flow`.

### CLI
*Proposed.* A command a person runs on their own machine: `flow`, or ankka's.

### flow
*Proposed.* The command a person runs to verify a blueprint, generate a resource, reset a pipeline
and print its version.

### JVM build
*Proposed.* `flow` as it is built from source and run on a Java virtual machine.

### native binary
*Proposed.* `flow` as one executable for one platform, run with no Java virtual machine.

### platform
*Proposed.* An operating system and processor a native binary is built for: macOS on Apple
silicon, macOS on Intel, Linux on x64, Linux on arm64.

Avoid: target, architecture

### release
*Proposed.* What a tagged version of ankka-flow publishes: images, the Python SDK, the plugin, and
the archives.

### archive
*Proposed.* A compressed file on a release holding one native binary, named for the version and the
platform.

### checksum
*Proposed.* The digest published beside an archive, by which the archive is verified.

### tap
*Proposed.* The Homebrew repository `thinkmorestupidless/homebrew-tap`, through which ankka's CLI
and `flow` are installed.

### formula
*Proposed.* A tap's definition of one installable: its version, its archives and their checksums.

### smoke script
*Proposed.* A script that runs a native binary and asks it for each thing a native build can
silently lose, failing with the name of the first thing missing.

### suite
*Proposed.* The CLI's own tests, which run against the JVM build or the native binary.

### checkout
*Proposed.* A copy of the repository on a machine, at one commit.

### dirty tree
*Proposed.* A checkout with changes not committed; a release refuses to build from one.

### build task
*Proposed.* The command that builds the JVM build from source.

### install page
*Proposed.* The documentation page that says how to get `flow`.

### guide
*Proposed.* A documentation page that walks a reader through doing something with ankka-flow.

### contributing page
*Proposed.* The documentation page for people changing ankka-flow itself.

### skill
*Proposed.* A rendering of the documentation for a coding agent, published with the plugin.

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
needs, time, asks, failure, replacement, anything, waits, ankka's
