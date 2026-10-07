# Feature Specification: `flow mcp` — The Platform as Tools for a Coding Agent

**Feature Branch**: `007-flow-mcp`

**Created**: 2026-10-07

**Status**: Draft

**Input**: User description: "Copy ankka's `ankka mcp` command so that Claude can communicate with
the MCP server when the project is being worked on via Claude: `flow mcp` serves ankka-flow's
documentation and `flow`'s abilities — verify a blueprint, generate a resource, see a pipeline on a
cluster and its logs and lag, apply and reset one — to a coding agent over the Model Context
Protocol, and `flow init` writes the project file that connects Claude Code to it."

## Context

A project written by `flow init` carries the ankka-flow skills, so a coding agent opened in it
knows the platform's rules and pages. It cannot yet *do* anything with the platform: to verify a
blueprint, read a pipeline's status or its sidecar's logs, the agent shells out to `flow` and
`kubectl` and parses what they print, with no description of what each command does, what it
needs, or whether it is safe to run.

ankka answers this for its platform with `ankka mcp`: the CLI serves a Model Context Protocol
server over standard input and output, exposing the documentation as resources and the platform's
operations as tools, each described with its inputs and marked read-only or destructive so a client
can ask before changing anything. `ankka init` writes the `.mcp.json` that connects Claude Code to
it with no configuration, and `ankka mcp install` connects other clients.

`flow mcp` is the same for ankka-flow. What differs is the platform: `flow` acts on a Kubernetes
cluster through a kubeconfig, not on a control plane with a login and roles, so what the server is
allowed to touch — and on which cluster — has to be said explicitly, or an agent working in a
project would act on whatever cluster the developer's shell happened to point at.

## Clarifications

### Session 2026-10-07

- Q: How does a project name the cluster its tools may touch? → A: A project file, `flow.toml`, that `flow init` writes with the kind defaults (`context = "kind-ankka"`, `namespace = "<project name>"`) and `flow mcp` reads from the directory it is started in, so every client gets the same cluster; without the file, or with it empty, every cluster tool refuses.
- Q: Which cluster reads does the server offer? → A: All four — list, get (status, conditions, events), logs of the process or sidecar container, and lag — with lag read from the sidecar's metrics endpoint through the pod, as the observe page has a person do by hand.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - The documentation and the pure commands, as tools (Priority: P1)

A coding agent connected to `flow mcp` can search and read ankka-flow's documentation of the
`flow` version it is talking to, verify a blueprint against descriptors, generate a pipeline
resource without applying it, and ask the versions — all without a cluster, a network or any
change to anything.

**Why this priority**: It is what an agent needs most while writing a streamlet and its blueprint,
and it needs nothing outside the project to be safe.

**Independent Test**: A client sends `initialize`, lists the tools and resources, searches the
docs, reads a page, verifies the project's blueprint and generates its resource; every answer is
the same as `flow`'s own output; nothing on disk or in a cluster changes.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/cli/mcp.feature`: the server answers initialize with its tools and resources
- added `features/cli/mcp.feature`: a blueprint is verified through a tool as flow verifies it
- added `features/cli/mcp.feature`: a resource is generated through a tool and nothing is applied
- added `features/cli/mcp.feature`: every documentation page is a resource and a search finds it
- added `features/cli/mcp.feature`: a tool's failure is a tool result, not a protocol error

---

### User Story 2 - A pipeline on a cluster, read (Priority: P1)

An agent can list the pipelines in a namespace, read one pipeline's status, conditions and
events, read a streamlet's process and sidecar logs, and read the consumer lag per inlet — the
things a person asks when a pipeline is not doing what they expected — on the cluster the project
names, and only there.

**Why this priority**: Reading a cluster is the second thing an agent needs and the first that
touches anything outside the project; getting the boundary right here sets it for the writes.

**Independent Test**: With a cluster named for the server, each read tool answers what `kubectl`
shows; with no cluster named, each refuses, saying what to set; none changes anything.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/cli/mcp.feature`: a cluster tool acts only on the cluster the project names
- added `features/cli/mcp.feature`: a pipeline's status, conditions and events are read through a tool
- added `features/cli/mcp.feature`: a streamlet's logs are read through a tool
- added `features/cli/mcp.feature`: a pipeline's lag is read through a tool

---

### User Story 3 - A pipeline on a cluster, changed (Priority: P2)

An agent can apply a generated pipeline resource to the named cluster and request a reset of a
pipeline or one of its streamlets, with each tool marked as changing the cluster so the client
asks the person before calling it, and with `flow`'s own guards — a reset refused while a
streamlet still runs, a resource refused when its blueprint does not verify — applied as they are.

**Why this priority**: The loop an agent runs — change the streamlet, redeploy, watch — closes
only with these; they are last because they change things.

**Independent Test**: Applying the project's resource through the tool creates the pipeline the
same `kubectl apply` would; a reset through the tool is the same request `flow reset` makes; both
tools are marked destructive; both refuse with no cluster named.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/cli/mcp.feature`: a pipeline is applied through a tool as kubectl would apply it
- added `features/cli/mcp.feature`: a reset is requested through a tool as flow requests it
- added `features/cli/mcp.feature`: a tool that changes the cluster says so

---

### User Story 4 - Connected without configuration (Priority: P2)

A project written by `flow init` holds the file that connects Claude Code to `flow mcp`, so an
agent opened in it has the tools at once; `flow mcp install` connects Claude Code for the person
(every project), a project, or Claude Desktop, merging into the client's settings without
disturbing other servers. The coding-agents page and the CLI reference describe it, and the
skills tell an agent what the tools are for.

**Why this priority**: A server nobody is connected to is not used.

**Independent Test**: A project from `flow init` has the connection file naming `flow mcp`; `flow
mcp install` in each of its modes writes or merges what it says and changes nothing else; a dry run
changes nothing; the docs build is clean.

**Acceptance Scenarios** *(each names a scenario in a living feature; none is written here)*:

- added `features/cli/init.feature`: a project connects Claude Code to flow mcp
- added `features/cli/mcp.feature`: flow mcp install connects a client without disturbing its other servers
- added `features/cli/mcp.feature`: the native binary serves the documentation

### Edge Cases

- **No cluster named.** No `flow.toml`, or one naming no context: every cluster tool refuses with
  a message saying what to write in `flow.toml`; the pure tools and the docs still work.
- **`flow mcp` started outside a project.** There is no `flow.toml` where it started, so it is a
  documentation and verification server only, and says so on standard error.
- **A cluster named that the kubeconfig does not hold.** The server starts; each cluster tool
  refuses naming the context it could not find.
- **The cluster is unreachable.** A cluster tool answers with the client's own error as a tool
  result; the server keeps serving.
- **A tool's inputs are wrong.** A missing blueprint path, an unknown pipeline, a namespace that
  does not exist: a tool result with the message `flow` or the cluster gives, never a protocol
  error that would end the session.
- **A client of another protocol version.** The server answers `initialize` with the newest
  version it speaks that the client offered; one it cannot serve is told so.
- **Started at a terminal.** A person who runs `flow mcp` by hand sees a note on standard error
  saying what it is and that a client is expected; standard output carries only the protocol.
- **An existing connection entry.** `flow mcp install` leaves an existing `ankka-flow` entry alone
  unless told to replace it, and never touches another server's.
- **A development build.** The docs served are the ones built into that `flow`, whatever its
  version.

## Requirements *(mandatory)*

### Functional Requirements

**The server**

- **FR-001**: `flow mcp` MUST serve the Model Context Protocol over standard input and output,
  with standard output carrying only the protocol; it MUST answer `initialize` with its tools and
  resources and the newest protocol version it speaks that the client offered, and answer the
  client's pings, listings and calls.
- **FR-002**: Every tool MUST carry a description, an input schema, and hints saying whether it is
  read-only or changes the cluster; a tool's failure MUST be returned as a tool result marked as an
  error, never as a protocol error.

**Tools without a cluster**

- **FR-003**: A tool MUST verify a blueprint against descriptors and deploy-time configuration,
  answering exactly what `flow verify` prints and refuses.
- **FR-004**: A tool MUST generate a pipeline resource from a blueprint, descriptors, images and
  deploy-time configuration, answering the resource `flow generate` writes, without applying it.
- **FR-005**: A tool MUST answer the CLI and protocol versions.
- **FR-006**: Every documentation page of the `flow` version MUST be a resource, and tools MUST
  search the pages and read one.

**Tools on a cluster**

- **FR-007**: A cluster tool MUST act only on the Kubernetes context and namespace the project
  file `flow.toml` names, read from the directory `flow mcp` is started in; with no file, or with
  neither named in it, every cluster tool MUST refuse, saying what to write there, and the server
  MUST still serve everything else.
- **FR-008**: Tools MUST list the pipelines in the namespace with their phase; read one pipeline's
  status, conditions and recent events; read the process or sidecar container's recent logs of one
  streamlet; and read the consumer lag per inlet of one pipeline.
- **FR-009**: A tool MUST apply a generated pipeline resource to the named cluster as `kubectl
  apply` would, after verifying its blueprint, and a tool MUST request a reset of a pipeline or
  named streamlets with `flow reset`'s guards; both MUST be marked as changing the cluster.

**Connecting**

- **FR-010**: A project written by `flow init` MUST hold `.mcp.json` naming `flow mcp`, so Claude
  Code opened in it connects with no configuration, and `flow.toml` naming the kind cluster the
  documentation sets up (`kind-ankka`) and the project's own namespace, so a fresh project's tools
  work there and a person pointing at another cluster edits one file.
- **FR-011**: `flow mcp install` MUST connect Claude Code for the person or for a project, or
  Claude Desktop, by merging one `ankka-flow` entry into the client's settings, leaving every other
  entry as found, leaving an existing `ankka-flow` entry unless told to replace it, and changing
  nothing on a dry run.

**Proof and documentation**

- **FR-012**: A suite MUST drive the server over its protocol: version negotiation, listings, each
  tool against a Kubernetes API double, each refusal, and that every page is a resource.
- **FR-013**: The native smoke script MUST start the binary's server, initialize it and list its
  resources.
- **FR-014**: The coding-agents page MUST describe the server, how to connect to it, and what the
  tools can do; the CLI reference MUST document `mcp` and `mcp install`; the skills MUST tell an
  agent what the tools are for and which change a cluster.

### Key Entities

- **server**: `flow mcp`, serving the protocol over standard input and output for one client.
- **tool**: one operation the server offers, with a description, an input schema and hints.
- **resource**: one documentation page, addressed by its path.
- **named cluster**: the Kubernetes context and namespace the project file `flow.toml` names, the
  only ones the server's cluster tools touch.
- **project file**: `flow.toml` in the project's root, holding the named cluster.
- **connection file**: `.mcp.json`, or the client's settings, naming the server's command.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Every tool's answer for a blueprint, a resource or a version is byte-identical to
  the corresponding `flow` command's output.
- **SC-002**: With no cluster named, no tool call reaches any cluster, and each cluster tool's
  refusal names what to set.
- **SC-003**: An agent in a project from `flow init` lists the server's tools with no step beyond
  opening the project.
- **SC-004**: The server answers `initialize` and a documentation search within one second in the
  native binary.
- **SC-005**: `flow`'s other commands, `flow init`, the SDKs, the sidecar, the operator and the
  protocol pass the suites they pass today.

## Assumptions

- **ankka's server, re-pointed.** The protocol handling — a hand-written server over standard
  input and output with no library, tool hints, docs built in as resources, `install` delegating
  to the client's own command — follows `ankka mcp`; the tools are `flow`'s and the cluster's.
- **The cluster is named in the project.** `flow.toml` holds it (clarified), so every client
  connected to the project's server touches the same cluster; the kubeconfig's current context is
  never used, because an agent would then act on whatever the shell pointed at.
- **Reads through the Kubernetes API.** Status, events and logs come from the cluster as
  `kubectl` reads them; lag is the sidecar's own metric (clarified), scraped from each streamlet
  pod's sidecar through the API server's port-forward, as the observe page has a person do.
- **Apply is `kubectl apply`'s shape.** The tool creates or updates the resource server-side; the
  operator does the rest.
- **The docs are the plugin's pages.** The same Markdown the site and the skills are built from,
  bundled into the CLI at build time.

## Out of Scope

- Prompts, sampling, or a transport other than standard input and output.
- Tools on the operator's installation: installing or upgrading the platform, Kafka clusters,
  secrets.
- Deleting a pipeline, or anything on a cluster the project does not name.
- The MCP server as a long-running service or a web endpoint.
- Any change to the SDKs, the sidecar, the operator or the protocol.
