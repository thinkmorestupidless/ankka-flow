# Work with a coding agent

> Give a coding agent this documentation as skills from the ankka marketplace and flow's abilities as tools through flow mcp — connecting Claude, naming the one cluster the tools may touch, and the loop from a change to a running pipeline.

Source: https://flow.ankka.cloud/get-started/coding-agents/
This documentation is rendered into agent skills: directories a coding agent loads on demand, each with
a `SKILL.md` saying when to use it and the pages it carries as reference files. They are published as
the `ankka-flow` plugin of the ankka marketplace, beside ankka's own plugin, so an agent building a
pipeline next to an ankka service can hold both.

## Install the plugin

In Claude Code, add the marketplace once and install the plugin:

```text
/plugin marketplace add thinkmorestupidless/ankka-marketplace
/plugin install ankka-flow@ankka
```

The plugin's version is the ankka-flow release whose documentation it carries.

## The skills

| Skill | Load it when the task is |
|---|---|
| `ankka-flow` | designing a pipeline, writing a blueprint, deciding between an ankka consumer and a flow, or anything else about how ankka-flow behaves |
| `ankka-flow-scala` | a streamlet in Scala: its code, its tests with the Harness, its descriptor, its image, running it on a laptop |
| `ankka-flow-python` | a streamlet in Python: its code, its tests with the Harness, its descriptor, its image, running it on a laptop |
| `ankka-flow-deploy` | installing the platform, `flow verify`, `flow generate`, `flow reset`, the `AnkkaFlow` resource, the operator, lag, stalls and a pipeline that is not `Ready` |
| `ankka-flow-protocol` | implementing the streamlet protocol in a language with no SDK, or debugging what a process sends the sidecar |

Each skill's body holds the rules an agent must keep for that task, the questions to settle before
writing, and the mistakes to check for. The pages under `references/` are the same Markdown as this
site, so a correction made here reaches the skills at the next release.

## The MCP server

`flow mcp` is a Model Context Protocol server over standard input and output: a coding agent connected
to it has `flow`'s abilities as tools, each described with its inputs and marked read-only or not, and
this documentation as resources. It serves the pages of the `flow` version it is, so the samples an
agent reads match the commands it drives.

| Group | Tools | Acts on |
|---|---|---|
| Without a cluster | `verify_blueprint`, `generate_resource`, `flow_version`, `search_docs`, `read_doc` | files in the project and the documentation; nothing is applied anywhere |
| The named cluster, read | `list_pipelines`, `get_pipeline`, `pipeline_logs`, `pipeline_lag` | the pipelines of one namespace on one cluster: phase, status, events, a streamlet's process or sidecar log, consumer lag per inlet partition |
| The named cluster, changed | `apply_pipeline`, `reset_pipeline` | the same cluster: create or update this pipeline's resource; request a reset, with `flow reset`'s guards |

`verify_blueprint` answers exactly what `flow verify` prints, refusals included, and `generate_resource`
answers the YAML `flow generate` writes without applying it. Every page of this documentation is a
resource at `ankka-flow://docs/<path>`, and `search_docs` ranks pages by the words of a query.

A tool that cannot do what it was asked answers with an error the agent can read — a blueprint that
does not verify, a pipeline that does not exist, a cluster that cannot be reached — and the session
continues.

### The named cluster

The cluster tools touch one cluster: the one `flow.toml` names, beside the project, and never the
context `kubectl` happens to point at. `flow init` writes it with the kind cluster
[Deploy to a local cluster](deploy-locally.md) sets up and the project's own namespace:

```toml
context   = "kind-ankka"
namespace = "greeter"
```

Change both for another cluster. `flow mcp` reads the file from the directory it is started in, which
for Claude Code is the project's root. Without the file, or with either key missing, every cluster tool
refuses and says what to write there; the tools that need no cluster and the documentation still work.

`apply_pipeline` and `reset_pipeline` carry the protocol's destructive hint, so a client asks you before
running them; the other nine are marked read-only. A reset is refused while a target streamlet still
runs, exactly as on the command line.

### Connect Claude to it

| You use | Run | What it changes |
|---|---|---|
| Claude Code, in a project from `flow init` | nothing | the project's `.mcp.json` already names the server |
| Claude Code, in every project | `flow mcp install` | Claude Code's own configuration, for you, through `claude mcp add --scope user` |
| Claude Code, in one existing project, for everyone who works on it | `flow mcp install --scope project` | a `.mcp.json` in the project, to commit |
| Claude Desktop | `flow mcp install --client desktop` | Claude Desktop's `claude_desktop_config.json`; quit and reopen Desktop afterwards |

**Claude Code asks before starting a project's server.** A `.mcp.json` is part of the repository, so
Claude Code asks each person once whether to trust it. `flow init` leaves that question in place rather
than answering it for you.

**The project file names `flow`; Desktop's names a path.** A `.mcp.json` is read on other machines, so
it names the command and relies on `PATH`. Claude Desktop is started from the Dock rather than a shell,
so `flow mcp install --client desktop` writes the absolute path of the first `flow` on yours, and for
the JVM build a `JAVA_HOME`. `--command` names a different `flow`.

**Nothing is overwritten.** Every write keeps the other servers and settings in the file, and an
existing server named `ankka-flow` is left as it is and shown to you; `--force` replaces it. `--dry-run`
prints what would change and changes nothing. When the `claude` command is not on `PATH`, `flow mcp
install` changes nothing and prints the command to run.

Any other MCP client starts the server the same way. A client that takes a JSON configuration names
the command and its argument:

```json
{
  "mcpServers": {
    "ankka-flow": { "command": "flow", "args": ["mcp"] }
  }
}
```

Started by hand at a terminal, `flow mcp` prints a line on standard error saying it is waiting for a
client, and then waits: its standard output carries protocol messages and nothing else. Press Ctrl-D to
stop it. An MCP client starts it over pipes, and then it prints nothing but the protocol.

### The development loop, for an agent

With the skills and the server, an agent can take a change from code to a running, observed pipeline:

1. Read the page for what it is writing, with `read_doc` or from the skill — the samples there are
   compiled and tested.
2. Change the streamlet and its test, and run the project's tests.
3. Rewrite and check the descriptor (`sbt descriptorCheck`, or `uv run descriptor --check`) after any
   change to a port, a parameter or the name.
4. `verify_blueprint` on the project's blueprint and descriptor; fix every refusal before going on.
5. Build the image and load it where the named cluster can pull it, then `apply_pipeline`.
6. `get_pipeline` until the phase is `Ready`; `pipeline_logs` for the process or the sidecar when it
   is not, and `pipeline_lag` when records are not keeping up.
7. `reset_pipeline` only when the inputs must be read again from the start, with the target streamlets
   scaled to zero first.

## Without the plugin

`llms.txt` at the site root lists every page with a one-sentence description, and `llms-full.txt` holds
the whole documentation in one file. Each page is served as Markdown at its own path with a `.md`
suffix, so `https://flow.ankka.cloud/concepts/sidecar.md` is the Markdown of
[The sidecar](../concepts/sidecar.md). `docs-index.json` lists every page with its title, description,
kind and headings, for a tool that wants to choose pages itself.
