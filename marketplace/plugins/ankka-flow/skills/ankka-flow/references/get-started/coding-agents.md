# Work with a coding agent

> Give a coding agent this documentation as skills from the ankka marketplace, or as llms.txt and Markdown pages, and know what each skill carries.

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

## Without the plugin

`llms.txt` at the site root lists every page with a one-sentence description, and `llms-full.txt` holds
the whole documentation in one file. Each page is served as Markdown at its own path with a `.md`
suffix, so `https://flow.ankka.cloud/concepts/sidecar.md` is the Markdown of
[The sidecar](../concepts/sidecar.md). `docs-index.json` lists every page with its title, description,
kind and headings, for a tool that wants to choose pages itself.
