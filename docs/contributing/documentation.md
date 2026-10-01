---
title: Writing documentation
description: How ankka-flow's documentation is built with ankka's docs tool, where a page goes, the rules every page follows, and how the skills are rendered and published.
kind: contributing
related: [reference/glossary.md]
---

# Writing documentation

ankka-flow's documentation is one tree of plain Markdown under `docs/`, and every way of reading it is a
rendering of that tree: the site, `llms.txt`, `llms-full.txt`, a Markdown copy of each page,
`docs-index.json`, and the agent skills in the marketplace plugin. The tool that renders and checks it
is ankka's, taken as a dependency, and the rules are ankka's too; the page
[Writing documentation](https://docs.ankka.cloud/contributing/documentation/) in ankka's documentation
states every rule with its reason. This page says what is particular to this repository.

## The build

```bash
uv run --project tools/docs docs check    # every rule; exits 1 on a problem
uv run --project tools/docs docs sync     # refresh included samples, the generated table and the skills
uv run --project tools/docs docs build    # check, build the site, write the machine renderings
uv run --project tools/docs docs serve    # the site with live reload, while writing
```

`just docs`, `just docs-sync` and `just docs-serve` are the same commands. The site lands in
`target/docs-site`. The tool comes from ankka's repository, named in `tools/docs/pyproject.toml` and
pinned to a commit by `tools/docs/uv.lock` (`uv lock --upgrade --project tools/docs` moves it). What
it needs to know about this repository is `extra.docs` in `mkdocs.yml`: the frontmatter vocabulary,
where the skills are rendered, and which directory the protocol table is generated from.

## Where a page goes

| Kind | Directory | The reader wants to |
|---|---|---|
| `tutorial` | `get-started/` | be walked from nothing to something working |
| `concept` | `concepts/` | understand how something works and why |
| `guide` | `build/`, `deploy/` | get one task done |
| `reference` | `reference/` | look one fact up |
| `contributing` | `contributing/` | change ankka-flow itself |

A new page is added to the `nav` in `mkdocs.yml` and to at least one skill's `pages:` list, or the
check fails.

## Frontmatter

Every page starts with `title`, `description` and `kind`. One optional key is this repository's:
`languages`, currently only `python`, for a page that shows streamlet code. `related` lists the pages
a reader most often needs next.

## Rules that bite

- **A page stands alone and tells no history.** A model most often reads one page, retrieved alone.
  Nothing that assumes the reader arrived from another page, no feature or task numbers, no
  specification paths; `docs check` refuses them. The
  specification and plan under `specs/` and the design notes under `notes/` are records of how the
  system came to be, not pages.
- **Links between pages are relative; anything else is a URL.** A link to a repository file is its
  GitHub URL, so it works from the site, from a skill and from `llms-full.txt` alike.
- **Every fence names its language**, `text` for output and diagrams.

## Samples come from tested code

A code block preceded by `<!-- include: path#region -->` is filled from a region marked
`# docs:start region` and `# docs:end region` in a source file, and the check fails when the copy has
drifted. Without `#region` the whole file is included. The regions this documentation includes are in
three samples — the cart router, the checkout feed and the checkout graph (each one's streamlet source
and its tests) — and the samples' blueprints, Dockerfiles and configuration files are included whole,
so every sample shown is one the build runs. Markers never go in `protocol/`, which the SDKs copy byte
for byte.

## The generated table

The RPC table on [Streamlet protocol](../reference/protocol.md) is generated from the `.proto` files in
`protocol/src/main/protobuf/ankka/flow/v1` by `docs sync`; the prose around it is written by hand.

## The skills

Four skills are curated under `tools/docs/skill/`, one `SKILL.md` each, and rendered into
`marketplace/plugins/ankka-flow/skills/`, which is committed and checked. The rendered plugin is what
the ankka marketplace publishes as `ankka-flow`: on every tag, the release workflow clones the
marketplace repository, replaces `plugins/ankka-flow/` and ankka-flow's entry in its manifest, and
pushes, leaving every other project's plugin as it found it. It needs a `MARKETPLACE_REPO_TOKEN`
secret with write access to `thinkmorestupidless/ankka-marketplace`.

## The site

The docs workflow builds the site on every pull request that touches the documentation and publishes
it to GitHub Pages from `main`, at `flow.ankka.cloud`: a custom domain set in the repository's Pages
settings, with a DNS CNAME to the GitHub Pages host. Pages is enabled once, with "GitHub Actions" as
its source.
