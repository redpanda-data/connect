# Connect reference docs

The reference docs for Redpanda Connect components and Bloblang are generated from the specs in this repo and published at https://docs.redpanda.com/connect/. They aren't committed. To change what a field or component page says, edit the spec in `internal/impl/` (or the shared field helpers in benthos). The first line of every generated partial names the file that registers its component or Bloblang function, for example `redpanda-data/benthos@v4.84.0:internal/impl/pure/processor_mapping.go`, so a reader of the docs or of their Markdown export can tell which repository and file the text comes from. A component registered in a loop or from a struct field keeps a repository-level banner; the generator lists those when it runs.

## How they're published

1. `cmd/tools/docs_gen` writes Antora partials and examples to `docs/modules/components/`, which is gitignored.
2. When a release is tagged, the release workflow runs the generator, builds the docs site with the output, and, if that build passes, attaches the output to the GitHub release as `redpanda-connect-docs.tar.gz`. A release whose docs fail the build gets no asset, so the docs site keeps the previous release's. The asset also carries the release's connector data, `attachments/connect-<version>.json`, which the Bloblang playground and other site tools read.
3. The docs site build downloads that asset for the latest release and merges it into the `connect` Antora component, alongside the pages in [rp-connect-docs](https://github.com/redpanda-data/rp-connect-docs).

The published reference therefore always matches a released version, never unreleased code on `main`.

## Generating them locally

Run the same command CI runs:

```bash
CGO_ENABLED=1 TAGS=x_benthos_extra task docs
```

The `x_benthos_extra` tag includes the components that need external C libraries, such as zmq4 (install `libzmq` first). Without it, the generator keeps existing files instead of clearing its directories, so docs for removed components aren't pruned locally.

`task docs` also lints every generated config snippet against the schema, and CI runs it on every pull request.

## Checks on pull requests

On every pull request from a branch in this repo that isn't a draft, the `docs-check` job builds the Connect docs with the PR's generated docs and compares them with the PR's merge base:

- It fails on Antora errors from the generated docs, and on broken anchors, broken links, or unconverted AsciiDoc on the pages the PR changes. Problems already on the published site are listed in the job summary but don't fail the PR.
- When the PR changes a published page, it posts one comment that lists the changed pages with links to a preview of the rendered docs, adds the `documentation` label, and asks the docs team to review. Review the rendered pages, not only the Go strings.

To preview your changes in a docs build, point the docs build at your generated output with `REDPANDA_CONNECT_DOCS_DIR=/path/to/connect/docs`.

## What lives where

| Content                                                                                | Repo            | Owner                |
|----------------------------------------------------------------------------------------|-----------------|----------------------|
| Field reference, examples, metadata, and descriptions (`modules/components/partials/`) | connect         | Generated from specs |
| Common and Advanced config snippets (`modules/components/examples/`)                   | connect         | Generated from specs |
| Bloblang function and method reference (`modules/components/partials/bloblang-*`)      | connect         | Generated from specs |
| Component pages, guides, cookbooks, navigation, and the component catalog              | rp-connect-docs | Docs team            |

Each component page in rp-connect-docs is written by hand and includes the generated partials. Keeping the reference data in the specs means it changes in the same pull request as the code it describes. Keeping the pages in rp-connect-docs lets writers add context, such as prerequisites, tutorials, and Redpanda Cloud notes, without editing Go.

## Writing doc strings

Every doc string in a spec (`Summary`, `Description`, `Footnotes`, field descriptions, annotated option summaries, example summaries, and Bloblang docs) is AsciiDoc, and it is published in four places:

| Surface | How it is rendered | Who sets `env-cloud` |
|---|---|---|
| Self-managed docs site (rp-connect-docs) | `docs_gen` partials, built by Antora | Nobody |
| Redpanda Cloud docs site (cloud-docs) | The same partials, built by Antora | cloud-docs `antora.yml` |
| `redpanda-connect list --format json-full --docs-format markdown` | Markdown, rendered by the binary | Nobody: `schema.Standard()` declares no attributes |
| `redpanda-connect-cloud` and `-ai` `list ... --docs-format markdown`, which the Redpanda Cloud UI shows | Markdown, rendered by the binary | The distribution: `schema.Cloud()` and `schema.CloudAI()` declare `env-cloud` |

The rule that keeps those four in step: a distribution declares the attributes its readers have on its schema in `public/schema/schema.go`, and the docs site for that distribution sets the same attributes. Nothing else decides what Cloud readers see. The same attributes pick the docs site that the binary's Markdown links point at (`docslinks.MarkdownOptions` in `internal/docslinks`): a schema that declares `env-cloud` links to the Cloud copy of the Connect docs.

Links are written as `xref:components:inputs/kafka.adoc[text]`, `xref:configuration:batching.adoc[text]`, or `<<field_name>>`, always with link text. They resolve on every surface. A link to a component that a distribution does not ship renders as plain text there, on the docs site and in the binaries, so you never need a conditional just to hide a link. Link to other docs sites with a component-qualified xref, such as `xref:streaming:manage:x.adoc[text]`. Don't write a `docs.redpanda.com` URL: it points every distribution at one copy of the page, and `go test ./cmd/tools/docs_gen/` (`TestDocStringsLinkDocsWithXrefs`) rejects it. The API reference under `docs.redpanda.com/api/` is not part of the docs site, so it keeps its URL.

Use a conditional only for text about behaviour that differs in Redpanda Cloud. Wrap one whole paragraph, with the directive at column 0 and a blank line before and after:

```go
Description(`
Sends data directly to inputs in the same process by a unique ID.

ifndef::env-cloud[]
This links isolated streams in xref:guides:streams_mode/about.adoc[streams mode],
which Redpanda Cloud does not run.
endif::[]

It is possible to connect multiple inputs to the same ID.`)
```

`ifdef::env-cloud[]` is for text that applies only in Cloud. `env-cloud` is the only attribute. The single-line form `ifndef::env-cloud[text]` and a conditional that touches the paragraphs around it are rejected by `go test ./cmd/tools/docs_gen/` (`TestDocStringConditionalsAreWellFormed`), because a malformed conditional makes a renderer drop text without any error. A conditional inside a component `Summary` is rejected too (`TestComponentSummariesAreOneLine`), because summaries render as one line in catalogs and page metadata. The same tests render every distribution's doc strings as Markdown and fail on anything left unconverted.

Version notes and availability notes are generated by `docs_gen`, which wraps them in `ifndef::env-cloud[]` itself.
