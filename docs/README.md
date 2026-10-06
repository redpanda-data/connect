# Connect reference docs

The reference docs for Redpanda Connect components and Bloblang are generated from the specs in this repo and published at https://docs.redpanda.com/redpanda-connect/. They aren't committed. To change what a field or component page says, edit the spec in `internal/impl/` (or the shared field helpers in benthos).

## How they're published

1. `cmd/tools/docs_gen` writes Antora partials and examples to `docs/modules/components/`, which is gitignored.
2. When a release is tagged, the release workflow runs the generator and attaches the output to the GitHub release as `redpanda-connect-docs.tar.gz`.
3. The docs site build downloads that asset for the latest release and merges it into the `connect` Antora component, alongside the pages in [rp-connect-docs](https://github.com/redpanda-data/rp-connect-docs).

The published reference therefore always matches a released version, never unreleased code on `main`.

## Generating them locally

Run the same command CI runs:

```bash
CGO_ENABLED=1 TAGS=x_benthos_extra task docs
```

The `x_benthos_extra` tag includes the components that need external C libraries, such as zmq4 (install `libzmq` first). Without it, the generator keeps existing files instead of clearing its directories, so docs for removed components aren't pruned locally.

`task docs` also lints every generated config snippet against the schema. CI runs it on every pull request, so a spec change that breaks the generated docs fails the PR.

To preview your changes in a docs build, point the docs build at your generated output with `REDPANDA_CONNECT_DOCS_DIR=/path/to/connect/docs`.

## What lives where

| Content | Repo | Owner |
|---|---|---|
| Field reference, examples, metadata, and descriptions (`modules/components/partials/`) | connect | Generated from specs |
| Common and Advanced config snippets (`modules/components/examples/`) | connect | Generated from specs |
| Bloblang function and method reference (`modules/components/partials/bloblang-*`) | connect | Generated from specs |
| Component pages, guides, cookbooks, navigation, and the component catalog | rp-connect-docs | Docs team |

Each component page in rp-connect-docs is written by hand and includes the generated partials. Keeping the reference data in the specs means it changes in the same pull request as the code it describes. Keeping the pages in rp-connect-docs lets writers add context, such as prerequisites, tutorials, and Redpanda Cloud notes, without editing Go.

Redpanda Cloud docs reuse the same pages, so the generated partials wrap self-managed-only text, such as version notes, in `ifndef::env-cloud[]`.
