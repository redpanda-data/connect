# Connect reference docs

This directory is an Antora content source. The docs site build merges it with [rp-connect-docs](https://github.com/redpanda-data/rp-connect-docs) into one Antora component, `connect`, which is published at https://docs.redpanda.com/redpanda-connect/.

Everything under `modules/` is generated from the component specs. Don't edit these files by hand. To change what a field or component page says, edit the spec in `internal/impl/` (or the shared field helpers in benthos) and regenerate with the same command CI runs:

```bash
CGO_ENABLED=1 TAGS=x_benthos_extra task docs
```

The `x_benthos_extra` tag includes the components that need external C libraries, such as zmq4 (install `libzmq` first). Without it, the generator keeps existing files instead of clearing its directories, so docs for removed components aren't pruned locally. CI fails the PR if the committed output differs from a full run.

## What lives where

| Content | Repo | Owner |
|---|---|---|
| Field reference, examples, metadata, and descriptions (`modules/components/partials/`) | connect | Generated from specs |
| Common and Advanced config snippets (`modules/components/examples/`) | connect | Generated from specs |
| Bloblang function and method reference (`modules/components/partials/bloblang-*`) | connect | Generated from specs |
| Component pages, guides, cookbooks, navigation, and the component catalog | rp-connect-docs | Docs team |

Each component page in rp-connect-docs is written by hand and includes the generated partials from this directory. Keeping the reference data here means it changes in the same pull request as the code it describes. Keeping the pages in rp-connect-docs lets writers add context, such as prerequisites, tutorials, and Redpanda Cloud notes, without editing Go.

Redpanda Cloud docs reuse the same pages, so the generated partials wrap self-managed-only text, such as version notes, in `ifndef::env-cloud[]`.

## Rules for this directory

- Keep `modules/` limited to `partials/` and `examples/`. The docs build ignores anything else from this directory, so a page added here never publishes.
- Keep `antora.yml` to `name` and `version` only. Antora merges this file with the rp-connect-docs one, and a `title` set here replaces the title on every page of the published component.
- Hand-written guides, such as migration guides, belong in rp-connect-docs.
