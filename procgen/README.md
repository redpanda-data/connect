# procgen

Code generation for third-party HTTP API clients used by Redpanda Connect processors.

## Structure

Each subdirectory targets a specific API:

| Directory | API | Output |
|---|---|---|
| `notion/` | [Notion API](https://developers.notion.com) | `notion/notionapi/` |

## Shared tooling

- `ogen.yaml` - Shared [ogen](https://github.com/ogen-go/ogen) configuration for OpenAPI code generation
- `tools.go` - Go tool dependencies (build-tagged `tools`)

Each subdirectory has its own `Taskfile.yml` and `README.md` with API-specific setup.

## Adding a new API

1. Create a new directory: `procgen/<api>/`
1. Go to https://www.postman.com and search for the product workspace (e.g. "Notion")
1. Fork the collection to your workspace
1. Export as JSON ("..." > "More" > "Export" > "Export as JSON")
1. Save as `<api>/postman_collection.json`
1. Add a `Taskfile.yml` with tasks for schema download, OpenAPI generation, and Go client generation
1. Reference `../ogen.yaml` for shared ogen config (or add API-specific overrides)
1. Add a `README.md` documenting the pipeline
