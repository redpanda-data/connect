# Notion API Code Generation

Generates a typed Go client for the [Notion API](https://developers.notion.com) from a Postman collection enriched with types from the official [Notion JS SDK](https://github.com/makenotion/notion-sdk-js).

## Prerequisites

```bash
brew install bun
cd procgen && bun install
```

## Pipeline

```
task download   -> schema/api-endpoints.ts   (from Notion JS SDK)
task openapi    -> openapi.yaml              (Postman + TS SDK types merged)
task notionapi  -> notionapi/*.go            (ogen Go client)
```

Run `task notionapi` to execute the full pipeline (dependencies are automatic).

## Files

| Path | Purpose |
|---|---|
| `postman_collection.json` | Exported Postman collection for Notion API |
| `openapi.yaml` | Generated OpenAPI spec (Postman + TS SDK schemas) |
| `schema/api-endpoints.ts` | Type definitions from the Notion JS SDK |
| `schema/ts-to-openapi.ts` | Script to extract TS types into OpenAPI schemas |
| `notionapi/` | Generated Go client (ogen) |
| `api_test.go` | Smoke tests for the generated client |

## Updating the Postman collection

See [procgen README](../README.md#exporting-a-postman-collection) for export instructions. Save as `postman_collection.json` and run `task notionapi`.
