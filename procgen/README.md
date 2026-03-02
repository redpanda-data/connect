# Postman Collection to Redpanda Connect Processor

## Prerequisites

- Install [bun](https://bun.sh)
- Run `bun install postman-to-openapi`

## Extracting Postman Collection to OpenAPI

- Go to https://www.postman.com and Search product Workspace ex. "Notion"
- Open the Workspace and click on the "Variables" tab
- Click on "Fork Collection" and save it to your workspace
- Click on "..." > "More" > "Export" > "Export as JSON" and save it to `postman_collection.json`
- Run `bunx p2o "./Notion API.postman_collection.json" -f notion_openapi.yml`
