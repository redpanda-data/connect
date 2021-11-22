#!/bin/sh
curl -s \
  -X POST "http://localhost:8081/subjects/benthos_example/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "$(cat blob_schema.json | jq '{schema: . | tostring}')" \
  | jq
