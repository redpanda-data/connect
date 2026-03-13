#!/usr/bin/env bun
// gen_info.ts — Generate info.json from an OpenAPI spec.
//
// Produces a JSON file with:
//   - doc_urls: mapping of "METHOD /path" → documentation URL
//   - ignored: list of "METHOD /path" keys to skip during code generation
//
// Usage: bun run gen_info.ts <openapi.json> <base-url> <output.json>
//
// Example:
//   bun run gen_info.ts openapi.json https://developers.notion.com/reference info.json

import { readFileSync, writeFileSync } from "fs";

const [specPath, baseURL, outputPath] = process.argv.slice(2);

if (!specPath || !baseURL || !outputPath) {
  console.error(
    "Usage: bun run gen_info.ts <openapi.json> <base-url> <output.json>"
  );
  process.exit(1);
}

const spec = JSON.parse(readFileSync(specPath, "utf-8"));

// Normalize a path by replacing all {param} segments with {}.
function normalizePath(path: string): string {
  return path
    .replace(/\{[^}]+\}/g, "{}")
    .replace(/\/+$/, "");
}

const docURLs: Record<string, string> = {};
const ignored: string[] = [];

for (const [path, methods] of Object.entries(spec.paths ?? {})) {
  for (const [method, op] of Object.entries(methods as Record<string, any>)) {
    if (typeof op !== "object" || !op.operationId) continue;
    const key = `${method.toUpperCase()} ${normalizePath(path)}`;
    docURLs[key] = `${baseURL.replace(/\/+$/, "")}/${op.operationId}`;

    // Auto-detect endpoints to ignore.
    const requestContent = op.requestBody?.content ?? {};
    const contentTypes = Object.keys(requestContent);
    const isNonJSON =
      contentTypes.length > 0 && !contentTypes.includes("application/json");
    const isOAuth = path.includes("/oauth/");

    if (isNonJSON || isOAuth) {
      ignored.push(key);
    }
  }
}

const sortedURLs = Object.fromEntries(
  Object.entries(docURLs).sort(([a], [b]) => a.localeCompare(b))
);

const result = {
  doc_urls: sortedURLs,
  ignored: ignored.sort(),
};

writeFileSync(outputPath, JSON.stringify(result, null, 2) + "\n");
console.log(
  `Wrote ${Object.keys(sortedURLs).length} doc URLs, ${ignored.length} ignored to ${outputPath}`
);
