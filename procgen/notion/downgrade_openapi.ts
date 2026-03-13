#!/usr/bin/env bun
// downgrade_openapi.ts — Downgrade an OpenAPI 3.1 spec to 3.0 and flatten
// complex schema patterns for ogen compatibility.
//
// Handles:
// - type: ["string", "null"] → type: "string", nullable: true
// - exclusiveMinimum/Maximum numeric → boolean form
// - Remove webhooks (3.1-only)
// - Flatten oneOf/anyOf/allOf into merged object schemas
//
// Usage: bun run downgrade_openapi.ts <input.json> <output.json>

import { readFileSync, writeFileSync } from "fs";

const [inputPath, outputPath] = process.argv.slice(2);

if (!inputPath || !outputPath) {
  console.error(
    "Usage: bun run downgrade_openapi.ts <input.json> <output.json>"
  );
  process.exit(1);
}

type Schema = Record<string, any>;

// ---------------------------------------------------------------------------
// Phase 1: 3.1 → 3.0 downgrade (in-place)
// ---------------------------------------------------------------------------

function downgrade(obj: any): void {
  if (obj == null || typeof obj !== "object") return;
  if (Array.isArray(obj)) {
    for (const item of obj) downgrade(item);
    return;
  }

  // Version marker + webhooks.
  if (typeof obj.openapi === "string" && obj.openapi.startsWith("3.1")) {
    obj.openapi = "3.0.3";
    delete obj.webhooks;
  }

  // type arrays → single type + nullable.
  if (Array.isArray(obj.type)) {
    const types: string[] = obj.type;
    const nonNull = types.filter((t: string) => t !== "null");
    if (nonNull.length === 1) {
      obj.type = nonNull[0];
      if (types.includes("null")) obj.nullable = true;
    } else if (nonNull.length === 0) {
      obj.type = "string";
      obj.nullable = true;
    }
  }

  // const → enum with single value (const is 3.1-only).
  if (obj.const !== undefined) {
    const val = obj.const;
    obj.enum = [val];
    if (!obj.type) {
      obj.type = typeof val === "number" ? (Number.isInteger(val) ? "integer" : "number") : typeof val;
    }
    delete obj.const;
  }

  // Numeric exclusiveMinimum/Maximum → boolean form.
  if (typeof obj.exclusiveMinimum === "number") {
    obj.minimum = obj.exclusiveMinimum;
    obj.exclusiveMinimum = true;
  }
  if (typeof obj.exclusiveMaximum === "number") {
    obj.maximum = obj.exclusiveMaximum;
    obj.exclusiveMaximum = true;
  }

  for (const val of Object.values(obj)) {
    downgrade(val);
  }
}

// ---------------------------------------------------------------------------
// Phase 2: Flatten oneOf/anyOf/allOf for ogen compatibility
// ---------------------------------------------------------------------------

function resolveSchema(
  ref: string,
  schemas: Record<string, Schema>
): Schema | null {
  const name = ref.replace("#/components/schemas/", "");
  return schemas[name] ?? null;
}

function resolveToObject(
  schema: Schema,
  schemas: Record<string, Schema>,
  depth = 0
): Schema | null {
  if (depth > 10) return null;

  if (schema.$ref) {
    const resolved = resolveSchema(schema.$ref, schemas);
    if (!resolved) return null;
    return resolveToObject(resolved, schemas, depth + 1);
  }

  if (schema.type === "object" || schema.properties) {
    return schema;
  }

  if (schema.allOf) {
    return mergeAllOf(schema.allOf, schemas, depth + 1);
  }

  if (schema.oneOf || schema.anyOf) {
    const variants = (schema.oneOf || schema.anyOf) as Schema[];
    return mergeVariants(variants, schemas, depth + 1);
  }

  return null;
}

function mergeAllOf(
  parts: Schema[],
  schemas: Record<string, Schema>,
  depth: number
): Schema | null {
  const merged: Record<string, Schema> = {};
  const required: string[] = [];

  for (const part of parts) {
    const resolved = resolveToObject(part, schemas, depth);
    if (!resolved) return null;
    const props = (resolved.properties as Record<string, Schema>) || {};
    Object.assign(merged, props);
    const req = (resolved.required as string[]) || [];
    required.push(...req);
  }

  const result: Schema = { type: "object", properties: merged };
  if (required.length > 0) result.required = [...new Set(required)];
  return result;
}

function mergeVariants(
  variants: Schema[],
  schemas: Record<string, Schema>,
  depth: number
): Schema | null {
  const allProperties: Record<string, Schema> = {};
  const requiredSets: Set<string>[] = [];

  for (const variant of variants) {
    const resolved = resolveToObject(variant, schemas, depth);
    if (!resolved) return null;
    const props = (resolved.properties as Record<string, Schema>) || {};
    for (const [k, v] of Object.entries(props)) {
      if (!allProperties[k]) {
        allProperties[k] = { ...v };
      } else if (v.enum && allProperties[k].enum) {
        allProperties[k].enum = [
          ...new Set([...allProperties[k].enum, ...v.enum]),
        ];
      }
    }
    requiredSets.push(new Set((resolved.required as string[]) || []));
  }

  // Required = intersection of all variants.
  let required: string[] = [];
  if (requiredSets.length > 0) {
    required = [...requiredSets[0]].filter((f) =>
      requiredSets.every((s) => s.has(f))
    );
  }

  const result: Schema = { type: "object", properties: allProperties };
  if (required.length > 0) result.required = required;
  return result;
}

// Recursively flatten oneOf/anyOf/allOf throughout the schema tree (bottom-up).
function flattenSchema(
  schema: Schema,
  schemas: Record<string, Schema>,
  visited: Set<Schema>
): void {
  if (!schema || typeof schema !== "object" || visited.has(schema)) return;
  visited.add(schema);

  // Recurse into sub-schemas first (bottom-up).
  if (schema.properties && typeof schema.properties === "object") {
    for (const val of Object.values(schema.properties as Record<string, Schema>)) {
      flattenSchema(val, schemas, visited);
    }
  }
  if (schema.items && typeof schema.items === "object") {
    flattenSchema(schema.items as Schema, schemas, visited);
  }
  if (
    schema.additionalProperties &&
    typeof schema.additionalProperties === "object"
  ) {
    flattenSchema(schema.additionalProperties as Schema, schemas, visited);
  }
  // Recurse into allOf/oneOf/anyOf members before flattening them.
  for (const key of ["allOf", "oneOf", "anyOf"] as const) {
    if (schema[key] && Array.isArray(schema[key])) {
      for (const member of schema[key] as Schema[]) {
        flattenSchema(member, schemas, visited);
      }
    }
  }

  // Flatten allOf.
  if (schema.allOf && Array.isArray(schema.allOf)) {
    const merged = mergeAllOf(schema.allOf, schemas, 0);
    if (merged) {
      if (schema.nullable) merged.nullable = true;
      delete schema.allOf;
      delete schema.discriminator;
      Object.assign(schema, merged);
    }
  }

  // Flatten oneOf/anyOf with 2+ variants.
  for (const key of ["oneOf", "anyOf"] as const) {
    if (schema[key] && Array.isArray(schema[key]) && schema[key].length >= 2) {
      const allVariants = schema[key] as Schema[];

      // Strip null variants and convert to nullable.
      const nonNull = allVariants.filter(
        (v) => !(v.type === "null" || (v.nullable === true && Object.keys(v).length === 1))
      );
      const hasNull = nonNull.length < allVariants.length;

      if (nonNull.length === 1) {
        // Single non-null variant + null → just use the variant with nullable.
        const only = { ...nonNull[0] };
        if (hasNull || schema.nullable) only.nullable = true;
        delete schema[key];
        delete schema.discriminator;
        Object.assign(schema, only);
      } else if (nonNull.length >= 2) {
        const merged = mergeVariants(nonNull, schemas, 0);
        if (merged) {
          if (hasNull || schema.nullable) merged.nullable = true;
          delete schema[key];
          delete schema.discriminator;
          Object.assign(schema, merged);
        }
      }
    }
  }
}

function flattenAllSchemas(spec: any): number {
  const schemas: Record<string, Schema> =
    spec.components?.schemas || {};
  let count = 0;

  // Run multiple passes until convergence (flattening may expose new nested oneOf).
  let prev = -1;
  while (count !== prev) {
    prev = count;
    const visited = new Set<Schema>();

    // Flatten component schemas.
    for (const schema of Object.values(schemas)) {
      const before = JSON.stringify(schema);
      flattenSchema(schema as Schema, schemas, visited);
      if (JSON.stringify(schema) !== before) count++;
    }

    // Flatten inline schemas in paths (request/response bodies).
    for (const methods of Object.values(spec.paths || {})) {
      for (const op of Object.values(methods as Record<string, any>)) {
        if (!op || typeof op !== "object") continue;
        // Request body schemas.
        const reqSchemas =
          op.requestBody?.content?.["application/json"]?.schema;
        if (reqSchemas) {
          const before = JSON.stringify(reqSchemas);
          flattenSchema(reqSchemas, schemas, visited);
          if (JSON.stringify(reqSchemas) !== before) count++;
        }
        // Response schemas.
        for (const resp of Object.values(op.responses || {})) {
          const respSchema = (resp as any)?.content?.["application/json"]
            ?.schema;
          if (respSchema) {
            const before = JSON.stringify(respSchema);
            flattenSchema(respSchema, schemas, visited);
            if (JSON.stringify(respSchema) !== before) count++;
          }
        }
      }
    }
  }

  return count;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const spec = JSON.parse(readFileSync(inputPath, "utf-8"));

downgrade(spec);
console.log("Downgraded 3.1 → 3.0");

const flattened = flattenAllSchemas(spec);
console.log(`Flattened ${flattened} complex schemas`);

writeFileSync(outputPath, JSON.stringify(spec, null, 2) + "\n");
console.log(`Wrote ${outputPath}`);
