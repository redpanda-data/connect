# SMT Coverage Expansion — Design Addendum

**Date:** 2026-06-22
**Branch:** `kafka_converter`
**Extends:** `2026-06-22-kafka-connect-converter-design.md`

## Goal

Broaden Single Message Transform (SMT) coverage beyond the v1 set
(InsertField, ReplaceField, RegexRouter) by adding mappers for the most common
remaining Kafka Connect SMTs. Purely additive: each new SMT is a new file
registering itself via `init()`; the engine (`mapSMTs`, `convert.go`, registry)
is unchanged.

## Approach

Same `SMTMapper` pattern as v1. Each mapper emits one or more RPCN `mapping`
(Bloblang) processors (string-valued `mapping: <bloblang>`), validated end-to-end
through the benthos linter via `assertValidRPCN`.

Register BOTH the `$Value` and `$Key` class variants where they exist (matching
how InsertField/ReplaceField already register both). The generated Bloblang
targets the value (`root`/`this`); for `$Key` variants the mapper adds a
`# TODO:` note that the transform should target the message key and needs review
(RPCN handles keys via output `key` fields / `meta key`, not the value document).

Anything without a clean, lint-valid Bloblang equivalent must degrade to a
`root = this` stub + `ctx.Warn` + TODO — never a guessed/incorrect mapping. The
linter is the source of truth for which Bloblang forms are valid.

## SMTs to add

| SMT (`org.apache.kafka.connect.transforms.*`) | Intended Bloblang (value semantics) |
|---|---|
| `ExtractField` | `root = this.<field>` (from `field`) |
| `HoistField` | `root = {"<field>": this}` (from `field`) |
| `MaskField` | for each field in `fields`: `root.<f> = ""` (string zero); numeric mask via `replacement` if present |
| `Cast` | for each `<f>:<type>` in `spec`: `root.<f> = this.<f>.<conv>()` where int*/float*→`.number()`, string→`.string()`, boolean→`.bool()` |
| `TimestampConverter` | `field` + `target.type`: `root.<field> = this.<field>.ts_parse(...)` / `.ts_format(...)` / `.ts_unix()` as applicable — TODO-annotate the format if not directly derivable |
| `ValueToKey` | `meta key = this.<field>.string()` (from `fields`, first field); TODO if multiple fields |

Each mapper:
- reads its props via `ctx.String(...)` (marks them consumed);
- emits valid Bloblang verified by `assertValidRPCN`;
- TODO-annotates anything ambiguous (key-targeting `$Key` variants, multi-field
  ValueToKey, non-derivable timestamp formats) and records a `ctx.Warn`.

## Out of scope (still)

- Debezium SMTs (`io.debezium.transforms.*`) — pair with Debezium CDC sources,
  which remain out of converter scope.
- `Filter`, `Flatten`, `HeaderFrom`, `SetSchemaMetadata` — deferrable; add later if
  needed (Filter needs a `mapping`+drop or dedicated processor; Flatten needs
  recursive key flattening).

## Testing

One `smt_<name>_test.go`-style test per SMT (or grouped in `smt_test.go`),
each calling `assertValidRPCN`. Add the most representative cases to the golden
suite if useful. `task lint` must stay clean.
