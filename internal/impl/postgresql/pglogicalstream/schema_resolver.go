// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
)

// resolveSchemas expands a schema name or glob pattern into the set of
// quoted PostgreSQL identifiers that exist in the database.
//
// For unquoted patterns (e.g. "tenant_*") the pattern is matched
// case-insensitively against information_schema.schemata using LIKE, because
// PostgreSQL folds unquoted identifiers to lower-case at creation time.
//
// For quoted identifiers (e.g. `"MySchema"`) an exact case-sensitive lookup
// is performed.
//
// System schemas (pg_* and information_schema) are always excluded so that
// wildcard patterns like "*" do not attempt to replicate catalog tables.
//
// Returns an error if the query fails. Returns (nil, nil) if no schemas match.
// The caller is responsible for treating an empty result as an error.

// schemaPatternToLike converts a schema name or glob pattern into the LIKE
// pattern used by resolveSchemas. Extracted for unit testing.
//
// For quoted identifiers the inner name is exact-escaped (no wildcard expansion).
// For unquoted patterns the '*' wildcard is converted to '%' and the input is
// folded to lower-case to match PostgreSQL's identifier folding.
func schemaPatternToLike(pattern string) (string, error) {
	if strings.HasPrefix(pattern, `"`) {
		unquoted, err := sanitize.UnquotePostgresIdentifier(pattern)
		if err != nil {
			return "", fmt.Errorf("invalid quoted schema identifier %q: %w", pattern, err)
		}
		return escapeLike(unquoted), nil
	}
	return globToLike(strings.ToLower(pattern)), nil
}

func resolveSchemas(ctx context.Context, conn *pgconn.PgConn, pattern string) ([]string, error) {
	likePattern, err := schemaPatternToLike(pattern)
	if err != nil {
		return nil, err
	}

	q, err := sanitize.SQLQuery(
		"SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE $1 ESCAPE '!' AND schema_name NOT LIKE 'pg!_%' ESCAPE '!' AND schema_name != 'information_schema'",
		likePattern,
	)
	if err != nil {
		return nil, fmt.Errorf("building schema resolution query: %w", err)
	}

	results, err := conn.Exec(ctx, q).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("querying schemas matching %q: %w", pattern, err)
	}

	var schemas []string
	if len(results) > 0 {
		for _, row := range results[0].Rows {
			// QuotePostgresIdentifier preserves the exact stored name (including
			// case for case-sensitive schemas), unlike NormalizePostgresIdentifier
			// which would incorrectly fold to lower-case.
			schemas = append(schemas, sanitize.QuotePostgresIdentifier(string(row[0])))
		}
	}
	return schemas, nil
}

// resolveExistingTables returns the set of quoted table identifiers that
// actually exist in the given (already quoted) schema.
//
// This is used to resolve a schema glob × table list combination per-schema
// rather than assuming every matched schema contains every listed table. A
// schema matching the glob but missing one of the configured tables (e.g. a
// tenant schema that's still being provisioned) would otherwise cause
// CreatePublication's FOR TABLE clause to reference a non-existent relation,
// failing publication setup for every schema, not just the drifted one.
func resolveExistingTables(ctx context.Context, conn *pgconn.PgConn, quotedSchema string) (map[string]struct{}, error) {
	schema, err := sanitize.UnquotePostgresIdentifier(quotedSchema)
	if err != nil {
		return nil, fmt.Errorf("unquoting schema identifier %q: %w", quotedSchema, err)
	}

	q, err := sanitize.SQLQuery(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = $1",
		schema,
	)
	if err != nil {
		return nil, fmt.Errorf("building table resolution query for schema %q: %w", quotedSchema, err)
	}

	results, err := conn.Exec(ctx, q).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("querying tables in schema %q: %w", quotedSchema, err)
	}

	existing := map[string]struct{}{}
	if len(results) > 0 {
		for _, row := range results[0].Rows {
			existing[sanitize.QuotePostgresIdentifier(string(row[0]))] = struct{}{}
		}
	}
	return existing, nil
}

// globToLike converts an unquoted glob pattern (using '*' as wildcard) into a
// PostgreSQL LIKE pattern that uses '!' as the escape character.
//
// Mapping:
//   - '*' → '%'   (zero or more characters)
//   - '_' → '!_'  (literal underscore, not the LIKE single-char wildcard)
//   - '%' → '!%'  (literal percent, not the LIKE multi-char wildcard)
//   - '!' → '!!'  (literal escape character)
func globToLike(pattern string) string {
	var b strings.Builder
	b.Grow(len(pattern) + 4)
	for _, ch := range pattern {
		switch ch {
		case '*':
			b.WriteByte('%')
		case '_':
			b.WriteString("!_")
		case '%':
			b.WriteString("!%")
		case '!':
			b.WriteString("!!")
		default:
			b.WriteRune(ch)
		}
	}
	return b.String()
}

// escapeLike escapes LIKE metacharacters in s without expanding any wildcards.
// Used for exact quoted-identifier lookups.
func escapeLike(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, ch := range s {
		switch ch {
		case '_':
			b.WriteString("!_")
		case '%':
			b.WriteString("!%")
		case '!':
			b.WriteString("!!")
		default:
			b.WriteRune(ch)
		}
	}
	return b.String()
}
