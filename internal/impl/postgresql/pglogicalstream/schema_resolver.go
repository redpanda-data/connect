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
	"regexp"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
)

// schemaPatternToLike converts a schema name or glob pattern into the LIKE
// pattern used by resolveSchemas, plus whether it must be matched
// case-sensitively. Quoted identifiers are matched exactly and
// case-sensitively; unquoted patterns use '*' as a wildcard and match
// case-insensitively, mirroring how PostgreSQL folds unquoted identifiers.
func schemaPatternToLike(pattern string) (likePattern string, caseSensitive bool, err error) {
	if strings.HasPrefix(pattern, `"`) {
		unquoted, err := sanitize.UnquotePostgresIdentifier(pattern)
		if err != nil {
			return "", false, fmt.Errorf("invalid quoted schema identifier %q: %w", pattern, err)
		}
		return escapeLike(unquoted), true, nil
	}
	return globToLike(strings.ToLower(pattern)), false, nil
}

// resolveSchemas returns the schemas matching pattern that the role can see
// (visibleSchemas), plus schemas that also match but are hidden by missing
// privileges (inaccessibleSchemas) - surfaced separately so callers can warn
// instead of silently dropping them. System schemas (pg_* and
// information_schema) are always excluded. Returned names are quoted
// PostgreSQL identifiers. A nil visibleSchemas with a nil error means no
// match - callers should treat that as an error.
func resolveSchemas(ctx context.Context, conn *pgconn.PgConn, pattern string) (visibleSchemas, inaccessibleSchemas []string, err error) {
	likePattern, caseSensitive, err := schemaPatternToLike(pattern)
	if err != nil {
		return nil, nil, err
	}
	// Fixed, code-chosen operator (never derived from user input), so it's
	// safe to splice directly into the query text rather than parameterize.
	op := "ILIKE"
	if caseSensitive {
		op = "LIKE"
	}

	q, err := sanitize.SQLQuery(
		fmt.Sprintf("SELECT schema_name FROM information_schema.schemata WHERE schema_name %s $1 ESCAPE '!' AND schema_name NOT LIKE 'pg!_%%' ESCAPE '!' AND schema_name != 'information_schema'", op),
		likePattern,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("building schema resolution query: %w", err)
	}

	results, err := conn.Exec(ctx, q).ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("querying schemas matching %q: %w", pattern, err)
	}

	visible := map[string]struct{}{}
	var schemas []string
	if len(results) > 0 {
		for _, row := range results[0].Rows {
			name := string(row[0])
			visible[name] = struct{}{}
			// QuotePostgresIdentifier preserves the exact stored name (including
			// case for case-sensitive schemas), unlike NormalizePostgresIdentifier
			// which would incorrectly fold to lower-case.
			schemas = append(schemas, sanitize.QuotePostgresIdentifier(name))
		}
	}

	// pg_namespace isn't privilege-filtered, so a match here missing from
	// information_schema.schemata means the role lacks USAGE on that schema.
	nsQ, err := sanitize.SQLQuery(
		fmt.Sprintf("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname %s $1 ESCAPE '!' AND nspname NOT LIKE 'pg!_%%' ESCAPE '!' AND nspname != 'information_schema'", op),
		likePattern,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("building pg_namespace resolution query: %w", err)
	}

	nsResults, err := conn.Exec(ctx, nsQ).ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("querying pg_namespace for schemas matching %q: %w", pattern, err)
	}

	var hidden []string
	if len(nsResults) > 0 {
		for _, row := range nsResults[0].Rows {
			name := string(row[0])
			if _, ok := visible[name]; !ok {
				hidden = append(hidden, sanitize.QuotePostgresIdentifier(name))
			}
		}
	}

	return schemas, hidden, nil
}

// resolveIncludedSchemas resolves cfg.DBSchemaInclude against conn, applies
// cfg.DBSchemaExclude filtering, and warns about schema-set drift against
// the previously resolved set cached on cfg.
func resolveIncludedSchemas(ctx context.Context, conn *pgconn.PgConn, cfg *Config) ([]string, error) {
	schemas, inaccessibleSchemas, err := resolveSchemas(ctx, conn, cfg.DBSchemaInclude)
	if err != nil {
		return nil, fmt.Errorf("resolving schema_include pattern %q: %w", cfg.DBSchemaInclude, err)
	}
	matchedSchemas := schemas

	if len(cfg.DBSchemaExclude) > 0 {
		// Filtering happens entirely against the schemas slice we already
		// fetched above - no extra DB round-trips per exclude pattern.
		var excluded []string
		remaining := make([]string, 0, len(schemas))
		for _, schema := range schemas {
			var isExcluded bool
			for _, pattern := range cfg.DBSchemaExclude {
				matched, err := schemaMatchesExcludePattern(schema, pattern)
				if err != nil {
					return nil, fmt.Errorf("evaluating schema_exclude pattern %q against schema %q: %w", pattern, schema, err)
				}
				if matched {
					isExcluded = true
					break
				}
			}
			if isExcluded {
				excluded = append(excluded, schema)
				continue
			}
			remaining = append(remaining, schema)
		}
		if len(excluded) > 0 {
			cfg.Logger.Debugf("schema_exclude %v excluded %d schema(s) %v from schema_include pattern %q; %d schema(s) remain: %v", cfg.DBSchemaExclude, len(excluded), excluded, cfg.DBSchemaInclude, len(remaining), remaining)
		}
		schemas = remaining
	}

	if len(inaccessibleSchemas) > 0 && !slices.Equal(inaccessibleSchemas, cfg.previouslyInaccessibleSchemas) {
		cfg.Logger.Warnf("schema_include pattern %q matches schema(s) %v that the configured role cannot see (missing USAGE privilege); they will be skipped", cfg.DBSchemaInclude, inaccessibleSchemas)
	}
	cfg.previouslyInaccessibleSchemas = slices.Clone(inaccessibleSchemas)

	if len(schemas) == 0 {
		if len(matchedSchemas) > 0 {
			return nil, fmt.Errorf("schema_include pattern %q matched schema(s) %v, but schema_exclude %v excluded all of them", cfg.DBSchemaInclude, matchedSchemas, cfg.DBSchemaExclude)
		}
		return nil, fmt.Errorf("no schemas found matching schema_include pattern %q", cfg.DBSchemaInclude)
	}
	cfg.Logger.Debugf("schema_include pattern %q resolved to %d schema(s): %v", cfg.DBSchemaInclude, len(schemas), schemas)

	if cfg.previouslyResolvedSchemas != nil {
		added, removed := diffSchemaSets(cfg.previouslyResolvedSchemas, schemas)
		if len(added) > 0 {
			cfg.Logger.Warnf("schema_include pattern %q now also matches schema(s) %v that did not match on the previous connect; their tables are being added to the publication, but any rows already in them will NOT be snapshotted even if stream_snapshot is enabled - only changes made from now on will be captured", cfg.DBSchemaInclude, added)
		}
		if len(removed) > 0 {
			cfg.Logger.Warnf("schema(s) %v no longer match schema_include pattern %q (dropped, renamed, or the role lost USAGE) since the previous connect; their tables are being removed from the publication and will stop replicating", removed, cfg.DBSchemaInclude)
		}
	}
	cfg.previouslyResolvedSchemas = slices.Clone(schemas)

	return schemas, nil
}

// resolveExistingTables returns the quoted names of the publishable base
// tables in each of the given (already quoted) schemas, keyed by quoted
// schema name. A single query covering every schema, rather than one per
// schema, keeps this to one round-trip regardless of tenant count - the
// difference between one query and, say, one hundred on a multi-tenant,
// schema-per-tenant database on every connect and reconnect. Restricted to
// table_type = 'BASE TABLE' so a same-named view or foreign table is treated
// as missing rather than breaking CreatePublication.
func resolveExistingTables(ctx context.Context, conn *pgconn.PgConn, quotedSchemas []string) (map[string]map[string]struct{}, error) {
	rawToQuoted := make(map[string]string, len(quotedSchemas))
	args := make([]any, len(quotedSchemas))
	placeholders := make([]string, len(quotedSchemas))
	for i, quotedSchema := range quotedSchemas {
		schema, err := sanitize.UnquotePostgresIdentifier(quotedSchema)
		if err != nil {
			return nil, fmt.Errorf("unquoting schema identifier %q: %w", quotedSchema, err)
		}
		rawToQuoted[schema] = quotedSchema
		args[i] = schema
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	q, err := sanitize.SQLQuery(
		fmt.Sprintf("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN (%s) AND table_type = 'BASE TABLE'", strings.Join(placeholders, ", ")),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("building table resolution query for schema(s) %v: %w", quotedSchemas, err)
	}

	results, err := conn.Exec(ctx, q).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("querying tables in schema(s) %v: %w", quotedSchemas, err)
	}

	existing := make(map[string]map[string]struct{}, len(quotedSchemas))
	for _, quotedSchema := range quotedSchemas {
		existing[quotedSchema] = map[string]struct{}{}
	}
	if len(results) > 0 {
		for _, row := range results[0].Rows {
			quotedSchema := rawToQuoted[string(row[0])]
			existing[quotedSchema][sanitize.QuotePostgresIdentifier(string(row[1]))] = struct{}{}
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

// schemaMatchesExcludePattern reports whether quotedSchemaName matches
// excludePattern, using the same pattern syntax as schema_include (exact
// name, '*' glob, or quoted exact identifier). Matches in memory against an
// already-resolved schema list, so no extra DB round-trip is needed. Returns
// an error only if a quoted operand fails to unquote.
func schemaMatchesExcludePattern(quotedSchemaName, excludePattern string) (bool, error) {
	schemaName, err := sanitize.UnquotePostgresIdentifier(quotedSchemaName)
	if err != nil {
		return false, fmt.Errorf("unquoting schema identifier %q: %w", quotedSchemaName, err)
	}

	if strings.HasPrefix(excludePattern, `"`) {
		unquoted, err := sanitize.UnquotePostgresIdentifier(excludePattern)
		if err != nil {
			return false, fmt.Errorf("invalid quoted schema identifier %q: %w", excludePattern, err)
		}
		return schemaName == unquoted, nil
	}

	re, err := globToRegexp(strings.ToLower(excludePattern))
	if err != nil {
		return false, fmt.Errorf("invalid exclude pattern %q: %w", excludePattern, err)
	}
	return re.MatchString(strings.ToLower(schemaName)), nil
}

// diffSchemaSets reports schemas present in current but not previous
// (added) and vice versa (removed). Callers should ignore the result when
// previous is nil - that's the first resolution, not real drift.
func diffSchemaSets(previous, current []string) (added, removed []string) {
	previousSet := make(map[string]struct{}, len(previous))
	for _, schema := range previous {
		previousSet[schema] = struct{}{}
	}
	currentSet := make(map[string]struct{}, len(current))
	for _, schema := range current {
		currentSet[schema] = struct{}{}
	}
	for _, schema := range current {
		if _, ok := previousSet[schema]; !ok {
			added = append(added, schema)
		}
	}
	for _, schema := range previous {
		if _, ok := currentSet[schema]; !ok {
			removed = append(removed, schema)
		}
	}
	return added, removed
}

// globToRegexp compiles an unquoted glob pattern ('*' as wildcard) into an
// anchored regexp - the in-memory equivalent of globToLike.
func globToRegexp(pattern string) (*regexp.Regexp, error) {
	parts := strings.Split(pattern, "*")
	for i, part := range parts {
		parts[i] = regexp.QuoteMeta(part)
	}
	return regexp.Compile("^" + strings.Join(parts, ".*") + "$")
}
