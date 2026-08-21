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
// pattern used by resolveSchemas, plus whether that pattern must be matched
// case-sensitively. Extracted for unit testing.
//
// For quoted identifiers the inner name is exact-escaped (no wildcard
// expansion) and matched case-sensitively, since a quoted identifier's case
// is significant and PostgreSQL does not fold it. For unquoted patterns the
// '*' wildcard is converted to '%' and matching is case-insensitive: this
// mirrors PostgreSQL folding unquoted identifiers to lower-case at creation
// time for the common case, but must hold even when a schema had to be
// created with a quoted identifier for an unrelated reason (e.g. a
// UUID-suffixed tenant schema, which requires quoting because hyphens are
// invalid in unquoted identifiers) and so kept whatever case it was written
// with — an unquoted glob like "tenant_*" is still expected to match it.
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

// resolveSchemas returns the schemas matching pattern that the connection's
// role has access to (visibleSchemas), plus any schemas that also match
// pattern but are hidden from information_schema.schemata by privileges
// (inaccessibleSchemas). The latter is surfaced separately so callers can
// warn the user instead of silently dropping schemas they expected to be
// included, e.g. `"tenant_*"` matching a schema the configured role can't
// see yet.
//
// For unquoted patterns (e.g. "tenant_*") the pattern is matched
// case-insensitively via ILIKE, regardless of whether the matched schema was
// itself created case-insensitively. For quoted identifiers (e.g.
// `"MySchema"`) an exact case-sensitive lookup is performed via LIKE. System
// schemas (pg_* and information_schema) are always excluded case-sensitively
// so that wildcard patterns like "*" do not attempt to replicate catalog
// tables.
//
// Returned schema names are quoted PostgreSQL identifiers. Returns an error
// if either query fails; returns a nil visibleSchemas slice (with a nil err)
// if no schemas match — callers should treat that as an error condition.
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

	// pg_namespace is not privilege-filtered, so any pattern match here that's
	// missing from information_schema.schemata means the role lacks USAGE (or
	// similar) on that schema rather than the schema simply not existing.
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

// resolveIncludedSchemas resolves config.DBSchemaInclude against conn,
// applies config.DBSchemaExclude filtering, and logs schema-set drift
// against the previous resolution cached on config (see
// Config.previouslyResolvedSchemas)
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
			cfg.Logger.Infof("schema_exclude %v excluded %d schema(s) %v from schema_include pattern %q; %d schema(s) remain: %v", cfg.DBSchemaExclude, len(excluded), excluded, cfg.DBSchemaInclude, len(remaining), remaining)
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
	cfg.Logger.Infof("schema_include pattern %q resolved to %d schema(s): %v", cfg.DBSchemaInclude, len(schemas), schemas)

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
// tables (including partitioned tables) that exist in the given (already
// quoted) schema.
//
// Used to resolve a schema glob × table list combination per-schema, since a
// matched schema may not contain a configured table (e.g. still being
// provisioned) or may have a same-named view/foreign/temporary table
// instead. table_type is restricted to 'BASE TABLE' so either case is
// treated as missing and falls through to the caller's "not found, skipping"
// warning - otherwise CreatePublication's FOR TABLE clause would reference
// an unpublishable relation and fail setup for every matched schema, not
// just the drifted one.
func resolveExistingTables(ctx context.Context, conn *pgconn.PgConn, quotedSchema string) (map[string]struct{}, error) {
	schema, err := sanitize.UnquotePostgresIdentifier(quotedSchema)
	if err != nil {
		return nil, fmt.Errorf("unquoting schema identifier %q: %w", quotedSchema, err)
	}

	q, err := sanitize.SQLQuery(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
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

// schemaMatchesExcludePattern reports whether quotedSchemaName - a schema
// identifier in the same quoted form resolveSchemas returns - matches
// excludePattern, a schema_exclude entry written in the same shape as a
// schema_include value (an exact name, a '*' glob, or a double-quoted exact
// identifier).
//
// Matching happens entirely in memory against a schema list we've already
// resolved, unlike resolveSchemas which queries the database: schema_exclude
// only ever narrows a candidate set that's already been fetched, so there's
// no reason to pay for another round-trip per exclude pattern. Semantics
// mirror schemaPatternToLike without going through SQL: a quoted pattern is
// an exact, case-sensitive match on the unquoted name; an unquoted pattern is
// matched case-insensitively with '*' as a wildcard.
//
// Returns an error only when a quoted operand fails to unquote. This should
// only happen for a malformed excludePattern in practice, since
// quotedSchemaName is always freshly quoted by resolveSchemas.
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

// diffSchemaSets reports which schemas are present in current but not
// previous (added) and vice versa (removed), used to detect schema-set drift
// between the schema_include resolution done on this connect and the one
// done on the previous connect/reconnect. A nil previous (the very first
// resolution in the process's lifetime) is not a meaningful "everything was
// just added" drift, so callers should only act on the result when previous
// is non-nil.
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

// globToRegexp compiles an unquoted glob pattern (using '*' as a wildcard)
// into an anchored regexp - the in-memory equivalent of globToLike for
// callers matching against values already held in Go rather than via a SQL
// LIKE clause.
func globToRegexp(pattern string) (*regexp.Regexp, error) {
	parts := strings.Split(pattern, "*")
	for i, part := range parts {
		parts[i] = regexp.QuoteMeta(part)
	}
	return regexp.Compile("^" + strings.Join(parts, ".*") + "$")
}
