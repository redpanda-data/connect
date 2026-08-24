// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

// Package multischema resolves a schema_include/schema_exclude
// configuration (replicating from multiple PostgreSQL schemas matched by a
// glob pattern) into the concrete set of schemas and tables to replicate.
package multischema

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
)

// Resolver is responsible for helping resolve DB schemas for multi-schema support.
type Resolver struct {
	// Include is the schema_include glob pattern.
	Include string
	// Exclude is the schema_exclude list, evaluated against the schemas
	// matched by Include.
	Exclude []string

	previouslyResolved     []string
	previouslyInaccessible []string
}

// NewResolver returns a Resolver for the given schema_include/schema_exclude
// configuration. Callers should only construct one when schema_include is
// set.
func NewResolver(include string, exclude []string) *Resolver {
	return &Resolver{Include: include, Exclude: exclude}
}

// Resolve resolves r.Include against conn, applies r.Exclude filtering, and
// warns about schema-set drift against the previously resolved set from an
// earlier call to Resolve on this same Resolver.
func (r *Resolver) Resolve(ctx context.Context, conn *pgconn.PgConn, logger *service.Logger) ([]string, error) {
	schemas, inaccessibleSchemas, err := resolveSchemas(ctx, conn, r.Include)
	if err != nil {
		return nil, fmt.Errorf("resolving schema_include pattern %q: %w", r.Include, err)
	}
	matchedSchemas := schemas

	if len(r.Exclude) > 0 {
		// Filtering happens entirely against the schemas slice we already
		// fetched above - no extra DB round-trips per exclude pattern.
		var excluded []string
		remaining := make([]string, 0, len(schemas))
		for _, schema := range schemas {
			var isExcluded bool
			for _, pattern := range r.Exclude {
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
			logger.Debugf("schema_exclude %v excluded %d schema(s) %v from schema_include pattern %q; %d schema(s) remain: %v", r.Exclude, len(excluded), excluded, r.Include, len(remaining), remaining)
		}
		schemas = remaining
	}

	if len(inaccessibleSchemas) > 0 && !slices.Equal(inaccessibleSchemas, r.previouslyInaccessible) {
		logger.Warnf("schema_include pattern %q matches schema(s) %v that the configured role cannot see (missing USAGE privilege); they will be skipped", r.Include, inaccessibleSchemas)
	}
	r.previouslyInaccessible = slices.Clone(inaccessibleSchemas)

	if len(schemas) == 0 {
		if len(matchedSchemas) > 0 {
			return nil, fmt.Errorf("schema_include pattern %q matched schema(s) %v, but schema_exclude %v excluded all of them", r.Include, matchedSchemas, r.Exclude)
		}
		return nil, fmt.Errorf("no schemas found matching schema_include pattern %q", r.Include)
	}
	logger.Debugf("schema_include pattern %q resolved to %d schema(s): %v", r.Include, len(schemas), schemas)

	if r.previouslyResolved != nil {
		added, removed := diffSchemaSets(r.previouslyResolved, schemas)
		if len(added) > 0 {
			logger.Warnf("schema_include pattern %q now also matches schema(s) %v that did not match on the previous connect; their tables are being added to the publication, but any rows already in them will NOT be snapshotted even if stream_snapshot is enabled - only changes made from now on will be captured", r.Include, added)
		}
		if len(removed) > 0 {
			logger.Warnf("schema(s) %v no longer match schema_include pattern %q (dropped, renamed, or the role lost USAGE) since the previous connect; their tables are being removed from the publication and will stop replicating", removed, r.Include)
		}
	}
	r.previouslyResolved = slices.Clone(schemas)

	return schemas, nil
}

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

// ResolveExistingTables returns the quoted names of the publishable base
// tables in each of the given (already quoted) schemas, keyed by quoted
// schema name. A single query covering every schema, rather than one per
// schema, keeps this to one round-trip regardless of tenant count.
//
// Queries pg_class/pg_namespace directly, filtered to relkind = 'r', so
// discovery only returns ordinary tables (including leaf partitions) and
// excludes partitioned parents, views, and foreign tables.
func ResolveExistingTables(ctx context.Context, conn *pgconn.PgConn, quotedSchemas []string) (map[string]map[string]struct{}, error) {
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
		fmt.Sprintf(`SELECT n.nspname AS table_schema, c.relname AS table_name
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN (%s) AND c.relkind = 'r'`, strings.Join(placeholders, ", ")),
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
