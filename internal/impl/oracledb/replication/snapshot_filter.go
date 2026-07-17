// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// clauseKeywords are words that may legally follow a FROM table reference without
// being mistaken for a table alias.
var clauseKeywords = map[string]bool{
	"where": true, "order": true, "group": true, "having": true,
	"connect": true, "start": true, "fetch": true, "for": true,
	"union": true, "intersect": true, "minus": true,
	"model": true, "pivot": true, "unpivot": true,
}

// NormalizeSnapshotFilterKeys uppercases every key in filters, returning a new map.
// Returns an error if two keys collide once uppercased (e.g. "testdb.foo" and
// "TESTDB.FOO" both present), since silently keeping only one would be surprising.
func NormalizeSnapshotFilterKeys(filters map[string]string) (map[string]string, error) {
	normalized := make(map[string]string, len(filters))
	for table, query := range filters {
		key := strings.ToUpper(table)
		if _, exists := normalized[key]; exists {
			return nil, fmt.Errorf("snapshot filter table %q is specified more than once (case-insensitive)", key)
		}
		normalized[key] = query
	}
	return normalized, nil
}

// ValidateSnapshotFilters validates that the snapshot filters are SELECT statements
// referencing exactly one simple table - no joins, subqueries, or UNION/INTERSECT/MINUS
// combinations - and that each filter's table matches its map key.
//
// This is a hand-rolled scan rather than a full SQL parse, in the same spirit as
// oracledb/logminer/sqlredo: these queries only ever take the fixed shape
// `SELECT ... FROM <table> [alias] [WHERE ...]`, so a full grammar isn't needed to
// reject anything else.
func ValidateSnapshotFilters(filters map[string]string) error {
	for table, query := range filters {
		parsedTable, err := parseFilterTable(table, query)
		if err != nil {
			return err
		}
		if strings.ToUpper(table) != parsedTable {
			return fmt.Errorf("snapshot filter key %q does not match the table referenced in the query (%q)", table, parsedTable)
		}
	}
	return nil
}

// parseFilterTable validates query and returns the uppercased, optionally
// schema-qualified table name referenced in its FROM clause.
func parseFilterTable(table, query string) (string, error) {
	sc := &filterScanner{s: strings.TrimRight(strings.TrimSpace(query), " \t\n\r;")}
	sc.ws()
	if !sc.keyword("select") {
		return "", fmt.Errorf("snapshot filter for table %q must be a SELECT statement", table)
	}

	found, err := sc.skipToFrom()
	if err != nil {
		return "", fmt.Errorf("snapshot filter for table %q is not valid SQL: %w", table, err)
	}
	if !found {
		return "", fmt.Errorf("snapshot filter for table %q is not valid SQL: missing FROM clause", table)
	}
	sc.ws()

	tableName, err := sc.readTableName()
	if err != nil {
		return "", fmt.Errorf("snapshot filter for table %q must reference a simple table name: %w", table, err)
	}
	sc.ws()

	switch {
	case sc.done():
	case sc.s[sc.i] == ',':
		return "", fmt.Errorf("snapshot filter for table %q must query exactly one table", table)
	case sc.joinKeyword():
		return "", fmt.Errorf("snapshot filter for table %q must reference a simple table name", table)
	default:
		if err := sc.skipOptionalAlias(); err != nil {
			return "", fmt.Errorf("snapshot filter for table %q must reference a simple table name: %w", table, err)
		}
	}

	if err := sc.checkRemainder(); err != nil {
		return "", fmt.Errorf("snapshot filter for table %q is not valid SQL: %w", table, err)
	}

	return tableName, nil
}

// filterScanner is a forward-only cursor over a snapshot filter SQL string.
// Character-by-character, no regex.
type filterScanner struct {
	s string
	i int
}

func (sc *filterScanner) done() bool { return sc.i >= len(sc.s) }

func isSpaceByte(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func isIdentStartByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isIdentByte(b byte) bool {
	return isIdentStartByte(b) || b == '$' || b == '#' || (b >= '0' && b <= '9')
}

func (sc *filterScanner) ws() {
	for sc.i < len(sc.s) && isSpaceByte(sc.s[sc.i]) {
		sc.i++
	}
}

// keyword matches kw (lowercase ASCII) at the current position case-insensitively,
// requiring word boundaries on both sides. Advances sc.i and returns true on a match;
// otherwise no-ops and returns false.
func (sc *filterScanner) keyword(kw string) bool {
	if sc.i > 0 && isIdentByte(sc.s[sc.i-1]) {
		return false
	}
	if sc.i+len(kw) > len(sc.s) {
		return false
	}
	for j := 0; j < len(kw); j++ {
		c := sc.s[sc.i+j]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != kw[j] {
			return false
		}
	}
	end := sc.i + len(kw)
	if end < len(sc.s) && isIdentByte(sc.s[end]) {
		return false
	}
	sc.i += len(kw)
	return true
}

// joinKeyword reports (and consumes) whether the scanner is positioned at a join
// keyword, which indicates the FROM clause references more than one table.
func (sc *filterScanner) joinKeyword() bool {
	joinKeywords := []string{"join", "inner", "outer", "left", "right", "full", "cross", "natural"}
	return slices.ContainsFunc(joinKeywords, sc.keyword)
}

// peekWord returns the lowercased identifier at the current position without
// advancing the scanner.
func (sc *filterScanner) peekWord() string {
	j := sc.i
	for j < len(sc.s) && isIdentByte(sc.s[j]) {
		j++
	}
	return strings.ToLower(sc.s[sc.i:j])
}

// skipQuoted advances past a quoted region opened by q (a single or double quote
// byte), where q is escaped within the region by doubling it. Returns false if the
// region is unterminated.
func (sc *filterScanner) skipQuoted(q byte) bool {
	sc.i++ // opening quote
	for sc.i < len(sc.s) {
		if sc.s[sc.i] == q {
			if sc.i+1 < len(sc.s) && sc.s[sc.i+1] == q {
				sc.i += 2 // escaped quote
				continue
			}
			sc.i++ // closing quote
			return true
		}
		sc.i++
	}
	return false
}

// skipToFrom advances past the SELECT list to the top-level FROM keyword, skipping
// over string/quoted-identifier contents and nested parenthesised expressions
// (e.g. subqueries or function calls in the select list). Returns false if no
// top-level FROM is found.
func (sc *filterScanner) skipToFrom() (bool, error) {
	depth := 0
	for !sc.done() {
		switch sc.s[sc.i] {
		case '\'':
			if !sc.skipQuoted('\'') {
				return false, errors.New("unterminated string literal")
			}
			continue
		case '"':
			if !sc.skipQuoted('"') {
				return false, errors.New("unterminated quoted identifier")
			}
			continue
		case '(':
			depth++
			sc.i++
			continue
		case ')':
			depth--
			sc.i++
			continue
		}
		if depth == 0 && sc.keyword("from") {
			return true, nil
		}
		sc.i++
	}
	return false, nil
}

// readIdentifier reads a double-quoted or bare identifier at the current position.
func (sc *filterScanner) readIdentifier() (string, error) {
	if sc.done() {
		return "", errors.New("expected identifier, got end of input")
	}
	if sc.s[sc.i] == '"' {
		sc.i++
		start := sc.i
		for sc.i < len(sc.s) && sc.s[sc.i] != '"' {
			sc.i++
		}
		if sc.done() {
			return "", errors.New("unterminated quoted identifier")
		}
		name := sc.s[start:sc.i]
		sc.i++ // closing "
		return name, nil
	}
	if !isIdentStartByte(sc.s[sc.i]) {
		snippet := sc.s[sc.i:]
		if len(snippet) > 20 {
			snippet = snippet[:20]
		}
		return "", fmt.Errorf("expected identifier, got %.20q", snippet)
	}
	start := sc.i
	for sc.i < len(sc.s) && isIdentByte(sc.s[sc.i]) {
		sc.i++
	}
	return sc.s[start:sc.i], nil
}

// readTableName reads a table reference of the form TABLE or SCHEMA.TABLE (either
// part optionally double-quoted) and returns it uppercased.
func (sc *filterScanner) readTableName() (string, error) {
	first, err := sc.readIdentifier()
	if err != nil {
		return "", err
	}
	if !sc.done() && sc.s[sc.i] == '.' {
		sc.i++
		second, err := sc.readIdentifier()
		if err != nil {
			return "", err
		}
		return strings.ToUpper(first) + "." + strings.ToUpper(second), nil
	}
	return strings.ToUpper(first), nil
}

// skipOptionalAlias consumes an optional table alias immediately following a FROM
// table reference, e.g. `t` or `AS t`. It leaves the scanner untouched if what
// follows is a clause keyword (WHERE, ORDER BY, UNION, ...) rather than an alias.
func (sc *filterScanner) skipOptionalAlias() error {
	sc.ws()
	if sc.done() {
		return nil
	}
	if sc.keyword("as") {
		sc.ws()
		_, err := sc.readIdentifier()
		return err
	}
	if sc.s[sc.i] == '"' {
		_, err := sc.readIdentifier()
		return err
	}
	word := sc.peekWord()
	if word == "" || clauseKeywords[word] {
		return nil
	}
	_, err := sc.readIdentifier()
	return err
}

// checkRemainder scans the rest of the query (WHERE/ORDER BY/etc.) to reject
// anything that would let the filter reach beyond its single declared table:
// statement chaining (';') at any depth, and a top-level UNION/INTERSECT/MINUS
// combining in a second SELECT. Nested subqueries (e.g. inside WHERE ... IN (...))
// are skipped over rather than rejected.
func (sc *filterScanner) checkRemainder() error {
	depth := 0
	for !sc.done() {
		switch sc.s[sc.i] {
		case '\'':
			if !sc.skipQuoted('\'') {
				return errors.New("unterminated string literal")
			}
			continue
		case '"':
			if !sc.skipQuoted('"') {
				return errors.New("unterminated quoted identifier")
			}
			continue
		case '(':
			depth++
			sc.i++
			continue
		case ')':
			depth--
			if depth < 0 {
				return errors.New("unbalanced parentheses")
			}
			sc.i++
			continue
		case ';':
			return errors.New("must not contain multiple statements")
		}
		if depth == 0 {
			switch {
			case sc.keyword("union"):
				return errors.New("must not combine multiple SELECT statements (UNION)")
			case sc.keyword("intersect"):
				return errors.New("must not combine multiple SELECT statements (INTERSECT)")
			case sc.keyword("minus"):
				return errors.New("must not combine multiple SELECT statements (MINUS)")
			}
		}
		sc.i++
	}
	if depth != 0 {
		return errors.New("unbalanced parentheses")
	}
	return nil
}
