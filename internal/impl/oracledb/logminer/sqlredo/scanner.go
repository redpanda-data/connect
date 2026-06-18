// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"errors"
	"fmt"
	"strings"
)

// ScanSQLCommand parses an Oracle LogMiner SQL_REDO string without a full SQL parser.
// It handles only the fixed forms that LogMiner produces:
//
//	INSERT: insert into "SCHEMA"."TABLE"("C1","C2") values ('v1','v2')
//	UPDATE: update "SCHEMA"."TABLE" set "C1" = 'v1' where "C2" = 'old'
//	DELETE: delete from "SCHEMA"."TABLE" where "C1" = 'v1'
//
// IS NULL / IS NOT NULL predicates in WHERE are excluded from the returned map.
// NULL values in SET / VALUES are included as nil.
// If converter is non-nil, bare (unquoted) values are passed to ConvertValue.
func ScanSQLCommand(sql string, converter *OracleValueConverter) (newValues, oldValues map[string]any, err error) {
	sc := &redoScanner{s: strings.TrimRight(sql, " \t\n\r;")}
	sc.ws()
	switch {
	case sc.keyword("insert"):
		return sc.doInsert(converter)
	case sc.keyword("update"):
		return sc.doUpdate(converter)
	case sc.keyword("delete"):
		return sc.doDelete(converter)
	default:
		return nil, nil, fmt.Errorf("unsupported SQL statement: %.40s", sql)
	}
}

// redoScanner is a forward-only cursor over an Oracle LogMiner SQL_REDO string.
// Character-by-character, no regex.
type redoScanner struct {
	s string
	i int
}

func (sc *redoScanner) done() bool { return sc.i >= len(sc.s) }

func (sc *redoScanner) ws() {
	for sc.i < len(sc.s) && isSpaceByte(sc.s[sc.i]) {
		sc.i++
	}
}

// keyword matches kw (lowercase ASCII) at the current position case-insensitively,
// requiring a word boundary immediately after (space, '(', '"', ';', or end).
// Advances sc.i and returns true on a match; otherwise no-ops and returns false.
func (sc *redoScanner) keyword(kw string) bool {
	if sc.i+len(kw) > len(sc.s) {
		return false
	}
	for j := 0; j < len(kw); j++ {
		if toLowerByte(sc.s[sc.i+j]) != kw[j] {
			return false
		}
	}
	end := sc.i + len(kw)
	if end < len(sc.s) {
		b := sc.s[end]
		if !isSpaceByte(b) && b != '(' && b != '"' && b != ';' {
			return false
		}
	}
	sc.i += len(kw)
	return true
}

// skipTableRef advances past an Oracle qualified table reference: "SCHEMA"."TABLE"
// or just "TABLE", consuming any following whitespace.
func (sc *redoScanner) skipTableRef() {
loop:
	for !sc.done() {
		switch sc.s[sc.i] {
		case '"':
			sc.i++
			for sc.i < len(sc.s) && sc.s[sc.i] != '"' {
				sc.i++
			}
			if !sc.done() {
				sc.i++ // closing "
			}
		case '.':
			sc.i++
		default:
			break loop
		}
	}
	sc.ws()
}

// readIdent reads a double-quoted identifier and returns its content without the quotes.
// It skips an optional alias prefix such as `a.` in `a."COL1"`.
func (sc *redoScanner) readIdent() (string, error) {
	sc.ws()
	// skip optional table-alias prefix (e.g. `a.` in `a."COL1"`)
	if !sc.done() && sc.s[sc.i] != '"' {
		for !sc.done() && sc.s[sc.i] != '"' {
			sc.i++
		}
	}
	if sc.done() || sc.s[sc.i] != '"' {
		snippet := sc.s[sc.i:]
		if len(snippet) > 20 {
			snippet = snippet[:20]
		}
		return "", fmt.Errorf("expected quoted identifier, got %.20q", snippet)
	}
	sc.i++ // skip opening "
	start := sc.i
	for sc.i < len(sc.s) && sc.s[sc.i] != '"' {
		sc.i++
	}
	if sc.done() {
		return "", errors.New("unterminated identifier")
	}
	name := sc.s[start:sc.i]
	sc.i++ // skip closing "
	return name, nil
}

// readValue reads the next SQL value: a single-quoted string, NULL, or a bare value
// (Oracle function call or numeric literal).
func (sc *redoScanner) readValue(converter *OracleValueConverter) (any, error) {
	sc.ws()
	if sc.done() {
		return nil, errors.New("expected value but reached end of SQL")
	}
	if sc.s[sc.i] == '\'' {
		return sc.readString()
	}
	return sc.readBare(converter)
}

// readString reads a single-quoted SQL string literal and returns the Go string.
// Oracle's only string escape is ” → '. Returns a sub-slice of the original string
// (zero allocation) when no escapes are present.
func (sc *redoScanner) readString() (string, error) {
	sc.i++ // skip opening '
	start := sc.i
	hasEscape := false

	for sc.i < len(sc.s) {
		if sc.s[sc.i] == '\'' {
			if sc.i+1 < len(sc.s) && sc.s[sc.i+1] == '\'' {
				hasEscape = true
				sc.i += 2 // consume ''
				continue
			}
			break // closing '
		}
		sc.i++
	}
	if sc.done() {
		return "", errors.New("unterminated string literal")
	}
	raw := sc.s[start:sc.i]
	sc.i++ // skip closing '

	if !hasEscape {
		return raw, nil // zero-alloc: sub-slice of original
	}
	var b strings.Builder
	b.Grow(len(raw))
	for i := 0; i < len(raw); {
		if raw[i] == '\'' && i+1 < len(raw) && raw[i+1] == '\'' {
			b.WriteByte('\'')
			i += 2
		} else {
			b.WriteByte(raw[i])
			i++
		}
	}
	return b.String(), nil
}

// readBare reads a bare SQL value: NULL, a numeric literal, or an Oracle function
// call such as TO_DATE(...), TO_TIMESTAMP(...), HEXTORAW(...), EMPTY_CLOB().
//
// Tracking rules:
//   - Parenthesis depth tracks nested function calls. Commas and closing parens
//     inside function arguments are NOT terminators.
//   - At depth 0, whitespace ends the value unless immediately followed by ||
//     (Oracle string concatenation), which allows UNISTR('...') || UNISTR('...')
//     to be captured as a single value.
//   - Single-quoted strings inside function args are skipped verbatim.
func (sc *redoScanner) readBare(converter *OracleValueConverter) (any, error) {
	start := sc.i
	depth := 0

	for sc.i < len(sc.s) {
		c := sc.s[sc.i]
		if c == '(' {
			depth++
			sc.i++
		} else if c == ')' {
			if depth == 0 {
				break
			}
			depth--
			sc.i++
		} else if c == ',' {
			if depth == 0 {
				break
			}
			sc.i++
		} else if c == '\'' {
			// skip quoted string inside function arguments
			sc.i++
			for sc.i < len(sc.s) {
				ch := sc.s[sc.i]
				sc.i++
				if ch == '\'' {
					if sc.i < len(sc.s) && sc.s[sc.i] == '\'' {
						sc.i++ // consume ''
					} else {
						break
					}
				}
			}
		} else if isSpaceByte(c) {
			if depth > 0 {
				sc.i++ // whitespace inside function args is fine
				continue
			}
			// depth == 0: stop unless followed by || (concatenation)
			j := sc.i
			for j < len(sc.s) && isSpaceByte(sc.s[j]) {
				j++
			}
			if j+1 < len(sc.s) && sc.s[j] == '|' && sc.s[j+1] == '|' {
				sc.i = j + 2 // consume whitespace + ||
				for sc.i < len(sc.s) && isSpaceByte(sc.s[sc.i]) {
					sc.i++ // consume whitespace after ||
				}
				continue
			}
			break // genuine end of bare value
		} else {
			sc.i++
		}
	}

	raw := strings.TrimSpace(sc.s[start:sc.i])
	if raw == "Unsupported" {
		// "Unsupported Type" is a two-word Oracle LogMiner sentinel; consume the second
		// word when present so the cursor isn't left mid-value-list.
		j := sc.i
		for j < len(sc.s) && isSpaceByte(sc.s[j]) {
			j++
		}
		if strings.HasPrefix(sc.s[j:], "Type") {
			end := j + len("Type")
			if end >= len(sc.s) || isSpaceByte(sc.s[end]) || sc.s[end] == ',' || sc.s[end] == ')' {
				sc.i = end
			}
		}
		return nil, nil
	}
	if raw == "NULL" || raw == "Unsupported Type" {
		return nil, nil
	}
	if converter != nil {
		return converter.ConvertValue(raw), nil
	}
	return raw, nil
}

// doInsert parses: insert into "SCHEMA"."TABLE"("C1","C2") values ('v1','v2')
func (sc *redoScanner) doInsert(converter *OracleValueConverter) (newValues, oldValues map[string]any, err error) {
	sc.ws()
	if !sc.keyword("into") {
		return nil, nil, errors.New("expected INTO after INSERT")
	}
	sc.ws()
	sc.skipTableRef()

	if sc.done() || sc.s[sc.i] != '(' {
		return nil, nil, errors.New("expected column list '(' after table name")
	}
	sc.i++ // skip (

	var columns []string
	for {
		sc.ws()
		if sc.done() {
			return nil, nil, errors.New("unterminated column list")
		}
		if sc.s[sc.i] == ')' {
			sc.i++
			break
		}
		col, err := sc.readIdent()
		if err != nil {
			return nil, nil, err
		}
		columns = append(columns, col)
		sc.ws()
		if !sc.done() && sc.s[sc.i] == ',' {
			sc.i++
		}
	}

	sc.ws()
	if !sc.keyword("values") {
		return nil, nil, errors.New("expected VALUES keyword")
	}
	sc.ws()
	if sc.done() || sc.s[sc.i] != '(' {
		return nil, nil, errors.New("expected '(' after VALUES")
	}
	sc.i++ // skip (

	newValues = make(map[string]any, len(columns))
	for i, col := range columns {
		sc.ws()
		val, err := sc.readValue(converter)
		if err != nil {
			return nil, nil, fmt.Errorf("column %s: %w", col, err)
		}
		newValues[col] = val
		sc.ws()
		if i < len(columns)-1 {
			if sc.done() || sc.s[sc.i] != ',' {
				return nil, nil, fmt.Errorf("expected ',' after value for column %s", col)
			}
			sc.i++
		}
	}
	return newValues, nil, nil
}

// doUpdate parses: update "SCHEMA"."TABLE" set "C1" = 'v1' where "C2" = 'old'
func (sc *redoScanner) doUpdate(converter *OracleValueConverter) (newValues, oldValues map[string]any, err error) {
	sc.ws()
	sc.skipTableRef()
	sc.ws()
	if !sc.keyword("set") {
		// Oracle LogMiner may emit a table alias before SET (e.g. "TABLE" a set a."C1"…)
		sc.skipBareWord()
		sc.ws()
		if !sc.keyword("set") {
			return nil, nil, errors.New("expected SET keyword")
		}
	}
	sc.ws()

	newValues = make(map[string]any)
	for !sc.done() {
		sc.ws()
		if sc.keyword("where") {
			break
		}
		col, err := sc.readIdent()
		if err != nil {
			return nil, nil, fmt.Errorf("SET clause: %w", err)
		}
		sc.ws()
		if sc.done() || sc.s[sc.i] != '=' {
			return nil, nil, fmt.Errorf("expected '=' after column %s in SET", col)
		}
		sc.i++
		val, err := sc.readValue(converter)
		if err != nil {
			return nil, nil, fmt.Errorf("SET column %s: %w", col, err)
		}
		newValues[col] = val
		sc.ws()
		if !sc.done() && sc.s[sc.i] == ',' {
			sc.i++
		}
	}

	oldValues, err = sc.doWhere(converter)
	return newValues, oldValues, err
}

// doDelete parses: delete from "SCHEMA"."TABLE" where "C1" = 'v1'
func (sc *redoScanner) doDelete(converter *OracleValueConverter) (newValues, oldValues map[string]any, err error) {
	sc.ws()
	if !sc.keyword("from") {
		return nil, nil, errors.New("expected FROM after DELETE")
	}
	sc.ws()
	sc.skipTableRef()
	sc.ws()
	if !sc.keyword("where") {
		// Oracle LogMiner may emit a table alias before WHERE (e.g. "TABLE" a where a."C1"…)
		sc.skipBareWord()
		sc.ws()
		if !sc.keyword("where") {
			return nil, make(map[string]any), nil
		}
	}
	oldValues, err = sc.doWhere(converter)
	return nil, oldValues, err
}

// doWhere parses a WHERE clause of col = val pairs joined by AND/OR.
// IS NULL and IS NOT NULL predicates are excluded from the returned map.
// Columns with a NULL equality value (= NULL) are also excluded.
func (sc *redoScanner) doWhere(converter *OracleValueConverter) (map[string]any, error) {
	result := make(map[string]any)
	for !sc.done() {
		sc.ws()
		if sc.done() {
			break
		}
		col, err := sc.readIdent()
		if err != nil {
			return nil, fmt.Errorf("WHERE clause: %w", err)
		}
		sc.ws()

		if sc.keyword("is") {
			// IS [NOT] NULL — exclude this column from the result
			sc.ws()
			_ = sc.keyword("not")
			sc.ws()
			if !sc.keyword("null") {
				return nil, fmt.Errorf("expected NULL after IS [NOT] for column %s", col)
			}
		} else {
			if sc.done() || sc.s[sc.i] != '=' {
				return nil, fmt.Errorf("expected '=' or IS after column %s in WHERE", col)
			}
			sc.i++
			val, err := sc.readValue(converter)
			if err != nil {
				return nil, fmt.Errorf("WHERE column %s: %w", col, err)
			}
			if val != nil {
				result[col] = val
			}
		}

		sc.ws()
		_ = sc.keyword("and")
		_ = sc.keyword("or")
	}
	return result, nil
}

// skipBareWord advances past a single bare identifier (no quotes, no parens).
func (sc *redoScanner) skipBareWord() {
	for !sc.done() && !isSpaceByte(sc.s[sc.i]) && sc.s[sc.i] != '(' && sc.s[sc.i] != '"' && sc.s[sc.i] != ';' {
		sc.i++
	}
}

func isSpaceByte(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }
func toLowerByte(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + 32
	}
	return b
}
