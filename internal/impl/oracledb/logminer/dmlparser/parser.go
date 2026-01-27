/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package dmlparser

import (
	"errors"
	"fmt"
	"strings"
)

// LogMinerDMLParser parses SQL_REDO statements from Oracle LogMiner
// It handles the specific format that LogMiner produces:
//
//	INSERT: insert into "schema"."table"("C1","C2") values ('v1','v2');
//	UPDATE: update "schema"."table" set "C1" = 'v1', "C2" = 'v2' where "C1" = 'old1' and "C2" = 'old2';
//	DELETE: delete from "schema"."table" where "C1" = 'v1' and "C2" = 'v2';
type LogMinerDMLParser struct {
	useRelaxedQuotes bool
}

func New(useRelaxedQuotes bool) *LogMinerDMLParser {
	return &LogMinerDMLParser{
		useRelaxedQuotes: useRelaxedQuotes,
	}
}

// ParseResult represents the parsed DML statement
type ParseResult struct {
	Operation  string         // INSERT, UPDATE, DELETE
	Schema     string         // Schema name
	Table      string         // Table name
	NewValues  map[string]any // After-state values (INSERT, UPDATE)
	OldValues  map[string]any // Before-state values (UPDATE, DELETE)
	ColumnList []string       // Column names in order
}

// Parse parses a SQL_REDO statement
func (p *LogMinerDMLParser) Parse(sql string) (*ParseResult, error) {
	if len(sql) == 0 {
		return nil, errors.New("empty SQL statement")
	}

	// Determine operation type by first character
	switch sql[0] {
	case 'i':
		return p.parseInsert(sql)
	case 'u':
		return p.parseUpdate(sql)
	case 'd':
		return p.parseDelete(sql)
	default:
		return nil, fmt.Errorf("unknown SQL operation: %s", sql)
	}
}

// parseInsert parses: insert into "schema"."table"("C1","C2") values ('v1','v2');
func (p *LogMinerDMLParser) parseInsert(sql string) (*ParseResult, error) {
	const insertInto = "insert into "
	if !strings.HasPrefix(sql, insertInto) {
		return nil, errors.New("invalid INSERT statement")
	}

	index := len(insertInto)

	// Parse schema.table
	schema, table, nextIdx, err := p.parseTableName(sql, index)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table name: %w", err)
	}
	index = nextIdx

	// Parse column list: ("C1","C2")
	columnList, nextIdx, err := p.parseColumnList(sql, index)
	if err != nil {
		return nil, fmt.Errorf("failed to parse column list: %w", err)
	}
	index = nextIdx

	// Parse values clause: values ('v1','v2')
	values, err := p.parseValuesClause(sql, index, columnList)
	if err != nil {
		return nil, fmt.Errorf("failed to parse values clause: %w", err)
	}

	return &ParseResult{
		Operation:  "INSERT",
		Schema:     schema,
		Table:      table,
		ColumnList: columnList,
		NewValues:  values,
	}, nil
}

// parseUpdate parses: update "schema"."table" set "C1" = 'v1', "C2" = 'v2' where "C1" = 'old1' and "C2" = 'old2';
func (p *LogMinerDMLParser) parseUpdate(sql string) (*ParseResult, error) {
	const update = "update "
	if !strings.HasPrefix(sql, update) {
		return nil, errors.New("invalid UPDATE statement")
	}

	index := len(update)

	// Parse schema.table
	schema, table, nextIdx, err := p.parseTableName(sql, index)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table name: %w", err)
	}
	index = nextIdx

	// Parse SET clause
	newValues, nextIdx, err := p.parseSetClause(sql, index)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SET clause: %w", err)
	}
	index = nextIdx

	// Parse WHERE clause
	oldValues, err := p.parseWhereClause(sql, index)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	return &ParseResult{
		Operation: "UPDATE",
		Schema:    schema,
		Table:     table,
		NewValues: newValues,
		OldValues: oldValues,
	}, nil
}

// parseDelete parses: delete from "schema"."table" where "C1" = 'v1' and "C2" = 'v2';
func (p *LogMinerDMLParser) parseDelete(sql string) (*ParseResult, error) {
	const deleteFrom = "delete from "
	if !strings.HasPrefix(sql, deleteFrom) {
		return nil, errors.New("invalid DELETE statement")
	}

	index := len(deleteFrom)

	// Parse schema.table
	schema, table, nextIdx, err := p.parseTableName(sql, index)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table name: %w", err)
	}
	index = nextIdx

	// Parse WHERE clause
	oldValues, err := p.parseWhereClause(sql, index)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	return &ParseResult{
		Operation: "DELETE",
		Schema:    schema,
		Table:     table,
		OldValues: oldValues,
	}, nil
}

// parseTableName parses "schema"."table" or just "table"
func (*LogMinerDMLParser) parseTableName(sql string, index int) (schema, table string, nextIndex int, err error) {
	// Look for first quoted identifier
	inQuote := false
	parts := []string{}
	partStart := 0

	for i := index; i < len(sql); i++ {
		c := sql[i]

		if c == '"' {
			if inQuote {
				// End of quoted identifier
				parts = append(parts, sql[partStart+1:i])
				inQuote = false
			} else {
				// Start of quoted identifier
				partStart = i
				inQuote = true
			}
		} else if !inQuote && (c == ' ' || c == '(') {
			// End of table name
			nextIndex = i
			break
		}
	}

	if len(parts) == 0 {
		return "", "", index, errors.New("failed to parse table name")
	}

	if len(parts) == 1 {
		// Just table name
		return "", parts[0], nextIndex, nil
	}

	// Schema.table
	return parts[0], parts[1], nextIndex, nil
}

// parseColumnList parses ("C1","C2","C3")
func (*LogMinerDMLParser) parseColumnList(sql string, index int) ([]string, int, error) {
	// Find opening parenthesis
	for index < len(sql) && sql[index] != '(' {
		index++
	}
	if index >= len(sql) {
		return nil, index, errors.New("column list not found")
	}
	index++ // skip '('

	columns := []string{}
	inQuote := false
	colStart := 0

	for i := index; i < len(sql); i++ {
		c := sql[i]

		if c == '"' {
			if inQuote {
				// End of column name
				columns = append(columns, sql[colStart+1:i])
				inQuote = false
			} else {
				// Start of column name
				colStart = i
				inQuote = true
			}
		} else if c == ')' && !inQuote {
			// End of column list
			return columns, i + 1, nil
		}
	}

	return nil, index, errors.New("unterminated column list")
}

// parseValuesClause parses values ('v1','v2',NULL,TO_DATE('2020-01-01','YYYY-MM-DD'))
func (*LogMinerDMLParser) parseValuesClause(sql string, index int, columnNames []string) (map[string]any, error) {
	// Find "values"
	valuesIdx := strings.Index(sql[index:], " values ")
	if valuesIdx == -1 {
		return nil, errors.New("values clause not found")
	}
	index += valuesIdx + len(" values ")

	// Find opening parenthesis
	for index < len(sql) && sql[index] != '(' {
		index++
	}
	if index >= len(sql) {
		return nil, errors.New("values list not found")
	}
	index++ // skip '('

	values := make(map[string]any)
	valueIdx := 0
	inSingleQuote := false
	nested := 0
	valueStart := index
	var collectedValue strings.Builder
	isQuotedValue := false

	for i := index; i < len(sql); i++ {
		c := sql[i]
		lookAhead := byte(0)
		if i+1 < len(sql) {
			lookAhead = sql[i+1]
		}

		if inSingleQuote {
			if c == '\'' {
				if lookAhead == '\'' {
					// Escaped single quote - add one quote to result
					collectedValue.WriteByte('\'')
					i++
					continue
				}
				// End of quoted value
				inSingleQuote = false
				continue
			}
			collectedValue.WriteByte(c)
			continue
		}

		// Not in single quote
		if c == '\'' && nested == 0 {
			// Start of a quoted string value
			inSingleQuote = true
			isQuotedValue = true
			collectedValue.Reset()
		} else if c == '(' {
			// Start of function call or nested expression
			nested++
		} else if c == ')' {
			if nested > 0 {
				// End of nested function/expression
				nested--
			} else {
				// End of values list - handle last value
				if isQuotedValue {
					// Was quoted - use collected value (even if empty string)
					if valueIdx < len(columnNames) {
						values[columnNames[valueIdx]] = collectedValue.String()
					}
				} else {
					// Was unquoted (NULL, function call, etc.)
					val := strings.TrimSpace(sql[valueStart:i])
					if val != "NULL" && val != "Unsupported Type" && val != "" {
						if valueIdx < len(columnNames) {
							values[columnNames[valueIdx]] = val
						}
					}
				}
				return values, nil
			}
		} else if c == ',' && nested == 0 {
			// End of current value
			if isQuotedValue {
				// Was a quoted value - use collected value
				if valueIdx < len(columnNames) {
					values[columnNames[valueIdx]] = collectedValue.String()
				}
				collectedValue.Reset()
			} else {
				// Was an unquoted value (NULL, function call, etc.)
				val := strings.TrimSpace(sql[valueStart:i])
				if val != "NULL" && val != "Unsupported Type" && val != "" {
					if valueIdx < len(columnNames) {
						values[columnNames[valueIdx]] = val
					}
				}
			}
			valueIdx++
			valueStart = i + 1
			isQuotedValue = false
		}
	}

	return values, nil
}

// parseSetClause parses set "C1" = 'v1', "C2" = 'v2', "C3" = NULL
func (*LogMinerDMLParser) parseSetClause(sql string, index int) (map[string]any, int, error) {
	// Find " set "
	setIdx := strings.Index(sql[index:], " set ")
	if setIdx == -1 {
		return nil, index, errors.New("SET clause not found")
	}
	index += setIdx + len(" set ")

	values := make(map[string]any)
	var currentColumn string
	var collectedValue strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	isQuotedValue := false
	nested := 0
	valueStart := 0
	state := "column" // "column", "equals", "value", "comma"

	for i := index; i < len(sql); i++ {
		c := sql[i]
		lookAhead := byte(0)
		if i+1 < len(sql) {
			lookAhead = sql[i+1]
		}

		// Check for WHERE clause (return index of space before "where")
		if c == 'w' && i > 0 && sql[i-1] == ' ' && strings.HasPrefix(sql[i:], "where ") {
			return values, i - 1, nil
		}

		// Handle single-quoted values
		if inSingleQuote {
			if c == '\'' {
				if lookAhead == '\'' {
					collectedValue.WriteByte('\'')
					i++
					continue
				}
				inSingleQuote = false
				continue
			}
			collectedValue.WriteByte(c)
			continue
		}

		// Handle double-quoted column names
		if inDoubleQuote {
			if c == '"' {
				currentColumn = sql[valueStart+1 : i]
				inDoubleQuote = false
				state = "equals"
			}
			continue
		}

		// State machine
		switch state {
		case "column":
			if c == '"' {
				inDoubleQuote = true
				valueStart = i
			} else if c == ',' || c == ' ' {
				// Skip whitespace and commas between assignments
				continue
			}

		case "equals":
			if c == '=' {
				state = "value"
				// Skip spaces after =
				for i+1 < len(sql) && sql[i+1] == ' ' {
					i++
				}
				valueStart = i + 1
				isQuotedValue = false
				collectedValue.Reset()
			}

		case "value":
			if c == '\'' && nested == 0 {
				inSingleQuote = true
				isQuotedValue = true
			} else if c == '(' {
				nested++
			} else if c == ')' && nested > 0 {
				nested--
			} else if (c == ',' || c == ' ') && nested == 0 {
				// End of value - store it
				if isQuotedValue {
					values[currentColumn] = collectedValue.String()
					state = "column"
				} else {
					val := strings.TrimSpace(sql[valueStart:i])
					if val == "NULL" {
						values[currentColumn] = nil
						state = "column"
					} else if val != "Unsupported Type" && val != "" {
						values[currentColumn] = val
						state = "column"
					}
				}
			}
		}
	}

	// Handle last value if we're at end
	if state == "value" {
		if isQuotedValue {
			values[currentColumn] = collectedValue.String()
		} else {
			val := strings.TrimSpace(sql[valueStart:])
			if val == "NULL" {
				values[currentColumn] = nil
			} else if val != "Unsupported Type" && val != "" && !strings.HasSuffix(val, ";") {
				values[currentColumn] = strings.TrimRight(val, ";")
			}
		}
	}

	return values, len(sql), nil
}

// parseWhereClause parses where "C1" = 'v1' and "C2" = 'v2' and "C3" IS NULL
func (*LogMinerDMLParser) parseWhereClause(sql string, index int) (map[string]any, error) {
	// Find " where "
	whereIdx := strings.Index(sql[index:], " where ")
	if whereIdx == -1 {
		// No WHERE clause (DBZ-3235 - LogMiner can generate SQL without WHERE)
		return make(map[string]any), nil
	}
	index += whereIdx + len(" where ")

	values := make(map[string]any)
	var currentColumn string
	var collectedValue strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	isQuotedValue := false
	nested := 0
	valueStart := 0
	state := "column" // "column", "equals", "value", "and"

	for i := index; i < len(sql); i++ {
		c := sql[i]
		lookAhead := byte(0)
		if i+1 < len(sql) {
			lookAhead = sql[i+1]
		}

		// Handle single-quoted values
		if inSingleQuote {
			if c == '\'' {
				if lookAhead == '\'' {
					collectedValue.WriteByte('\'')
					i++
					continue
				}
				inSingleQuote = false
				continue
			}
			collectedValue.WriteByte(c)
			continue
		}

		// Handle double-quoted column names
		if inDoubleQuote {
			if c == '"' {
				currentColumn = sql[valueStart+1 : i]
				inDoubleQuote = false
				state = "equals"
			}
			continue
		}

		// State machine
		switch state {
		case "column":
			if c == '"' {
				inDoubleQuote = true
				valueStart = i
			} else if c == ' ' {
				// Skip whitespace
				continue
			}

		case "equals":
			if c == '=' {
				state = "value"
				// Skip spaces after =
				for i+1 < len(sql) && sql[i+1] == ' ' {
					i++
				}
				valueStart = i + 1
				isQuotedValue = false
				collectedValue.Reset()
			} else if c == 'I' && strings.HasPrefix(sql[i:], "IS NULL") {
				values[currentColumn] = nil
				i += len("IS NULL") - 1
				state = "and"
			}

		case "value":
			if c == '\'' && nested == 0 {
				inSingleQuote = true
				isQuotedValue = true
			} else if c == '(' {
				nested++
			} else if c == ')' && nested > 0 {
				nested--
			} else if (c == ' ' || c == ';') && nested == 0 {
				// End of value
				if isQuotedValue {
					values[currentColumn] = collectedValue.String()
				} else {
					val := strings.TrimSpace(sql[valueStart:i])
					if val == "NULL" {
						values[currentColumn] = nil
					} else if val != "Unsupported Type" && val != "" {
						values[currentColumn] = val
					}
				}
				state = "and"
			}

		case "and":
			if c == ';' {
				return values, nil
			} else if c == ' ' {
				// Skip whitespace
				continue
			} else if strings.HasPrefix(sql[i:], "and ") {
				i += 3 // skip "and "
				state = "column"
			} else if strings.HasPrefix(sql[i:], "or ") {
				i += 2 // skip "or "
				state = "column"
			}
		}
	}

	// Handle last value if we're at end
	if state == "value" {
		if isQuotedValue {
			values[currentColumn] = collectedValue.String()
		} else {
			val := strings.TrimSpace(sql[valueStart:])
			val = strings.TrimRight(val, ";")
			if val == "NULL" {
				values[currentColumn] = nil
			} else if val != "Unsupported Type" && val != "" {
				values[currentColumn] = val
			}
		}
	}

	return values, nil
}
