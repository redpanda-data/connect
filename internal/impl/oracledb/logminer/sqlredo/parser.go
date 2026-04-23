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
	"time"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// Parser parses SQL_REDO statements from Oracle LogMiner
// It handles the specific format that LogMiner produces:
//
//	INSERT: insert into "schema"."table"("C1","C2") values ('v1','v2');
//	UPDATE: update "schema"."table" set "C1" = 'v1', "C2" = 'v2' where "C1" = 'old1' and "C2" = 'old2';
//	DELETE: delete from "schema"."table" where "C1" = 'v1' and "C2" = 'v2';
type Parser struct {
	valueConverter OracleValueConverter
}

// NewParser creates a new Parser instance for parsing SQL_REDO statements.
// The parser handles Oracle LogMiner's specific SQL format and automatically converts
// Oracle SQL functions (TO_DATE, TO_TIMESTAMP, HEXTORAW, etc.) to their Go equivalents.
// All timestamp conversions use UTC timezone.
func NewParser() *Parser {
	return &Parser{
		valueConverter: NewOracleValueConverter(time.UTC),
	}
}

// RedoEventToDMLEvent converts a RedoEvent (from V$LOGMNR_CONTENTS) into a DMLEvent
func (p Parser) RedoEventToDMLEvent(redoEvent *RedoEvent) (DMLEvent, error) {
	if len(redoEvent.SQLRedo.String) == 0 {
		return DMLEvent{}, errors.New("empty SQL statement")
	}

	event := DMLEvent{
		Operation:     redoEvent.Operation,
		Timestamp:     redoEvent.Timestamp,
		TransactionID: redoEvent.TransactionID,
	}

	if redoEvent.SchemaName.Valid {
		event.Schema = redoEvent.SchemaName.String
	}
	if redoEvent.TableName.Valid {
		event.Table = redoEvent.TableName.String
	}

	// Store SQL_REDO - will need to parse this to extract column values
	if strings.TrimSpace(redoEvent.SQLRedo.String) != "" {
		event.SQLRedo = redoEvent.SQLRedo.String
	}

	// Parse SQL to AST
	stmt, err := ParseSQLCommand(redoEvent.SQLRedo.String)
	if err != nil {
		return DMLEvent{}, fmt.Errorf("parsing sql from redo log: %w", err)
	}

	// Extract values from AST, applying type conversion for bare (unquoted) values.
	newValues, oldValues, err := ExtractValuesFromAST(stmt, &p.valueConverter)
	if err != nil {
		return DMLEvent{}, fmt.Errorf("extracting values from AST: %w", err)
	}

	event.Data = newValues
	event.OldValues = oldValues

	return event, nil
}

// ParseSQLCommand parses the sql string and returns an AST for extracting key/values.
func ParseSQLCommand(sql string) (sqlparser.Statement, error) {
	// Normalize Oracle SQL to MySQL syntax
	normalized := normalizeOracleToMySQL(sql)

	stmt, err := sqlparser.Parse(normalized)
	if err != nil {
		return nil, fmt.Errorf("parsing sql command from logminer: %w", err)
	}

	return stmt, nil
}

// ExtractValuesFromAST extracts column->value mappings from a parsed statement.
// Returns newValues (for INSERT/UPDATE) and oldValues (for UPDATE/DELETE WHERE clauses).
// When converter is non-nil, bare (unquoted) values are passed through ConvertValue
// to produce typed Go values (e.g. numeric literals become int64 or json.Number).
// Quoted string literals are always returned as plain strings without conversion.
func ExtractValuesFromAST(stmt sqlparser.Statement, converter *OracleValueConverter) (newValues, oldValues map[string]any, err error) {
	switch s := stmt.(type) {
	case *sqlparser.Insert:
		newValues = extractInsertValues(s, converter)
	case *sqlparser.Update:
		newValues = extractUpdateSetValues(s, converter)
		oldValues = extractWhereValues(s.Where, converter)
	case *sqlparser.Delete:
		oldValues = extractWhereValues(s.Where, converter)
	default:
		return nil, nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
	return newValues, oldValues, nil
}

// extractInsertValues extracts column-value pairs from an INSERT statement.
// When converter is non-nil, bare values are passed through ConvertValue.
func extractInsertValues(stmt *sqlparser.Insert, converter *OracleValueConverter) map[string]any {
	result := make(map[string]any)

	// Get column names
	columns := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		columns[i] = sqlparser.String(col)
	}

	// Get values from the first row (LogMiner always has single row inserts)
	if values, ok := stmt.Rows.(sqlparser.Values); ok && len(values) > 0 {
		row := values[0]
		for i, val := range row {
			if i < len(columns) {
				valStr := sqlparser.String(val)
				if parsedVal := processValue(valStr, converter); parsedVal != nil {
					result[columns[i]] = parsedVal
				}
			}
		}
	}

	return result
}

// extractUpdateSetValues extracts column-value pairs from UPDATE SET clause.
// When converter is non-nil, bare values are passed through ConvertValue.
func extractUpdateSetValues(stmt *sqlparser.Update, converter *OracleValueConverter) map[string]any {
	result := make(map[string]any)

	for _, expr := range stmt.Exprs {
		colName := sqlparser.String(expr.Name)
		valStr := sqlparser.String(expr.Expr)
		if parsedVal := processValue(valStr, converter); parsedVal != nil {
			result[colName] = parsedVal
		}
	}

	return result
}

// extractWhereValues extracts column-value pairs from WHERE clause.
// Handles simple equality conditions like: WHERE col1 = 'val1' AND col2 = 'val2'
// When converter is non-nil, bare values are passed through ConvertValue.
func extractWhereValues(where *sqlparser.Where, converter *OracleValueConverter) map[string]any {
	if where == nil {
		return make(map[string]any)
	}

	result := make(map[string]any)
	extractWhereConditions(where.Expr, result, converter)
	return result
}

// extractWhereConditions recursively extracts conditions from WHERE expression
func extractWhereConditions(expr sqlparser.Expr, result map[string]any, converter *OracleValueConverter) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		extractWhereConditions(e.Left, result, converter)
		extractWhereConditions(e.Right, result, converter)

	case *sqlparser.OrExpr:
		extractWhereConditions(e.Left, result, converter)
		extractWhereConditions(e.Right, result, converter)

	case *sqlparser.ComparisonExpr:
		if e.Operator == "=" {
			if colName, ok := e.Left.(*sqlparser.ColName); ok {
				colStr := sqlparser.String(colName)
				valStr := sqlparser.String(e.Right)
				if parsedVal := processValue(valStr, converter); parsedVal != nil {
					result[colStr] = parsedVal
				}
			}
		}

	case *sqlparser.IsExpr:
		// IS NULL / IS NOT NULL - NULL values are not included in the map
	}
}

// processValue handles a SQL value string from the AST.
// Returns nil for NULL values (to exclude them from the map).
// For quoted string literals: strips quotes and returns as plain string (no conversion).
// For bare values (function calls, numeric literals): passes through converter if non-nil.
func processValue(valStr string, converter *OracleValueConverter) any {
	valStr = strings.TrimSpace(valStr)

	// Handle NULL - return nil to exclude from map
	if valStr == "NULL" || valStr == "Unsupported Type" {
		return nil
	}

	// Quoted string literal → strip quotes, return as plain string without conversion.
	// This preserves VARCHAR values like '12345' as string("12345").
	if len(valStr) >= 2 && valStr[0] == '\'' && valStr[len(valStr)-1] == '\'' {
		unquoted := valStr[1 : len(valStr)-1]
		unquoted = strings.ReplaceAll(unquoted, "\\'", "'")
		unquoted = strings.ReplaceAll(unquoted, "''", "'")
		unquoted = strings.ReplaceAll(unquoted, "\\\"", "\"")
		return unquoted
	}

	// Bare value (function call, numeric literal) → convert if converter available.
	if converter != nil {
		return converter.ConvertValue(valStr)
	}
	return valStr
}

// normalizeOracleToMySQL converts Oracle SQL syntax to MySQL syntax
// Main transformations:
// - Replace double quotes (") around identifiers with backticks (`) or remove them
// - Keep single quotes (') as-is for string literals
func normalizeOracleToMySQL(sql string) string {
	var result strings.Builder
	result.Grow(len(sql))

	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		switch ch {
		case '\'':
			// Single quote - toggle string literal state
			// Handle escaped quotes: ''
			if inDoubleQuote {
				// Single quote inside a double-quoted identifier - keep as-is
				result.WriteByte(ch)
			} else if i+1 < len(sql) && sql[i+1] == '\'' && inSingleQuote {
				// Escaped single quote inside string literal
				result.WriteByte(ch)
				result.WriteByte(sql[i+1])
				i++ // Skip next quote
			} else {
				inSingleQuote = !inSingleQuote
				result.WriteByte(ch)
			}

		case '"':
			if inSingleQuote {
				// Double quote inside string literal - keep as-is
				result.WriteByte(ch)
			} else {
				// Double quote for identifier - convert to MySQL backtick
				inDoubleQuote = !inDoubleQuote
				result.WriteByte('`')
			}

		default:
			result.WriteByte(ch)
		}
	}

	return result.String()
}
