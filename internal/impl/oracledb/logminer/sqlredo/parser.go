//
// Copyright Debezium Authors.
//
// Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
//

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
	valueConverter *OracleValueConverter
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
// by parsing the SQL_REDO statement and extracting column values. The function:
//   - Parses INSERT, UPDATE, and DELETE statements from the SQL_REDO field
//   - Extracts schema, table, and column data from the parsed SQL
//   - Converts Oracle SQL functions (TO_DATE, TO_TIMESTAMP, HEXTORAW, etc.) to Go types
//   - Returns an error if the SQL_REDO field is empty or the statement cannot be parsed
func (p *Parser) RedoEventToDMLEvent(redoEvent *RedoEvent) (*DMLEvent, error) {
	if len(redoEvent.SQLRedo.String) == 0 {
		return nil, errors.New("empty SQL statement")
	}

	event := &DMLEvent{
		Operation: redoEvent.Operation,
		Timestamp: redoEvent.Timestamp,
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
		return nil, fmt.Errorf("parsing sql from redo log: %w", err)
	}

	// Extract values from AST
	newValues, _, err := ExtractValuesFromAST(stmt)
	if err != nil {
		return nil, fmt.Errorf("extracting values from AST: %w", err)
	}

	event.Data = make(map[string]any, len(newValues))
	for k, v := range newValues {
		// Convert Oracle SQL types (TO_DATE, TO_TIMESTAMP, etc.) to their Go equivalents
		event.Data[k] = p.valueConverter.ConvertValue(v, k)
	}

	return event, nil
}

// ParseSQLCommand parses the sql string and returns an AST for extracting key/values.
func ParseSQLCommand(sql string) (sqlparser.Statement, error) {
	// Normalize Oracle SQL to MySQL syntax
	normalized := normalizeOracleToMySQL(sql)

	stmt, err := sqlparser.Parse(normalized)
	if err != nil {
		return nil, fmt.Errorf("parsing sql: %w", err)
	}

	return stmt, nil
}

// ExtractValuesFromAST extracts column->value mappings from a parsed statement.
// Returns newValues (for INSERT/UPDATE) and oldValues (for UPDATE/DELETE WHERE clauses).
func ExtractValuesFromAST(stmt sqlparser.Statement) (newValues, oldValues map[string]any, err error) {
	switch s := stmt.(type) {
	case *sqlparser.Insert:
		newValues = extractInsertValues(s)
	case *sqlparser.Update:
		newValues = extractUpdateSetValues(s)
		oldValues = extractWhereValues(s.Where)
	case *sqlparser.Delete:
		oldValues = extractWhereValues(s.Where)
	default:
		return nil, nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
	return newValues, oldValues, nil
}

// extractInsertValues extracts column-value pairs from an INSERT statement
func extractInsertValues(stmt *sqlparser.Insert) map[string]any {
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
				// Convert the value expression to a string representation
				valStr := sqlparser.String(val)
				// Strip quotes from string literals, keep functions/NULL as-is
				parsedVal := stripQuotesFromValue(valStr)
				if parsedVal != nil {
					result[columns[i]] = parsedVal
				}
			}
		}
	}

	return result
}

// extractUpdateSetValues extracts column-value pairs from UPDATE SET clause
func extractUpdateSetValues(stmt *sqlparser.Update) map[string]any {
	result := make(map[string]any)

	for _, expr := range stmt.Exprs {
		colName := sqlparser.String(expr.Name)
		valStr := sqlparser.String(expr.Expr)
		// Strip quotes from string literals, keep functions/NULL as-is
		parsedVal := stripQuotesFromValue(valStr)
		if parsedVal != nil {
			result[colName] = parsedVal
		}
	}

	return result
}

// extractWhereValues extracts column-value pairs from WHERE clause
// Handles simple equality conditions like: WHERE col1 = 'val1' AND col2 = 'val2'
func extractWhereValues(where *sqlparser.Where) map[string]any {
	if where == nil {
		return make(map[string]any)
	}

	result := make(map[string]any)
	extractWhereConditions(where.Expr, result)
	return result
}

// extractWhereConditions recursively extracts conditions from WHERE expression
func extractWhereConditions(expr sqlparser.Expr, result map[string]any) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		// Handle AND: recursively process left and right
		extractWhereConditions(e.Left, result)
		extractWhereConditions(e.Right, result)

	case *sqlparser.OrExpr:
		// Handle OR: recursively process left and right
		extractWhereConditions(e.Left, result)
		extractWhereConditions(e.Right, result)

	case *sqlparser.ComparisonExpr:
		// Handle comparison: col = 'value'
		if e.Operator == "=" {
			if colName, ok := e.Left.(*sqlparser.ColName); ok {
				colStr := sqlparser.String(colName)
				valStr := sqlparser.String(e.Right)
				// Strip quotes from string literals, keep functions/NULL as-is
				parsedVal := stripQuotesFromValue(valStr)
				if parsedVal != nil {
					result[colStr] = parsedVal
				}
			}
		}

	case *sqlparser.IsExpr:
		// IS NULL / IS NOT NULL - NULL values are not included in the map
	}
}

// stripQuotesFromValue removes quotes from string literals and handles escaped quotes.
// Returns nil for NULL values (to exclude them from the map, matching old parser behavior).
// Keeps function calls and other non-string values as-is.
func stripQuotesFromValue(valStr string) any {
	valStr = strings.TrimSpace(valStr)

	// Handle NULL - return nil to exclude from map
	if valStr == "NULL" || valStr == "Unsupported Type" {
		return nil
	}

	// If it's a quoted string literal, strip quotes and handle escapes
	if len(valStr) >= 2 && valStr[0] == '\'' && valStr[len(valStr)-1] == '\'' {
		// Strip outer quotes
		unquoted := valStr[1 : len(valStr)-1]
		// Handle escaped single quotes: \' -> ' and '' -> '
		unquoted = strings.ReplaceAll(unquoted, "\\'", "'")
		unquoted = strings.ReplaceAll(unquoted, "''", "'")
		// Handle escaped double quotes: \" -> "
		unquoted = strings.ReplaceAll(unquoted, "\\\"", "\"")
		return unquoted
	}

	// Not a quoted string - return as-is (function calls, etc.)
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
			if i+1 < len(sql) && sql[i+1] == '\'' && inSingleQuote {
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
				// Double quote for identifier - remove it (or could replace with backtick)
				// For simple identifiers, MySQL doesn't require quotes
				inDoubleQuote = !inDoubleQuote
				// Skip the double quote (don't write it)
			}

		default:
			result.WriteByte(ch)
		}
	}

	return result.String()
}
