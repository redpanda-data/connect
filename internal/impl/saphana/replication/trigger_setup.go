package replication

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

const cdcChangesCreateSchemaSQL = `DO BEGIN
    DECLARE schema_exists CONDITION FOR SQL_ERROR_CODE 386;
    DECLARE EXIT HANDLER FOR schema_exists BEGIN END;
    EXEC 'CREATE SCHEMA _RPCN_CDC';
END`

const cdcChangesCreateTableSQL = `DO BEGIN
    DECLARE tbl_exists CONDITION FOR SQL_ERROR_CODE 288;
    DECLARE EXIT HANDLER FOR tbl_exists BEGIN END;
    EXEC 'CREATE COLUMN TABLE _RPCN_CDC.CHANGES (
        ID          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        OP          VARCHAR(1)  NOT NULL,
        SCHEMA_NAME VARCHAR(256) NOT NULL,
        TABLE_NAME  VARCHAR(256) NOT NULL,
        OP_TIME     TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
        PK_JSON     NCLOB,
        OLD_VALUES  NCLOB,
        NEW_VALUES  NCLOB
    )';
END`

var identRe = regexp.MustCompile(`^[A-Za-z_$#][A-Za-z0-9_$#]{0,127}$`)

// ValidateTargetTable validates that schema and table identifiers conform to HANA naming rules.
func ValidateTargetTable(schema, table string) error {
	if !identRe.MatchString(schema) {
		return fmt.Errorf("invalid schema identifier %q", schema)
	}
	if !identRe.MatchString(table) {
		return fmt.Errorf("invalid table identifier %q", table)
	}
	return nil
}

// TriggerName constructs a HANA trigger name from schema, table, and operation type.
func TriggerName(schema, table, op string) string {
	prefix := "_RPCN_"
	suffix := "_" + op
	maxBase := 127 - len(prefix) - len(suffix)
	base := strings.ToUpper(schema) + "_" + strings.ToUpper(table)
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	return prefix + base + suffix
}

// quoteIdent wraps a HANA identifier in ANSI double-quotes, escaping any
// embedded double-quotes by doubling them (SQL standard).
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func buildJSONObject(columns []string, rowPrefix string) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		quoted := quoteIdent(c)
		// The JSON key in the VARCHAR literal also needs single-quote escaping.
		jsonKey := strings.ReplaceAll(c, "'", "''")
		parts[i] = fmt.Sprintf("'%s' VALUE %s.%s", jsonKey, rowPrefix, quoted)
	}
	return "JSON_OBJECT(" + strings.Join(parts, ", ") + ")"
}

// buildPKJSON constructs a JSON object for primary key columns or NULL if empty.
func buildPKJSON(pkCols []string, rowPrefix string) string {
	if len(pkCols) == 0 {
		return "NULL"
	}
	return buildJSONObject(pkCols, rowPrefix)
}

// BuildInsertTriggerSQL generates a CREATE TRIGGER statement for INSERT events.
func BuildInsertTriggerSQL(schema, table string, columns []string, pkCols ...string) string {
	return fmt.Sprintf(`CREATE OR REPLACE TRIGGER %s
AFTER INSERT ON %s.%s
FOR EACH ROW
BEGIN
    INSERT INTO _RPCN_CDC.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, NEW_VALUES)
    VALUES ('I', '%s', '%s', %s, %s);
END`,
		TriggerName(schema, table, "ins"), quoteIdent(schema), quoteIdent(table), schema, table,
		buildPKJSON(pkCols, ":new"), buildJSONObject(columns, ":new"))
}

// BuildUpdateTriggerSQL generates a CREATE TRIGGER statement for UPDATE events.
func BuildUpdateTriggerSQL(schema, table string, columns []string, pkCols ...string) string {
	return fmt.Sprintf(`CREATE OR REPLACE TRIGGER %s
AFTER UPDATE ON %s.%s
FOR EACH ROW
BEGIN
    INSERT INTO _RPCN_CDC.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, OLD_VALUES, NEW_VALUES)
    VALUES ('U', '%s', '%s', %s, %s, %s);
END`,
		TriggerName(schema, table, "upd"), quoteIdent(schema), quoteIdent(table), schema, table,
		buildPKJSON(pkCols, ":old"), buildJSONObject(columns, ":old"), buildJSONObject(columns, ":new"))
}

// BuildDeleteTriggerSQL generates a CREATE TRIGGER statement for DELETE events.
func BuildDeleteTriggerSQL(schema, table string, columns []string, pkCols ...string) string {
	return fmt.Sprintf(`CREATE OR REPLACE TRIGGER %s
AFTER DELETE ON %s.%s
FOR EACH ROW
BEGIN
    INSERT INTO _RPCN_CDC.CHANGES (OP, SCHEMA_NAME, TABLE_NAME, PK_JSON, OLD_VALUES)
    VALUES ('D', '%s', '%s', %s, %s);
END`,
		TriggerName(schema, table, "del"), quoteIdent(schema), quoteIdent(table), schema, table,
		buildPKJSON(pkCols, ":old"), buildJSONObject(columns, ":old"))
}

// SetupCDCInfrastructure creates the shared _RPCN_CDC schema and CHANGES table.
// Call this once before any SetupCDC calls. Idempotent.
func SetupCDCInfrastructure(ctx context.Context, db *sql.DB) error {
	for _, stmt := range []string{cdcChangesCreateSchemaSQL, cdcChangesCreateTableSQL} {
		if _, err := db.ExecContext(ctx, strings.TrimSpace(stmt)); err != nil {
			return fmt.Errorf("CDC infrastructure setup: %w", err)
		}
	}
	return nil
}

// SetupCDC installs INSERT/UPDATE/DELETE triggers for schema.table.
// Call SetupCDCInfrastructure once before calling this in a loop.
func SetupCDC(ctx context.Context, db *sql.DB, schema, table string, columns []string, pkCols []string) error {
	if err := ValidateTargetTable(schema, table); err != nil {
		return err
	}
	// Only install the triggers — call SetupCDCInfrastructure once before this loop.
	for _, stmt := range []string{
		BuildInsertTriggerSQL(schema, table, columns, pkCols...),
		BuildUpdateTriggerSQL(schema, table, columns, pkCols...),
		BuildDeleteTriggerSQL(schema, table, columns, pkCols...),
	} {
		if _, err := db.ExecContext(ctx, strings.TrimSpace(stmt)); err != nil {
			return fmt.Errorf("CDC DDL for %s.%s: %w", schema, table, err)
		}
	}
	return nil
}
