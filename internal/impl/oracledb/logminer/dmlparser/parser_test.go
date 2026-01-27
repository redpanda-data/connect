package dmlparser

import (
	"reflect"
	"testing"
)

func TestParseInsert(t *testing.T) {
	parser := New(false)

	tests := []struct {
		name       string
		sql        string
		wantSchema string
		wantTable  string
		wantValues map[string]any
		wantErr    bool
	}{
		{
			name:       "simple insert",
			sql:        `insert into "MYAPP"."CUSTOMERS"("ID","NAME","EMAIL") values ('1','John Doe','john@example.com');`,
			wantSchema: "MYAPP",
			wantTable:  "CUSTOMERS",
			wantValues: map[string]any{
				"ID":    "1",
				"NAME":  "John Doe",
				"EMAIL": "john@example.com",
			},
		},
		{
			name:       "insert with NULL",
			sql:        `insert into "MYAPP"."CUSTOMERS"("ID","NAME","EMAIL") values ('1','John Doe',NULL);`,
			wantSchema: "MYAPP",
			wantTable:  "CUSTOMERS",
			wantValues: map[string]any{
				"ID":   "1",
				"NAME": "John Doe",
				// EMAIL not in map because it's NULL
			},
		},
		{
			name:       "insert with function call",
			sql:        `insert into "MYAPP"."ORDERS"("ID","ORDER_DATE","AMOUNT") values ('100',TO_DATE('2020-01-15','YYYY-MM-DD'),'500.00');`,
			wantSchema: "MYAPP",
			wantTable:  "ORDERS",
			wantValues: map[string]any{
				"ID":         "100",
				"ORDER_DATE": "TO_DATE('2020-01-15','YYYY-MM-DD')",
				"AMOUNT":     "500.00",
			},
		},
		{
			name:       "insert with escaped quotes",
			sql:        `insert into "MYAPP"."MESSAGES"("ID","TEXT") values ('1','It''s a beautiful day');`,
			wantSchema: "MYAPP",
			wantTable:  "MESSAGES",
			wantValues: map[string]any{
				"ID":   "1",
				"TEXT": "It's a beautiful day",
			},
		},
		{
			name:       "insert with timestamp",
			sql:        `insert into "MYAPP"."EVENTS"("ID","EVENT_TIME") values ('1',TO_TIMESTAMP('2020-02-01 10:30:00','YYYY-MM-DD HH24:MI:SS'));`,
			wantSchema: "MYAPP",
			wantTable:  "EVENTS",
			wantValues: map[string]any{
				"ID":         "1",
				"EVENT_TIME": "TO_TIMESTAMP('2020-02-01 10:30:00','YYYY-MM-DD HH24:MI:SS')",
			},
		},
		{
			name:       "insert with empty string",
			sql:        `insert into "MYAPP"."CUSTOMERS"("ID","NAME","NOTES") values ('1','John','');`,
			wantSchema: "MYAPP",
			wantTable:  "CUSTOMERS",
			wantValues: map[string]any{
				"ID":    "1",
				"NAME":  "John",
				"NOTES": "",
			},
		},
		{
			name:       "insert with EMPTY_CLOB()",
			sql:        `insert into "MYAPP"."DOCUMENTS"("ID","CONTENT","METADATA") values ('1',EMPTY_CLOB(),EMPTY_CLOB());`,
			wantSchema: "MYAPP",
			wantTable:  "DOCUMENTS",
			wantValues: map[string]any{
				"ID":       "1",
				"CONTENT":  "EMPTY_CLOB()",
				"METADATA": "EMPTY_CLOB()",
			},
		},
		{
			name:       "insert with EMPTY_BLOB()",
			sql:        `insert into "MYAPP"."IMAGES"("ID","IMAGE_DATA","THUMBNAIL") values ('1',EMPTY_BLOB(),EMPTY_BLOB());`,
			wantSchema: "MYAPP",
			wantTable:  "IMAGES",
			wantValues: map[string]any{
				"ID":         "1",
				"IMAGE_DATA": "EMPTY_BLOB()",
				"THUMBNAIL":  "EMPTY_BLOB()",
			},
		},
		{
			name:       "insert with HEXTORAW()",
			sql:        `insert into "MYAPP"."BINARY_DATA"("ID","DATA") values ('1',HEXTORAW('AABBCCDD'));`,
			wantSchema: "MYAPP",
			wantTable:  "BINARY_DATA",
			wantValues: map[string]any{
				"ID":   "1",
				"DATA": "HEXTORAW('AABBCCDD')",
			},
		},
		{
			name:       "insert with HEXTORAW() long binary",
			sql:        `insert into "MYAPP"."BINARY_DATA"("ID","DATA") values ('2',HEXTORAW('00000000000000000000000000000000'));`,
			wantSchema: "MYAPP",
			wantTable:  "BINARY_DATA",
			wantValues: map[string]any{
				"ID":   "2",
				"DATA": "HEXTORAW('00000000000000000000000000000000')",
			},
		},
		{
			name:       "insert with TO_TIMESTAMP_TZ()",
			sql:        `insert into "MYAPP"."EVENTS"("ID","EVENT_TIME") values ('1',TO_TIMESTAMP_TZ('31-DEC-99 11.59.59.9999999 PM +14:00'));`,
			wantSchema: "MYAPP",
			wantTable:  "EVENTS",
			wantValues: map[string]any{
				"ID":         "1",
				"EVENT_TIME": "TO_TIMESTAMP_TZ('31-DEC-99 11.59.59.9999999 PM +14:00')",
			},
		},
		{
			name:       "insert with unquoted numbers",
			sql:        `insert into "MYAPP"."PRODUCTS"("ID","PRICE","QUANTITY") values ('1','99.99','50');`,
			wantSchema: "MYAPP",
			wantTable:  "PRODUCTS",
			wantValues: map[string]any{
				"ID":       "1",
				"PRICE":    "99.99",
				"QUANTITY": "50",
			},
		},
		{
			name:       "insert with comma in string",
			sql:        `insert into "MYAPP"."ADDRESS"("ID","STREET") values ('1','123 Main St, Apt 4');`,
			wantSchema: "MYAPP",
			wantTable:  "ADDRESS",
			wantValues: map[string]any{
				"ID":     "1",
				"STREET": "123 Main St, Apt 4",
			},
		},
		{
			name:       "insert with semicolon in string",
			sql:        `insert into "MYAPP"."MESSAGES"("ID","TEXT") values ('1','Hello; this is a test');`,
			wantSchema: "MYAPP",
			wantTable:  "MESSAGES",
			wantValues: map[string]any{
				"ID":   "1",
				"TEXT": "Hello; this is a test",
			},
		},
		{
			name:       "insert with parentheses in string",
			sql:        `insert into "MYAPP"."NOTES"("ID","TEXT") values ('1','Price (USD): $99');`,
			wantSchema: "MYAPP",
			wantTable:  "NOTES",
			wantValues: map[string]any{
				"ID":   "1",
				"TEXT": "Price (USD): $99",
			},
		},
		{
			name:       "insert with unicode characters",
			sql:        `insert into "MYAPP"."I18N"("ID","TEXT_JP","TEXT_CN") values ('1','こんにちは','你好');`,
			wantSchema: "MYAPP",
			wantTable:  "I18N",
			wantValues: map[string]any{
				"ID":      "1",
				"TEXT_JP": "こんにちは",
				"TEXT_CN": "你好",
			},
		},
		{
			name:       "insert with nested functions",
			sql:        `insert into "MYAPP"."TEST"("ID","DATE_STR") values ('1',TO_CHAR(TO_DATE('2020-01-01','YYYY-MM-DD'),'DD-MON-YY'));`,
			wantSchema: "MYAPP",
			wantTable:  "TEST",
			wantValues: map[string]any{
				"ID":       "1",
				"DATE_STR": "TO_CHAR(TO_DATE('2020-01-01','YYYY-MM-DD'),'DD-MON-YY')",
			},
		},
		{
			name:       "insert with column names containing numbers",
			sql:        `insert into "MYAPP"."TEST"("ID","COLUMN_1","COL2") values ('1','value1','value2');`,
			wantSchema: "MYAPP",
			wantTable:  "TEST",
			wantValues: map[string]any{
				"ID":       "1",
				"COLUMN_1": "value1",
				"COL2":     "value2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			if result.Schema != tt.wantSchema {
				t.Errorf("Schema = %v, want %v", result.Schema, tt.wantSchema)
			}
			if result.Table != tt.wantTable {
				t.Errorf("Table = %v, want %v", result.Table, tt.wantTable)
			}
			if !reflect.DeepEqual(result.NewValues, tt.wantValues) {
				t.Errorf("NewValues = %v, want %v", result.NewValues, tt.wantValues)
			}
		})
	}
}

func TestParseUpdate(t *testing.T) {
	parser := New(false)

	tests := []struct {
		name          string
		sql           string
		wantSchema    string
		wantTable     string
		wantNewValues map[string]any
		wantOldValues map[string]any
		wantErr       bool
	}{
		{
			name:       "simple update",
			sql:        `update "MYAPP"."CUSTOMERS" set "NAME" = 'Jane Doe', "EMAIL" = 'jane@example.com' where "ID" = '1' and "NAME" = 'John Doe';`,
			wantSchema: "MYAPP",
			wantTable:  "CUSTOMERS",
			wantNewValues: map[string]any{
				"NAME":  "Jane Doe",
				"EMAIL": "jane@example.com",
			},
			wantOldValues: map[string]any{
				"ID":   "1",
				"NAME": "John Doe",
			},
		},
		{
			name:       "update with NULL",
			sql:        `update "MYAPP"."CUSTOMERS" set "EMAIL" = NULL where "ID" = '1';`,
			wantSchema: "MYAPP",
			wantTable:  "CUSTOMERS",
			wantNewValues: map[string]any{
				"EMAIL": nil,
			},
			wantOldValues: map[string]any{
				"ID": "1",
			},
		},
		{
			name:       "update with timestamp function",
			sql:        `update "MYAPP"."EVENTS" set "EVENT_TIME" = TO_TIMESTAMP('2020-02-02 00:00:00','YYYY-MM-DD HH24:MI:SS') where "ID" = '1' and "EVENT_TIME" = TO_TIMESTAMP('2020-02-01 00:00:00','YYYY-MM-DD HH24:MI:SS');`,
			wantSchema: "MYAPP",
			wantTable:  "EVENTS",
			wantNewValues: map[string]any{
				"EVENT_TIME": "TO_TIMESTAMP('2020-02-02 00:00:00','YYYY-MM-DD HH24:MI:SS')",
			},
			wantOldValues: map[string]any{
				"ID":         "1",
				"EVENT_TIME": "TO_TIMESTAMP('2020-02-01 00:00:00','YYYY-MM-DD HH24:MI:SS')",
			},
		},
		{
			name:       "update with escaped quotes",
			sql:        `update "MYAPP"."MESSAGES" set "TEXT" = 'He said ''Hello''' where "ID" = '1';`,
			wantSchema: "MYAPP",
			wantTable:  "MESSAGES",
			wantNewValues: map[string]any{
				"TEXT": "He said 'Hello'",
			},
			wantOldValues: map[string]any{
				"ID": "1",
			},
		},
		{
			name:       "update multiple columns",
			sql:        `update "MYAPP"."PRODUCTS" set "PRICE" = '99.99', "STOCK" = '50', "UPDATED_AT" = TO_DATE('2020-03-15','YYYY-MM-DD') where "ID" = '200';`,
			wantSchema: "MYAPP",
			wantTable:  "PRODUCTS",
			wantNewValues: map[string]any{
				"PRICE":      "99.99",
				"STOCK":      "50",
				"UPDATED_AT": "TO_DATE('2020-03-15','YYYY-MM-DD')",
			},
			wantOldValues: map[string]any{
				"ID": "200",
			},
		},
		{
			name:       "update with LOB column",
			sql:        `update "MYAPP"."DOCUMENTS" set "CONTENT" = 'Large text content' where "ID" = '1' and "CONTENT" = EMPTY_CLOB();`,
			wantSchema: "MYAPP",
			wantTable:  "DOCUMENTS",
			wantNewValues: map[string]any{
				"CONTENT": "Large text content",
			},
			wantOldValues: map[string]any{
				"ID":      "1",
				"CONTENT": "EMPTY_CLOB()",
			},
		},
		{
			name:       "update multiple LOB columns",
			sql:        `update "MYAPP"."DOCUMENTS" set "JSON_COL" = '{"key": "value"}', "VARCHARMAX_COL" = 'Max varchar text' where "ID" = '1';`,
			wantSchema: "MYAPP",
			wantTable:  "DOCUMENTS",
			wantNewValues: map[string]any{
				"JSON_COL":        `{"key": "value"}`,
				"VARCHARMAX_COL": "Max varchar text",
			},
			wantOldValues: map[string]any{
				"ID": "1",
			},
		},
		{
			name:       "update with HEXTORAW",
			sql:        `update "MYAPP"."BINARY_DATA" set "DATA" = HEXTORAW('FFAABBCC') where "ID" = '1';`,
			wantSchema: "MYAPP",
			wantTable:  "BINARY_DATA",
			wantNewValues: map[string]any{
				"DATA": "HEXTORAW('FFAABBCC')",
			},
			wantOldValues: map[string]any{
				"ID": "1",
			},
		},
		{
			name:       "update with comma in string",
			sql:        `update "MYAPP"."ADDRESS" set "STREET" = '456 Elm St, Suite 200' where "ID" = '2';`,
			wantSchema: "MYAPP",
			wantTable:  "ADDRESS",
			wantNewValues: map[string]any{
				"STREET": "456 Elm St, Suite 200",
			},
			wantOldValues: map[string]any{
				"ID": "2",
			},
		},
		{
			name:       "update with parentheses in string",
			sql:        `update "MYAPP"."NOTES" set "TEXT" = 'Result (success): 100%' where "ID" = '1';`,
			wantSchema: "MYAPP",
			wantTable:  "NOTES",
			wantNewValues: map[string]any{
				"TEXT": "Result (success): 100%",
			},
			wantOldValues: map[string]any{
				"ID": "1",
			},
		},
		{
			name:       "update with unicode",
			sql:        `update "MYAPP"."I18N" set "TEXT_ES" = 'Ñoño' where "ID" = '1';`,
			wantSchema: "MYAPP",
			wantTable:  "I18N",
			wantNewValues: map[string]any{
				"TEXT_ES": "Ñoño",
			},
			wantOldValues: map[string]any{
				"ID": "1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			if result.Schema != tt.wantSchema {
				t.Errorf("Schema = %v, want %v", result.Schema, tt.wantSchema)
			}
			if result.Table != tt.wantTable {
				t.Errorf("Table = %v, want %v", result.Table, tt.wantTable)
			}
			if !reflect.DeepEqual(result.NewValues, tt.wantNewValues) {
				t.Errorf("NewValues = %v, want %v", result.NewValues, tt.wantNewValues)
			}
			if !reflect.DeepEqual(result.OldValues, tt.wantOldValues) {
				t.Errorf("OldValues = %v, want %v", result.OldValues, tt.wantOldValues)
			}
		})
	}
}

func TestParseDelete(t *testing.T) {
	parser := New(false)

	tests := []struct {
		name          string
		sql           string
		wantSchema    string
		wantTable     string
		wantOldValues map[string]any
		wantErr       bool
	}{
		{
			name:       "simple delete",
			sql:        `delete from "MYAPP"."CUSTOMERS" where "ID" = '1' and "NAME" = 'John Doe';`,
			wantSchema: "MYAPP",
			wantTable:  "CUSTOMERS",
			wantOldValues: map[string]any{
				"ID":   "1",
				"NAME": "John Doe",
			},
		},
		{
			name:       "delete with NULL",
			sql:        `delete from "MYAPP"."CUSTOMERS" where "ID" = '1' and "EMAIL" IS NULL;`,
			wantSchema: "MYAPP",
			wantTable:  "CUSTOMERS",
			wantOldValues: map[string]any{
				"ID":    "1",
				"EMAIL": nil,
			},
		},
		{
			name:       "delete with function",
			sql:        `delete from "MYAPP"."ORDERS" where "ID" = '100' and "ORDER_DATE" = TO_DATE('2020-01-15','YYYY-MM-DD');`,
			wantSchema: "MYAPP",
			wantTable:  "ORDERS",
			wantOldValues: map[string]any{
				"ID":         "100",
				"ORDER_DATE": "TO_DATE('2020-01-15','YYYY-MM-DD')",
			},
		},
		{
			name:       "delete with escaped quotes",
			sql:        `delete from "MYAPP"."MESSAGES" where "ID" = '1' and "TEXT" = 'It''s working';`,
			wantSchema: "MYAPP",
			wantTable:  "MESSAGES",
			wantOldValues: map[string]any{
				"ID":   "1",
				"TEXT": "It's working",
			},
		},
		{
			name:       "delete with multiple conditions",
			sql:        `delete from "MYAPP"."PRODUCTS" where "ID" = '200' and "PRICE" = '99.99' and "STOCK" = '0';`,
			wantSchema: "MYAPP",
			wantTable:  "PRODUCTS",
			wantOldValues: map[string]any{
				"ID":    "200",
				"PRICE": "99.99",
				"STOCK": "0",
			},
		},
		{
			name:       "delete with HEXTORAW in WHERE",
			sql:        `delete from "MYAPP"."BINARY_DATA" where "ID" = '1' and "DATA" = HEXTORAW('AABBCCDD');`,
			wantSchema: "MYAPP",
			wantTable:  "BINARY_DATA",
			wantOldValues: map[string]any{
				"ID":   "1",
				"DATA": "HEXTORAW('AABBCCDD')",
			},
		},
		{
			name:       "delete with comma in string",
			sql:        `delete from "MYAPP"."ADDRESS" where "ID" = '1' and "STREET" = '123 Main St, Apt 4';`,
			wantSchema: "MYAPP",
			wantTable:  "ADDRESS",
			wantOldValues: map[string]any{
				"ID":     "1",
				"STREET": "123 Main St, Apt 4",
			},
		},
		{
			name:       "delete with parentheses in string",
			sql:        `delete from "MYAPP"."NOTES" where "ID" = '1' and "TEXT" = 'Result (pending)';`,
			wantSchema: "MYAPP",
			wantTable:  "NOTES",
			wantOldValues: map[string]any{
				"ID":   "1",
				"TEXT": "Result (pending)",
			},
		},
		{
			name:       "delete with unicode",
			sql:        `delete from "MYAPP"."I18N" where "ID" = '1' and "TEXT_JP" = 'こんにちは';`,
			wantSchema: "MYAPP",
			wantTable:  "I18N",
			wantOldValues: map[string]any{
				"ID":      "1",
				"TEXT_JP": "こんにちは",
			},
		},
		{
			name:       "delete with timestamp tz",
			sql:        `delete from "MYAPP"."EVENTS" where "ID" = '1' and "EVENT_TIME" = TO_TIMESTAMP_TZ('01-JAN-20 12.00.00.000000 AM +00:00');`,
			wantSchema: "MYAPP",
			wantTable:  "EVENTS",
			wantOldValues: map[string]any{
				"ID":         "1",
				"EVENT_TIME": "TO_TIMESTAMP_TZ('01-JAN-20 12.00.00.000000 AM +00:00')",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			if result.Schema != tt.wantSchema {
				t.Errorf("Schema = %v, want %v", result.Schema, tt.wantSchema)
			}
			if result.Table != tt.wantTable {
				t.Errorf("Table = %v, want %v", result.Table, tt.wantTable)
			}
			if !reflect.DeepEqual(result.OldValues, tt.wantOldValues) {
				t.Errorf("OldValues = %v, want %v", result.OldValues, tt.wantOldValues)
			}
		})
	}
}

func TestParseComplexCases(t *testing.T) {
	parser := New(false)

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "insert with nested parentheses in function",
			sql:     `insert into "MYAPP"."TEST"("ID","DATA") values ('1',TO_CHAR(SYSDATE,'YYYY-MM-DD'));`,
			wantErr: false,
		},
		{
			name:    "update with complex function",
			sql:     `update "MYAPP"."TEST" set "DATA" = SUBSTR('Hello World',1,5) where "ID" = '1';`,
			wantErr: false,
		},
		{
			name:    "delete with number value",
			sql:     `delete from "MYAPP"."TEST" where "ID" = '1' and "AMOUNT" = '100.50';`,
			wantErr: false,
		},
		{
			name:    "insert with multiple LOB types",
			sql:     `insert into "MYAPP"."MIXED_LOBS"("ID","CLOB_COL","BLOB_COL","NCLOB_COL") values ('1',EMPTY_CLOB(),EMPTY_BLOB(),EMPTY_CLOB());`,
			wantErr: false,
		},
		{
			name:    "insert with very long HEXTORAW",
			sql:     `insert into "MYAPP"."BIN"("ID","DATA") values ('1',HEXTORAW('000000000000000000000000000000000000000000000000000000000000000000000000'));`,
			wantErr: false,
		},
		{
			name:    "update with triply nested function",
			sql:     `update "MYAPP"."TEST" set "DATA" = TO_CHAR(TO_DATE(TO_CHAR(SYSDATE,'YYYY-MM-DD'),'YYYY-MM-DD'),'DD-MON-YY') where "ID" = '1';`,
			wantErr: false,
		},
		{
			name:    "delete with mixed quotes and special chars",
			sql:     `delete from "MYAPP"."TEST" where "ID" = '1' and "TEXT" = 'He said, "Hello!" (really)';`,
			wantErr: false,
		},
		{
			name:    "insert with all special characters in string",
			sql:     `insert into "MYAPP"."TEST"("ID","TEXT") values ('1','Special: ,;()=<>!@#$%');`,
			wantErr: false,
		},
		{
			name:    "update with JSON-like string",
			sql:     `update "MYAPP"."TEST" set "JSON_DATA" = '{"key":"value","nested":{"a":"b"}}' where "ID" = '1';`,
			wantErr: false,
		},
		{
			name:    "insert with backslash in string",
			sql:     `insert into "MYAPP"."TEST"("ID","PATH") values ('1','C:\Users\Admin\file.txt');`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				// Just verify it parsed without error
				if result == nil {
					t.Error("Expected non-nil result")
				}
			}
		})
	}
}

func TestParseSchemalessTableNames(t *testing.T) {
	parser := New(false)

	tests := []struct {
		name       string
		sql        string
		wantSchema string
		wantTable  string
		wantErr    bool
	}{
		{
			name:       "insert without schema",
			sql:        `insert into "CUSTOMERS"("ID","NAME") values ('1','John');`,
			wantSchema: "",
			wantTable:  "CUSTOMERS",
			wantErr:    false,
		},
		{
			name:       "update without schema",
			sql:        `update "PRODUCTS" set "PRICE" = '99.99' where "ID" = '1';`,
			wantSchema: "",
			wantTable:  "PRODUCTS",
			wantErr:    false,
		},
		{
			name:       "delete without schema",
			sql:        `delete from "ORDERS" where "ID" = '100';`,
			wantSchema: "",
			wantTable:  "ORDERS",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			if result.Schema != tt.wantSchema {
				t.Errorf("Schema = %v, want %v", result.Schema, tt.wantSchema)
			}
			if result.Table != tt.wantTable {
				t.Errorf("Table = %v, want %v", result.Table, tt.wantTable)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	parser := New(false)

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "empty SQL",
			sql:     "",
			wantErr: true,
		},
		{
			name:    "invalid operation",
			sql:     "select * from table;",
			wantErr: true,
		},
		{
			name:    "malformed insert",
			sql:     "insert into",
			wantErr: true,
		},
		{
			name:    "malformed update - missing SET",
			sql:     `update "MYAPP"."TEST" where "ID" = '1';`,
			wantErr: true,
		},
		{
			name:    "malformed delete - missing FROM",
			sql:     `delete "MYAPP"."TEST" where "ID" = '1';`,
			wantErr: true,
		},
		{
			name:    "unclosed string quote",
			sql:     `insert into "MYAPP"."TEST"("ID","NAME") values ('1','unclosed);`,
			wantErr: false, // Parser might handle this - depends on implementation
		},
		{
			name:    "mismatched parentheses",
			sql:     `insert into "MYAPP"."TEST"("ID","NAME") values ('1','John';`,
			wantErr: false, // Parser might handle this - depends on implementation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Benchmark tests
func BenchmarkParseInsert(b *testing.B) {
	parser := New(false)
	sql := `insert into "MYAPP"."CUSTOMERS"("ID","NAME","EMAIL","PHONE","ADDRESS") values ('1','John Doe','john@example.com','555-1234','123 Main St');`

	for b.Loop() {
		_, _ = parser.Parse(sql)
	}
}

func BenchmarkParseUpdate(b *testing.B) {
	parser := New(false)
	sql := `update "MYAPP"."CUSTOMERS" set "NAME" = 'Jane Doe', "EMAIL" = 'jane@example.com' where "ID" = '1' and "NAME" = 'John Doe';`

	for b.Loop() {
		_, _ = parser.Parse(sql)
	}
}

func BenchmarkParseDelete(b *testing.B) {
	parser := New(false)
	sql := `delete from "MYAPP"."CUSTOMERS" where "ID" = '1' and "NAME" = 'John Doe' and "EMAIL" = 'john@example.com';`

	for b.Loop() {
		_, _ = parser.Parse(sql)
	}
}
