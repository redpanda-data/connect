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
