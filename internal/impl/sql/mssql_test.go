package sql

import (
	"testing"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/golang-sql/civil"
	"github.com/stretchr/testify/assert"
)

func TestApplyMSSQLDataType(t *testing.T) {
	tests := []struct {
		name     string
		arg      any
		column   string
		DataType map[string]any
		expected any
		wantErr  bool
	}{
		{
			name:     "No DataType",
			arg:      "test",
			column:   "col1",
			DataType: map[string]any{},
			expected: "test",
			wantErr:  false,
		},
		{
			name:   "VARCHAR type",
			arg:    "test",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "VARCHAR"},
			},
			expected: mssql.VarChar("test"),
			wantErr:  false,
		},
		{
			name:   "DATETIME type",
			arg:    "2023-10-01T12:00:00Z",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME", "datetime": map[string]any{"format": time.RFC3339}},
			},
			expected: mssql.DateTime1(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:   "DATETIME_OFFSET type",
			arg:    "2023-10-01T12:00:00Z",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME_OFFSET", "datetime_offset": map[string]any{"format": time.RFC3339}},
			},
			expected: mssql.DateTimeOffset(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:   "DATE type",
			arg:    "2023-10-01",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATE", "date": map[string]any{"format": "2006-01-02"}},
			},
			expected: civil.Date{Year: 2023, Month: 10, Day: 1},
			wantErr:  false,
		},
		{
			name:   "Invalid DATETIME format",
			arg:    "invalid-date",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME", "datetime": map[string]any{"format": time.RFC3339}},
			},
			expected: "invalid-date",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := applyMSSQLDataType(tt.arg, tt.column, tt.DataType)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}
