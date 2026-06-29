package saphana_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana"
)

func TestHANATypeMapping(t *testing.T) {
	cases := []struct {
		hanaType string
		wantKind string
	}{
		{"INTEGER", "int64"},
		{"BIGINT", "int64"},
		{"SMALLINT", "int64"},
		{"TINYINT", "int64"},
		{"DOUBLE", "float64"},
		{"REAL", "float64"},
		{"DECIMAL", "string"},
		{"SMALLDECIMAL", "string"},
		{"VARCHAR", "string"},
		{"NVARCHAR", "string"},
		{"CHAR", "string"},
		{"NCHAR", "string"},
		{"ALPHANUM", "string"},
		{"SHORTTEXT", "string"},
		{"DATE", "string"},
		{"TIME", "string"},
		{"TIMESTAMP", "string"},
		{"SECONDDATE", "string"},
		{"LONGDATE", "string"},
		{"BOOLEAN", "bool"},
		{"CLOB", "string"},
		{"NCLOB", "string"},
		{"BLOB", "bytes"},
		{"BINARY", "bytes"},
		{"VARBINARY", "bytes"},
		{"UNKNOWN_FUTURE_TYPE", "string"}, // unknown types fall back to string
	}
	for _, tc := range cases {
		t.Run(tc.hanaType, func(t *testing.T) {
			got := saphana.HANATypeKind(tc.hanaType)
			assert.Equal(t, tc.wantKind, got)
		})
	}
}

func TestSchemaKey(t *testing.T) {
	assert.Equal(t, "HR\x00EMPLOYEES", saphana.SchemaKey("HR", "EMPLOYEES"))
	assert.Equal(t, "SALES\x00ORDERS", saphana.SchemaKey("SALES", "ORDERS"))
	// Empty strings are allowed (unusual but should not panic).
	assert.Equal(t, "\x00", saphana.SchemaKey("", ""))
}

func TestCoerceValueInt64(t *testing.T) {
	assert.Equal(t, int64(42), saphana.CoerceValue("INTEGER", float64(42)))
	assert.Equal(t, int64(100), saphana.CoerceValue("BIGINT", float64(100)))
	assert.Equal(t, int64(7), saphana.CoerceValue("TINYINT", "7"))
}

func TestCoerceValueFloat64(t *testing.T) {
	assert.Equal(t, 3.14, saphana.CoerceValue("DOUBLE", "3.14"))
	assert.Equal(t, float64(1), saphana.CoerceValue("REAL", float64(1)))
}

func TestCoerceValueBool(t *testing.T) {
	assert.Equal(t, true, saphana.CoerceValue("BOOLEAN", "true"))
	assert.Equal(t, true, saphana.CoerceValue("BOOLEAN", "TRUE"))
	assert.Equal(t, true, saphana.CoerceValue("BOOLEAN", "1"))
	assert.Equal(t, false, saphana.CoerceValue("BOOLEAN", "false"))
	assert.Equal(t, false, saphana.CoerceValue("BOOLEAN", "0"))
}

func TestCoerceValueNilPassthrough(t *testing.T) {
	assert.Nil(t, saphana.CoerceValue("INTEGER", nil))
	assert.Nil(t, saphana.CoerceValue("VARCHAR", nil))
}

func TestCoerceValueStringPassthrough(t *testing.T) {
	assert.Equal(t, "hello", saphana.CoerceValue("VARCHAR", "hello"))
	assert.Equal(t, "2024-01-01", saphana.CoerceValue("DATE", "2024-01-01"))
}

// TestCoerceValueInvalidIntReturnsRaw covers the error branch for
// strconv.ParseInt when the string is not a valid integer (Bug 12).
func TestCoerceValueInvalidIntReturnsRaw(t *testing.T) {
	raw := "abc"
	result := saphana.CoerceValue("INTEGER", raw)
	// On parse failure, the original raw value must be returned unchanged.
	assert.Equal(t, raw, result, "unparseable int string must return raw value, not zero")
}

// TestCoerceValueInvalidFloatReturnsRaw covers the error branch for
// strconv.ParseFloat when the string is not a valid float (Bug 12).
func TestCoerceValueInvalidFloatReturnsRaw(t *testing.T) {
	raw := "not-a-float"
	result := saphana.CoerceValue("DOUBLE", raw)
	assert.Equal(t, raw, result, "unparseable float string must return raw value, not zero")
}

func TestColumnInfoToReplicationMeta(t *testing.T) {
	cols := []saphana.ColumnInfo{
		{Name: "ID", TypeName: "INTEGER", Position: 1, Nullable: false, IsPK: true},
		{Name: "NAME", TypeName: "NVARCHAR", Position: 2, Nullable: true, IsPK: false},
	}
	meta := saphana.ColumnInfoToReplicationMeta(cols)
	assert.Len(t, meta, 2)
	assert.Equal(t, "ID", meta[0].Name)
	assert.Equal(t, "INTEGER", meta[0].TypeName)
	assert.Equal(t, 1, meta[0].Position)
	assert.False(t, meta[0].Nullable)
	assert.Equal(t, "NAME", meta[1].Name)
	assert.True(t, meta[1].Nullable)
}
