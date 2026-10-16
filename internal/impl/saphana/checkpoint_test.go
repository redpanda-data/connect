// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// incrementing_initial_value is a string in YAML but is bound as a query
// parameter, and go-hdb rejects mismatched Go types client-side, so it must
// be coerced to the incrementing column's actual type.
func TestCoerceIncrementingValue(t *testing.T) {
	tests := []struct {
		raw      string
		dataType string
		want     any
		wantErr  bool
	}{
		{raw: "000100", dataType: "NVARCHAR", want: "000100"},
		{raw: "100", dataType: "VARCHAR", want: "100"},
		{raw: "100", dataType: "BIGINT", want: int64(100)},
		{raw: "7", dataType: "INTEGER", want: int64(7)},
		{raw: "12.5", dataType: "DECIMAL", want: 12.5},
		{raw: "12.5", dataType: "DOUBLE", want: 12.5},
		{raw: "2024-01-01T00:00:00Z", dataType: "TIMESTAMP", want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{raw: "2024-01-01 10:30:00", dataType: "TIMESTAMP", want: time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)},
		{raw: "2024-01-01", dataType: "DATE", want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{raw: "2024-01-01T00:00:00Z", dataType: "SECONDDATE", want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{raw: "2024-01-01T00:00:00Z", dataType: "LONGDATE", want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{raw: "2024-01-01", dataType: "DAYDATE", want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{raw: "abc", dataType: "BIGINT", wantErr: true},
		{raw: "yesterday", dataType: "TIMESTAMP", wantErr: true},
		// Binary keys (a ULID or UUID stored as VARBINARY(16)) are written in
		// hex and bound as bytes, not as the ASCII of the hex text.
		{raw: "0189f0a1b2c3", dataType: "VARBINARY", want: []byte{0x01, 0x89, 0xf0, 0xa1, 0xb2, 0xc3}},
		{raw: "00ff", dataType: "BINARY", want: []byte{0x00, 0xff}},
		{raw: "zz", dataType: "VARBINARY", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.dataType+"/"+tc.raw, func(t *testing.T) {
			got, err := coerceIncrementingValue(tc.raw, tc.dataType)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// The column type comes from the catalog at connect time; a zero-padded
// NVARCHAR key must not be coerced to an integer just because it parses.
func TestSAPHANAInputResolvesInitialValueFromColumnType(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: incrementing
schema_name: S
table: T
incrementing_column: DOC_NO
incrementing_initial_value: "000100"
poll_interval: 1ms
fetch_size: 10
max_retries: 0
`)
	require.Equal(t, int64(100), s.hwm, "constructor heuristic guesses integer before the catalog is consulted")

	mock.ExpectQuery(hanaColumnTypeQuery).WithArgs("S", "T", "DOC_NO").
		WillReturnRows(sqlmock.NewRows([]string{"DATA_TYPE_NAME"}).AddRow("NVARCHAR"))
	require.NoError(t, s.resolveIncrementingInitialValue(t.Context(), s.db))
	assert.Equal(t, "000100", s.hwm)

	mock.ExpectQuery(`SELECT * FROM "S"."T" WHERE "DOC_NO" > ? ORDER BY "DOC_NO"`).WithArgs("000100").
		WillReturnRows(sqlmock.NewRows([]string{"DOC_NO"}))
	_, _, err := s.ReadBatch(t.Context())
	require.Error(t, err) // the poll after the empty one has no expectation
	require.NoError(t, mock.ExpectationsWereMet())
}

// When the catalog lookup fails the heuristic value is kept (with a warning)
// rather than failing the connection outright.
func TestSAPHANAInputInitialValueFallsBackWhenCatalogUnavailable(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: T
incrementing_column: ID
incrementing_initial_value: "100"
poll_interval: 1ms
fetch_size: 10
max_retries: 0
`)
	mock.ExpectQuery(hanaColumnTypeCurrentSchemaQuery).WithArgs("T", "ID").
		WillReturnError(errors.New("insufficient privilege"))
	require.NoError(t, s.resolveIncrementingInitialValue(t.Context(), s.db))
	assert.Equal(t, int64(100), s.hwm)
}

// TestValidateTimestampColumnType: the field doc promises a TIMESTAMP or
// LONGDATE column, so a wrongly typed timestamp_column must fail at connect
// time with the column and its type in the message, not on the first poll's
// bind. An unreadable catalog only warns, as for incrementing_column.
func TestValidateTimestampColumnType(t *testing.T) {
	const confYAML = `
dsn: hdb://user:pass@host:39017
mode: timestamp
schema_name: S
table: T
timestamp_column: TS
`
	for _, tc := range []struct {
		dataType string
		wantErr  string
	}{
		{dataType: "TIMESTAMP"},
		{dataType: "LONGDATE"},
		{dataType: "SECONDDATE"},
		{dataType: "NVARCHAR", wantErr: `timestamp_column "TS" has type NVARCHAR`},
		{dataType: "DATE", wantErr: `has type DATE; timestamp modes need`},
	} {
		t.Run(tc.dataType, func(t *testing.T) {
			s, mock := newTestInput(t, enterpriseResources(), confYAML)
			mock.ExpectQuery(hanaColumnTypeQuery).WithArgs("S", "T", "TS").
				WillReturnRows(sqlmock.NewRows([]string{"DATA_TYPE_NAME"}).AddRow(tc.dataType))
			err := s.validateTimestampColumn(t.Context(), s.db)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

	t.Run("catalog unreadable only warns", func(t *testing.T) {
		s, mock := newTestInput(t, enterpriseResources(), confYAML)
		mock.ExpectQuery(hanaColumnTypeQuery).WithArgs("S", "T", "TS").
			WillReturnError(errors.New("insufficient privilege"))
		require.NoError(t, s.validateTimestampColumn(t.Context(), s.db))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("without schema_name the lookup uses CURRENT_SCHEMA", func(t *testing.T) {
		s, mock := newTestInput(t, enterpriseResources(), `
dsn: hdb://user:pass@host:39017
mode: timestamp
table: T
timestamp_column: TS
`)
		mock.ExpectQuery(hanaColumnTypeCurrentSchemaQuery).WithArgs("T", "TS").
			WillReturnRows(sqlmock.NewRows([]string{"DATA_TYPE_NAME"}).AddRow("INTEGER"))
		require.ErrorContains(t, s.validateTimestampColumn(t.Context(), s.db), "has type INTEGER")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestHWMEqual(t *testing.T) {
	assert.True(t, hwmEqual([]byte{1, 2}, []byte{1, 2}))
	assert.False(t, hwmEqual([]byte{1, 2}, []byte{1, 3}))
	assert.False(t, hwmEqual([]byte{1}, "\x01"), "a byte slice never equals a string")
	assert.True(t, hwmEqual(int64(7), int64(7)))
	assert.False(t, hwmEqual(int64(7), int64(8)))
	assert.True(t, hwmEqual(nil, nil))
	assert.False(t, hwmEqual(nil, int64(0)))
	assert.True(t, hwmEqual("a", "a"))
}
