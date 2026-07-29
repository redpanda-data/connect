// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/mssqlservertest"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/replication"

	"github.com/stretchr/testify/require"
)

func TestIntegration_MicrosoftSQLServerCDC_CheckpointCache(t *testing.T) {
	integration.CheckSkip(t)
	connStr, db := mssqlservertest.MustSetupTestWithMicrosoftSQLServerVersion(t)

	t.Run("cache initialises checkpoint table", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn;`)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn.CdcCheckpointCache"
		_, err = newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		// verify table is created
		var exists bool
		q := `SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID(?) AND name = ?;`
		require.NoError(t, db.QueryRowContext(t.Context(), q, "rpcn", "CdcCheckpointCache").Scan(&exists))
		require.Truef(t, exists, "expected table '%s' to exist but it does not", cacheTableToCreate)

		// verify stored procedure is created
		exists = false
		q = `SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(?) AND type = 'P';`
		require.NoError(t, db.QueryRowContext(t.Context(), q, fmt.Sprintf("%s.%s", "rpcn", "CdcCheckpointCacheUpdate")).Scan(&exists))
		require.True(t, exists, "expected stored procedure to exist")
	})

	t.Run("can set and get cache entries", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn1;`)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn1.CdcCheckpointCache"
		cache, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		// verify set
		var wanted replication.LSN
		require.NoError(t, wanted.Scan([]byte{0x00, 0x00, 0x00, 0x2d, 0x00, 0x00, 0x04, 0xb0, 0x00, 0x03}))
		require.NoError(t, cache.Set(t.Context(), "", wanted, nil))

		// verify get
		lsn, err := cache.Get(t.Context(), "")
		require.NoError(t, err)
		var got replication.LSN

		require.NoError(t, got.Scan(lsn))
		require.Equal(t, wanted, got)
	})

	t.Run("get reports empty cache as key not found", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn2;`)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn2.empty_cache"
		cache, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		lsn, err := cache.Get(t.Context(), "")
		require.ErrorIs(t, err, service.ErrKeyNotFound)
		require.Nil(t, lsn)
	})

	t.Run("closes gracefully", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn3;`)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn3.closing_cache"
		cache, err := newCheckpointCache(t.Context(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		require.NoError(t, cache.Close(t.Context()))

		_, err = cache.cacheSetStmt.Exec()
		require.Error(t, err)
		require.Contains(t, err.Error(), "sql: statement is closed")

		err = cache.db.PingContext(t.Context())
		require.Contains(t, err.Error(), "sql: database is closed")
	})
}

func TestIntegration_MicrosoftSQLServerCDC_CheckpointCache_ConvertOnRead(t *testing.T) {
	integration.CheckSkip(t)
	connStr, db := mssqlservertest.MustSetupTestWithMicrosoftSQLServerVersion(t)

	// highByteLSN contains bytes >= 0x80 (0x8b, 0xe2), which corrupt when round-tripped
	// through a character column: the driver's varchar decode path reinterprets them via
	// the column's collation and re-encodes as UTF-8, expanding the value beyond 10 bytes.
	highByteLSN := replication.LSN{0x00, 0x04, 0x8b, 0x73, 0x00, 0x01, 0x73, 0xe2, 0x00, 0x01}

	t.Run("round trips a high-byte LSN cleanly", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn1;`)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn1.CdcCheckpointCache"
		cache, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		require.NoError(t, cache.Set(t.Context(), "", highByteLSN, nil))

		got, err := cache.Get(t.Context(), "")
		require.NoError(t, err)
		require.Equal(t, []byte(highByteLSN), got)
	})

	t.Run("reads a legacy varchar cache table and recovers the true LSN via CONVERT, no DDL", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn2;`)
		require.NoError(t, err)

		// Recreate the table using the pre-fix schema (cache_val varchar(100)) and write the
		// LSN bytes directly via a varbinary bind so they land on disk intact, mirroring the
		// real-world bug: writes round-trip fine, only the varchar *read* path corrupts.
		_, err = db.Exec(`CREATE TABLE rpcn2.CdcCheckpointCache (
			cache_key varchar(7) NOT NULL PRIMARY KEY,
			cache_val varchar(100)
		);`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO rpcn2.CdcCheckpointCache (cache_key, cache_val) VALUES (?, ?);`,
			defaultCacheKey, []byte(highByteLSN))
		require.NoError(t, err)

		cacheTableToCreate := "rpcn2.CdcCheckpointCache"
		cache, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		got, err := cache.Get(t.Context(), "")
		require.NoError(t, err)
		require.Equal(t, []byte(highByteLSN), got, "CONVERT-on-read should recover the true on-disk LSN, not a re-corrupted value")

		// no DDL is ever run - the column must still be varchar
		var typeName string
		require.NoError(t, db.QueryRow(`
			SELECT t.name FROM sys.columns c
			JOIN sys.types t ON t.user_type_id = c.user_type_id
			WHERE c.object_id = OBJECT_ID('rpcn2.CdcCheckpointCache') AND c.name = 'cache_val';`).Scan(&typeName))
		require.Equal(t, "varchar", typeName)
	})

	t.Run("Get fails on a too-short legacy value rather than resuming from a bogus LSN", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn4;`)
		require.NoError(t, err)

		shortVal := []byte{0x01, 0x02, 0x03, 0x04}

		_, err = db.Exec(`CREATE TABLE rpcn4.CdcCheckpointCache (
			cache_key varchar(7) NOT NULL PRIMARY KEY,
			cache_val varchar(100)
		);`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO rpcn4.CdcCheckpointCache (cache_key, cache_val) VALUES (?, ?);`,
			defaultCacheKey, shortVal)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn4.CdcCheckpointCache"
		cache, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err, "construction only validates the column type, not row values")

		_, err = cache.Get(t.Context(), "")
		require.ErrorContains(t, err, "cannot be safely recovered")

		var shortAfter []byte
		require.NoError(t, db.QueryRow(`SELECT cache_val FROM rpcn4.CdcCheckpointCache WHERE cache_key = ?;`, defaultCacheKey).Scan(&shortAfter))
		require.Equal(t, shortVal, shortAfter, "row should be left untouched, pending manual intervention")
	})

	t.Run("Get fails on a too-long legacy value, proving source length is checked before truncation", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn10;`)
		require.NoError(t, err)

		// CONVERT(varbinary(10), x) truncates a too-long source to exactly 10 bytes with no
		// error, so this proves DATALENGTH is checked on the raw source alongside the CONVERT
		// expression, not derived from the (always <= 10 byte) converted result.
		overlongVal := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

		_, err = db.Exec(`CREATE TABLE rpcn10.CdcCheckpointCache (
			cache_key varchar(7) NOT NULL PRIMARY KEY,
			cache_val varchar(100)
		);`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO rpcn10.CdcCheckpointCache (cache_key, cache_val) VALUES (?, ?);`,
			defaultCacheKey, overlongVal)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn10.CdcCheckpointCache"
		cache, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		_, err = cache.Get(t.Context(), "")
		require.ErrorContains(t, err, "cannot be safely recovered")
	})

	t.Run("fails startup for a char legacy column rather than corrupting it via a mismatched proc", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn5;`)
		require.NoError(t, err)

		// char(100) is deliberately NOT treated as a recoverable legacy shape: DATALENGTH
		// on a fixed-length char column always returns the declared length (blank-padded),
		// not the actual content length, so the "<> 10" length check would false-positive
		// on every row. It must hard-fail startup rather than silently continue: continuing
		// would still (re)create the stored proc with @Value varbinary(10) against this
		// unvalidated character column, reproducing the exact corruption this fix exists for.
		_, err = db.Exec(`CREATE TABLE rpcn5.CdcCheckpointCache (
			cache_key varchar(7) NOT NULL PRIMARY KEY,
			cache_val char(100)
		);`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO rpcn5.CdcCheckpointCache (cache_key, cache_val) VALUES (?, ?);`,
			defaultCacheKey, []byte(highByteLSN))
		require.NoError(t, err)

		cacheTableToCreate := "rpcn5.CdcCheckpointCache"
		_, err = newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.ErrorContains(t, err, "unexpected type")
		require.ErrorContains(t, err, "manual inspection is required")

		// column type must be left exactly as it was, not migrated
		var typeName string
		require.NoError(t, db.QueryRow(`
			SELECT t.name FROM sys.columns c
			JOIN sys.types t ON t.user_type_id = c.user_type_id
			WHERE c.object_id = OBJECT_ID('rpcn5.CdcCheckpointCache') AND c.name = 'cache_val';`).Scan(&typeName))
		require.Equal(t, "char", typeName)
	})

	t.Run("fails startup for an nvarchar legacy column rather than corrupting it via a mismatched proc", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn6;`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE TABLE rpcn6.CdcCheckpointCache (
			cache_key varchar(7) NOT NULL PRIMARY KEY,
			cache_val nvarchar(100)
		);`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO rpcn6.CdcCheckpointCache (cache_key, cache_val) VALUES (?, ?);`,
			defaultCacheKey, []byte(highByteLSN))
		require.NoError(t, err)

		cacheTableToCreate := "rpcn6.CdcCheckpointCache"
		_, err = newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.ErrorContains(t, err, "unexpected type")
		require.ErrorContains(t, err, "manual inspection is required")

		// column type must be left exactly as it was, not migrated
		var typeName string
		require.NoError(t, db.QueryRow(`
			SELECT t.name FROM sys.columns c
			JOIN sys.types t ON t.user_type_id = c.user_type_id
			WHERE c.object_id = OBJECT_ID('rpcn6.CdcCheckpointCache') AND c.name = 'cache_val';`).Scan(&typeName))
		require.Equal(t, "nvarchar", typeName)
	})

	t.Run("fails startup for an nchar legacy column rather than corrupting it via a mismatched proc", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn7;`)
		require.NoError(t, err)

		// nchar shares char's fixed-length DATALENGTH padding problem and nvarchar's
		// UTF-16LE expansion problem - unrecoverable for two independent reasons.
		_, err = db.Exec(`CREATE TABLE rpcn7.CdcCheckpointCache (
			cache_key varchar(7) NOT NULL PRIMARY KEY,
			cache_val nchar(100)
		);`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO rpcn7.CdcCheckpointCache (cache_key, cache_val) VALUES (?, ?);`,
			defaultCacheKey, []byte(highByteLSN))
		require.NoError(t, err)

		cacheTableToCreate := "rpcn7.CdcCheckpointCache"
		_, err = newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.ErrorContains(t, err, "unexpected type")
		require.ErrorContains(t, err, "manual inspection is required")

		// column type must be left exactly as it was, not migrated
		var typeName string
		require.NoError(t, db.QueryRow(`
			SELECT t.name FROM sys.columns c
			JOIN sys.types t ON t.user_type_id = c.user_type_id
			WHERE c.object_id = OBJECT_ID('rpcn7.CdcCheckpointCache') AND c.name = 'cache_val';`).Scan(&typeName))
		require.Equal(t, "nchar", typeName)
	})

	t.Run("treats an existing binary column as directly readable", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn8;`)
		require.NoError(t, err)

		// binary(10) is a fixed-length binary type, distinct from varbinary(10) but
		// handled identically - no read-path corruption risk since it's binary, not
		// character, storage, and CONVERT(varbinary(10), binaryVal) is a no-op.
		_, err = db.Exec(`CREATE TABLE rpcn8.CdcCheckpointCache (
			cache_key varchar(7) NOT NULL PRIMARY KEY,
			cache_val binary(10)
		);`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO rpcn8.CdcCheckpointCache (cache_key, cache_val) VALUES (?, ?);`,
			defaultCacheKey, []byte(highByteLSN))
		require.NoError(t, err)

		cacheTableToCreate := "rpcn8.CdcCheckpointCache"
		cache, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		got, err := cache.Get(t.Context(), "")
		require.NoError(t, err)
		require.Equal(t, []byte(highByteLSN), got)

		// no DDL is ever run - the column type is left exactly as it was
		var typeName string
		require.NoError(t, db.QueryRow(`
			SELECT t.name FROM sys.columns c
			JOIN sys.types t ON t.user_type_id = c.user_type_id
			WHERE c.object_id = OBJECT_ID('rpcn8.CdcCheckpointCache') AND c.name = 'cache_val';`).Scan(&typeName))
		require.Equal(t, "binary", typeName)
	})

	t.Run("is idempotent across multiple constructions", func(t *testing.T) {
		t.Parallel()

		_, err := db.Exec(`CREATE SCHEMA rpcn3;`)
		require.NoError(t, err)

		cacheTableToCreate := "rpcn3.CdcCheckpointCache"

		cacheA, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)
		require.NoError(t, cacheA.Set(t.Context(), "", highByteLSN, nil))

		// second construction against the same, already-migrated table must not error
		cacheB, err := newCheckpointCache(context.Background(), connStr, cacheTableToCreate, nil)
		require.NoError(t, err)

		got, err := cacheB.Get(t.Context(), "")
		require.NoError(t, err)
		require.Equal(t, []byte(highByteLSN), got)

		require.NoError(t, cacheB.Set(t.Context(), "", highByteLSN, nil))
		got, err = cacheA.Get(t.Context(), "")
		require.NoError(t, err)
		require.Equal(t, []byte(highByteLSN), got)
	})
}

func TestValidateTableName(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		expectedErr error
	}{
		// Valid cases
		{name: "Valid simple table name", tableName: "dbo.users", expectedErr: nil},
		{name: "Valid table name with numbers", tableName: "dbo.orders_2024", expectedErr: nil},
		{name: "Valid table name with underscore prefix", tableName: "dbo._temp_table", expectedErr: nil},
		{name: "Valid table name with dollar sign", tableName: "dbo.user$data", expectedErr: nil},
		{name: "Valid table name with mixed case", tableName: "dbo.UserProfiles", expectedErr: nil},
		// Invalid cases
		{name: "Empty table name not allowed", tableName: "", expectedErr: errEmptyTableName},
		{name: "Schema is required", tableName: "users", expectedErr: errInvalidTableFormat},
		{name: "Missing schema", tableName: ".users", expectedErr: errInvalidSchemaLength},
		{name: "Table name starting with number not allowed", tableName: "dbo.2users", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name starting with # sign not allowed", tableName: "dbo.#users", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name starting with @ sign not allowed", tableName: "dbo.@users", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name with special characters not allowed", tableName: "dbo.users@table", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name with spaces not allowed", tableName: "dbo.user table", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name with hyphens not allowed", tableName: "dbo.user-table", expectedErr: errInvalidIdentifiedInTableName},
		{name: "Table name is no more than 128 characters", tableName: "dbo." + strings.Repeat("a", 129), expectedErr: errInvalidTableLength},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateCacheTableName(tc.tableName)

			if tc.expectedErr == nil && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if tc.expectedErr != nil && err == nil {
				t.Errorf("expected error %v, got nil", tc.expectedErr)
			}
			if tc.expectedErr != nil && err != nil && tc.expectedErr.Error() != err.Error() {
				t.Errorf("expected error %v, got %v", tc.expectedErr, err)
			}
		})
	}
}
