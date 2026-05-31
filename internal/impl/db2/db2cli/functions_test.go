// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2cli

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// resetLibraryState resets the package-level load state so individual tests
// can simulate an unloaded library without interference.
func resetLibraryState() {
	libHandle = 0
	loadOnce = sync.Once{}
	loadErr = nil
}

// TestNewFunctionsReturnSQLErrorWhenLibraryNotLoaded verifies that every new
// public wrapper returns SQL_ERROR (not panics) when the DB2 library has not
// been loaded. This is the fallback contract for deployment environments where
// libdb2 is not installed.
func TestNewFunctionsReturnSQLErrorWhenLibraryNotLoaded(t *testing.T) {
	resetLibraryState()

	t.Run("SQLGetConnectAttr", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLGetConnectAttr(0, 0, nil, 0, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLGetTypeInfo", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLGetTypeInfo(0, 0)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLGetFunctions", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLGetFunctions(0, 0, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLDataSources", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLDataSources(0, 0, nil, 0, nil, nil, 0, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLDescribeParam", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLDescribeParam(0, 0, nil, nil, nil, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLTables", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLTables(0, "", "", "", "")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLColumns", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLColumns(0, "", "", "", "")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLPrimaryKeys", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLPrimaryKeys(0, "", "", "")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLForeignKeys", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLForeignKeys(0, "", "", "", "", "", "")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLStatistics", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLStatistics(0, "", "", "", 0, 0)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLSpecialColumns", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLSpecialColumns(0, 0, "", "", "", 0, 0)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLTablePrivileges", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLTablePrivileges(0, "", "", "")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLColumnPrivileges", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLColumnPrivileges(0, "", "", "", "")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLProcedures", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLProcedures(0, "", "", "")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLProcedureColumns", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLProcedureColumns(0, "", "", "", "")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLExtendedFetch", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLExtendedFetch(0, 0, 0, nil, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLSetPos", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLSetPos(0, 0, 0, 0)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLBulkOperations", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLBulkOperations(0, 0)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLNextResult", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLNextResult(0, 0)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLParamData", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLParamData(0, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLPutData", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLPutData(0, nil, 0)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLBrowseConnect", func(t *testing.T) {
		assert.NotPanics(t, func() {
			_, ret := SQLBrowseConnect(0, "")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLNativeSql", func(t *testing.T) {
		assert.NotPanics(t, func() {
			_, ret := SQLNativeSql(0, "SELECT 1")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLGetEnvAttr", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLGetEnvAttr(0, 0, nil, 0, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLGetCursorName", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLGetCursorName(0, nil, 0, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLSetCursorName", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLSetCursorName(0, "my_cursor")
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLCopyDesc", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLCopyDesc(0, 0)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLGetDescField", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLGetDescField(0, 0, 0, nil, 0, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLSetDescField", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLSetDescField(0, 0, 0, nil, 0)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLGetDescRec", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLGetDescRec(0, 0, nil, 0, nil, nil, nil, nil, nil, nil, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})

	t.Run("SQLSetDescRec", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ret := SQLSetDescRec(0, 0, 0, 0, 0, 0, 0, nil, nil, nil)
			assert.Equal(t, SQL_ERROR, ret)
		})
	})
}

// TestSqlStrPtrEmptyString verifies that sqlStrPtr returns nil for an empty string,
// matching the ODBC convention for "no filter" in catalog functions.
func TestSqlStrPtrEmptyString(t *testing.T) {
	ptr, length, buf := sqlStrPtr("")
	assert.Nil(t, ptr)
	assert.Equal(t, SQLSMALLINT(0), length)
	assert.Nil(t, buf)
}

// TestSqlStrPtrNonEmpty verifies that sqlStrPtr returns a non-nil pointer and
// SQL_NTS as the length sentinel so that the DB2 driver measures the string
// itself, avoiding SQLSMALLINT overflow for strings ≥ 32 KiB.
func TestSqlStrPtrNonEmpty(t *testing.T) {
	ptr, length, buf := sqlStrPtr("hello")
	assert.NotNil(t, ptr)
	assert.Equal(t, SQLSMALLINT(SQL_NTS), length)
	assert.NotNil(t, buf)
	runtime.KeepAlive(buf)
}
