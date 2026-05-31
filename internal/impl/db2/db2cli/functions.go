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
	"unsafe"

	"github.com/ebitengine/purego"
)

// Function pointers for DB2 CLI functions
// These are registered at library load time

// Connection Management Functions
var (
	sqlAllocHandle    func(handleType SQLSMALLINT, inputHandle SQLHANDLE, outputHandle *SQLHANDLE) SQLRETURN
	sqlFreeHandle     func(handleType SQLSMALLINT, handle SQLHANDLE) SQLRETURN
	sqlConnect        func(hdbc SQLHDBC, dsn *SQLCHAR, dsnLen SQLSMALLINT, uid *SQLCHAR, uidLen SQLSMALLINT, pwd *SQLCHAR, pwdLen SQLSMALLINT) SQLRETURN
	sqlDriverConnect  func(hdbc SQLHDBC, hwnd uintptr, connStr *SQLCHAR, connStrLen SQLSMALLINT, outConnStr *SQLCHAR, outConnStrMax SQLSMALLINT, outConnStrLen *SQLSMALLINT, driverCompletion SQLUSMALLINT) SQLRETURN
	sqlDisconnect     func(hdbc SQLHDBC) SQLRETURN
	sqlSetEnvAttr     func(henv SQLHENV, attr SQLINTEGER, value uintptr, strLen SQLINTEGER) SQLRETURN
	sqlSetConnectAttr func(hdbc SQLHDBC, attr SQLINTEGER, value uintptr, strLen SQLINTEGER) SQLRETURN
	sqlGetConnectAttr func(hdbc SQLHDBC, attr SQLINTEGER, value SQLPOINTER, bufLen SQLINTEGER, strLen *SQLINTEGER) SQLRETURN
	sqlGetInfo        func(hdbc SQLHDBC, infoType SQLUSMALLINT, infoValue SQLPOINTER, bufLen SQLSMALLINT, strLen *SQLSMALLINT) SQLRETURN
)

// Statement Functions
var (
	sqlExecDirect  func(hstmt SQLHSTMT, sql *SQLCHAR, sqlLen SQLINTEGER) SQLRETURN
	sqlPrepare     func(hstmt SQLHSTMT, sql *SQLCHAR, sqlLen SQLINTEGER) SQLRETURN
	sqlExecute     func(hstmt SQLHSTMT) SQLRETURN
	sqlFetch       func(hstmt SQLHSTMT) SQLRETURN
	sqlFetchScroll func(hstmt SQLHSTMT, fetchOrientation SQLSMALLINT, fetchOffset SQLLEN) SQLRETURN
	sqlCloseCursor func(hstmt SQLHSTMT) SQLRETURN
	sqlFreeStmt    func(hstmt SQLHSTMT, option SQLUSMALLINT) SQLRETURN
	sqlSetStmtAttr func(hstmt SQLHSTMT, attr SQLINTEGER, value uintptr, strLen SQLINTEGER) SQLRETURN
	sqlGetStmtAttr func(hstmt SQLHSTMT, attr SQLINTEGER, value SQLPOINTER, bufLen SQLINTEGER, strLen *SQLINTEGER) SQLRETURN
	sqlRowCount    func(hstmt SQLHSTMT, rowCount *SQLLEN) SQLRETURN
	sqlMoreResults func(hstmt SQLHSTMT) SQLRETURN
	sqlCancel      func(hstmt SQLHSTMT) SQLRETURN
)

// Result Set Functions
var (
	sqlNumResultCols func(hstmt SQLHSTMT, colCount *SQLSMALLINT) SQLRETURN
	sqlDescribeCol   func(hstmt SQLHSTMT, colNum SQLUSMALLINT, colName *SQLCHAR, colNameMax SQLSMALLINT, colNameLen, dataType *SQLSMALLINT, colSize *SQLULEN, decimalDigits, nullable *SQLSMALLINT) SQLRETURN
	sqlColAttribute  func(hstmt SQLHSTMT, colNum, fieldID SQLUSMALLINT, charAttr SQLPOINTER, bufLen SQLSMALLINT, strLen *SQLSMALLINT, numAttr *SQLLEN) SQLRETURN
	sqlBindCol       func(hstmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN
	sqlGetData       func(hstmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN
)

// Parameter Functions
var (
	sqlBindParameter func(hstmt SQLHSTMT, paramNum SQLUSMALLINT, inputOutputType, valueType, paramType SQLSMALLINT, colSize SQLULEN, decimalDigits SQLSMALLINT, paramValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN
	sqlNumParams     func(hstmt SQLHSTMT, paramCount *SQLSMALLINT) SQLRETURN
)

// Transaction Functions
var (
	sqlEndTran func(handleType SQLSMALLINT, handle SQLHANDLE, completionType SQLSMALLINT) SQLRETURN
)

// Diagnostic Functions
var (
	sqlGetDiagRec   func(handleType SQLSMALLINT, handle SQLHANDLE, recNum SQLSMALLINT, sqlState *SQLCHAR, nativeError *SQLINTEGER, messageText *SQLCHAR, bufLen SQLSMALLINT, textLen *SQLSMALLINT) SQLRETURN
	sqlGetDiagField func(handleType SQLSMALLINT, handle SQLHANDLE, recNum, diagID SQLSMALLINT, diagInfo SQLPOINTER, bufLen SQLSMALLINT, strLen *SQLSMALLINT) SQLRETURN
	sqlError        func(henv SQLHENV, hdbc SQLHDBC, hstmt SQLHSTMT, sqlState *SQLCHAR, nativeError *SQLINTEGER, messageText *SQLCHAR, bufLen SQLSMALLINT, textLen *SQLSMALLINT) SQLRETURN
)

// Metadata / Catalog Functions
var (
	sqlTables           func(hstmt SQLHSTMT, catalogName *SQLCHAR, catalogNameLen SQLSMALLINT, schemaName *SQLCHAR, schemaNameLen SQLSMALLINT, tableName *SQLCHAR, tableNameLen SQLSMALLINT, tableType *SQLCHAR, tableTypeLen SQLSMALLINT) SQLRETURN
	sqlColumns          func(hstmt SQLHSTMT, catalogName *SQLCHAR, catalogNameLen SQLSMALLINT, schemaName *SQLCHAR, schemaNameLen SQLSMALLINT, tableName *SQLCHAR, tableNameLen SQLSMALLINT, columnName *SQLCHAR, columnNameLen SQLSMALLINT) SQLRETURN
	sqlPrimaryKeys      func(hstmt SQLHSTMT, catalogName *SQLCHAR, catalogNameLen SQLSMALLINT, schemaName *SQLCHAR, schemaNameLen SQLSMALLINT, tableName *SQLCHAR, tableNameLen SQLSMALLINT) SQLRETURN
	sqlForeignKeys      func(hstmt SQLHSTMT, pkCatalogName *SQLCHAR, pkCatalogNameLen SQLSMALLINT, pkSchemaName *SQLCHAR, pkSchemaNameLen SQLSMALLINT, pkTableName *SQLCHAR, pkTableNameLen SQLSMALLINT, fkCatalogName *SQLCHAR, fkCatalogNameLen SQLSMALLINT, fkSchemaName *SQLCHAR, fkSchemaNameLen SQLSMALLINT, fkTableName *SQLCHAR, fkTableNameLen SQLSMALLINT) SQLRETURN
	sqlStatistics       func(hstmt SQLHSTMT, catalogName *SQLCHAR, catalogNameLen SQLSMALLINT, schemaName *SQLCHAR, schemaNameLen SQLSMALLINT, tableName *SQLCHAR, tableNameLen SQLSMALLINT, unique SQLUSMALLINT, accuracy SQLUSMALLINT) SQLRETURN
	sqlSpecialColumns   func(hstmt SQLHSTMT, colType SQLUSMALLINT, catalogName *SQLCHAR, catalogNameLen SQLSMALLINT, schemaName *SQLCHAR, schemaNameLen SQLSMALLINT, tableName *SQLCHAR, tableNameLen SQLSMALLINT, scope SQLUSMALLINT, nullable SQLUSMALLINT) SQLRETURN
	sqlTablePrivileges  func(hstmt SQLHSTMT, catalogName *SQLCHAR, catalogNameLen SQLSMALLINT, schemaName *SQLCHAR, schemaNameLen SQLSMALLINT, tableName *SQLCHAR, tableNameLen SQLSMALLINT) SQLRETURN
	sqlColumnPrivileges func(hstmt SQLHSTMT, catalogName *SQLCHAR, catalogNameLen SQLSMALLINT, schemaName *SQLCHAR, schemaNameLen SQLSMALLINT, tableName *SQLCHAR, tableNameLen SQLSMALLINT, columnName *SQLCHAR, columnNameLen SQLSMALLINT) SQLRETURN
	sqlProcedures       func(hstmt SQLHSTMT, catalogName *SQLCHAR, catalogNameLen SQLSMALLINT, schemaName *SQLCHAR, schemaNameLen SQLSMALLINT, procName *SQLCHAR, procNameLen SQLSMALLINT) SQLRETURN
	sqlProcedureColumns func(hstmt SQLHSTMT, catalogName *SQLCHAR, catalogNameLen SQLSMALLINT, schemaName *SQLCHAR, schemaNameLen SQLSMALLINT, procName *SQLCHAR, procNameLen SQLSMALLINT, columnName *SQLCHAR, columnNameLen SQLSMALLINT) SQLRETURN
	sqlGetTypeInfo      func(hstmt SQLHSTMT, dataType SQLSMALLINT) SQLRETURN
	sqlGetFunctions     func(hdbc SQLHDBC, function SQLUSMALLINT, supported *SQLUSMALLINT) SQLRETURN
	sqlDataSources      func(henv SQLHENV, direction SQLUSMALLINT, serverName *SQLCHAR, serverNameMax SQLSMALLINT, serverNameLen *SQLSMALLINT, description *SQLCHAR, descriptionMax SQLSMALLINT, descriptionLen *SQLSMALLINT) SQLRETURN
	sqlDescribeParam    func(hstmt SQLHSTMT, paramNum SQLUSMALLINT, dataType *SQLSMALLINT, paramSize *SQLULEN, decimalDigits *SQLSMALLINT, nullable *SQLSMALLINT) SQLRETURN
)

// Cursor / Extended Fetch Functions
var (
	sqlExtendedFetch  func(hstmt SQLHSTMT, fetchType SQLUSMALLINT, irow SQLLEN, rowCountPtr *SQLULEN, rowStatusArray *SQLUSMALLINT) SQLRETURN
	sqlSetPos         func(hstmt SQLHSTMT, rowNum SQLULEN, operation SQLUSMALLINT, lockType SQLUSMALLINT) SQLRETURN
	sqlBulkOperations func(hstmt SQLHSTMT, operation SQLUSMALLINT) SQLRETURN
	sqlNextResult     func(hstmtSource SQLHSTMT, hstmtTarget SQLHSTMT) SQLRETURN
)

// Data-at-Execution Functions
var (
	sqlParamData func(hstmt SQLHSTMT, value *SQLPOINTER) SQLRETURN
	sqlPutData   func(hstmt SQLHSTMT, data SQLPOINTER, strLen SQLLEN) SQLRETURN
)

// Extended Connection Functions
var (
	sqlBrowseConnect func(hdbc SQLHDBC, connStrIn *SQLCHAR, connStrInLen SQLSMALLINT, connStrOut *SQLCHAR, connStrOutMax SQLSMALLINT, connStrOutLen *SQLSMALLINT) SQLRETURN
	sqlNativeSql     func(hdbc SQLHDBC, sqlStrIn *SQLCHAR, sqlStrInLen SQLINTEGER, sqlStr *SQLCHAR, sqlStrMax SQLINTEGER, sqlStrLen *SQLINTEGER) SQLRETURN
	sqlGetEnvAttr    func(henv SQLHENV, attr SQLINTEGER, value SQLPOINTER, bufLen SQLINTEGER, strLen *SQLINTEGER) SQLRETURN
)

// Cursor Name Functions
var (
	sqlGetCursorName func(hstmt SQLHSTMT, cursor *SQLCHAR, cursorMax SQLSMALLINT, cursorLen *SQLSMALLINT) SQLRETURN
	sqlSetCursorName func(hstmt SQLHSTMT, cursor *SQLCHAR, cursorLen SQLSMALLINT) SQLRETURN
)

// Descriptor Functions
var (
	sqlCopyDesc     func(srcDesc SQLHDESC, dstDesc SQLHDESC) SQLRETURN
	sqlGetDescField func(descHandle SQLHDESC, recNum SQLSMALLINT, fieldID SQLSMALLINT, value SQLPOINTER, bufLen SQLINTEGER, strLen *SQLINTEGER) SQLRETURN
	sqlSetDescField func(descHandle SQLHDESC, recNum SQLSMALLINT, fieldID SQLSMALLINT, value SQLPOINTER, bufLen SQLINTEGER) SQLRETURN
	sqlGetDescRec   func(descHandle SQLHDESC, recNum SQLSMALLINT, name *SQLCHAR, nameMax SQLSMALLINT, nameLen *SQLSMALLINT, dataType *SQLSMALLINT, subType *SQLSMALLINT, length *SQLLEN, precision *SQLSMALLINT, scale *SQLSMALLINT, nullable *SQLSMALLINT) SQLRETURN
	sqlSetDescRec   func(descHandle SQLHDESC, recNum SQLSMALLINT, dataType SQLSMALLINT, subType SQLSMALLINT, length SQLLEN, precision SQLSMALLINT, scale SQLSMALLINT, data SQLPOINTER, strLen *SQLLEN, indicator *SQLLEN) SQLRETURN
)

// registerConnectionFunctions registers connection-related DB2 CLI functions
func registerConnectionFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlAllocHandle, "SQLAllocHandle"},
		{&sqlFreeHandle, "SQLFreeHandle"},
		{&sqlConnect, "SQLConnect"},
		{&sqlDriverConnect, "SQLDriverConnect"},
		{&sqlDisconnect, "SQLDisconnect"},
		{&sqlSetEnvAttr, "SQLSetEnvAttr"},
		{&sqlSetConnectAttr, "SQLSetConnectAttr"},
		{&sqlGetConnectAttr, "SQLGetConnectAttr"},
		{&sqlGetInfo, "SQLGetInfo"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerStatementFunctions registers statement-related DB2 CLI functions
func registerStatementFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlExecDirect, "SQLExecDirect"},
		{&sqlPrepare, "SQLPrepare"},
		{&sqlExecute, "SQLExecute"},
		{&sqlFetch, "SQLFetch"},
		{&sqlFetchScroll, "SQLFetchScroll"},
		{&sqlCloseCursor, "SQLCloseCursor"},
		{&sqlFreeStmt, "SQLFreeStmt"},
		{&sqlSetStmtAttr, "SQLSetStmtAttr"},
		{&sqlGetStmtAttr, "SQLGetStmtAttr"},
		{&sqlRowCount, "SQLRowCount"},
		{&sqlMoreResults, "SQLMoreResults"},
		{&sqlCancel, "SQLCancel"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerResultFunctions registers result set-related DB2 CLI functions
func registerResultFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlNumResultCols, "SQLNumResultCols"},
		{&sqlDescribeCol, "SQLDescribeCol"},
		{&sqlColAttribute, "SQLColAttribute"},
		{&sqlBindCol, "SQLBindCol"},
		{&sqlGetData, "SQLGetData"},
		{&sqlBindParameter, "SQLBindParameter"},
		{&sqlNumParams, "SQLNumParams"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerTransactionFunctions registers transaction-related DB2 CLI functions
func registerTransactionFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlEndTran, "SQLEndTran"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerDiagnosticFunctions registers diagnostic-related DB2 CLI functions
func registerDiagnosticFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlGetDiagRec, "SQLGetDiagRec"},
		{&sqlGetDiagField, "SQLGetDiagField"},
		{&sqlError, "SQLError"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerMetadataFunctions registers catalog/metadata DB2 CLI functions
func registerMetadataFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlTables, "SQLTables"},
		{&sqlColumns, "SQLColumns"},
		{&sqlPrimaryKeys, "SQLPrimaryKeys"},
		{&sqlForeignKeys, "SQLForeignKeys"},
		{&sqlStatistics, "SQLStatistics"},
		{&sqlSpecialColumns, "SQLSpecialColumns"},
		{&sqlTablePrivileges, "SQLTablePrivileges"},
		{&sqlColumnPrivileges, "SQLColumnPrivileges"},
		{&sqlProcedures, "SQLProcedures"},
		{&sqlProcedureColumns, "SQLProcedureColumns"},
		{&sqlGetTypeInfo, "SQLGetTypeInfo"},
		{&sqlGetFunctions, "SQLGetFunctions"},
		{&sqlDataSources, "SQLDataSources"},
		{&sqlDescribeParam, "SQLDescribeParam"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerCursorFunctions registers cursor and extended-fetch DB2 CLI functions
func registerCursorFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlExtendedFetch, "SQLExtendedFetch"},
		{&sqlSetPos, "SQLSetPos"},
		{&sqlBulkOperations, "SQLBulkOperations"},
		{&sqlNextResult, "SQLNextResult"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerDataAtExecFunctions registers data-at-execution DB2 CLI functions
func registerDataAtExecFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlParamData, "SQLParamData"},
		{&sqlPutData, "SQLPutData"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerExtendedConnFunctions registers extended connection and utility functions
func registerExtendedConnFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlBrowseConnect, "SQLBrowseConnect"},
		{&sqlNativeSql, "SQLNativeSql"},
		{&sqlGetEnvAttr, "SQLGetEnvAttr"},
		{&sqlGetCursorName, "SQLGetCursorName"},
		{&sqlSetCursorName, "SQLSetCursorName"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// registerDescriptorFunctions registers descriptor DB2 CLI functions
func registerDescriptorFunctions() error {
	fns := []struct {
		ptr  any
		name string
	}{
		{&sqlCopyDesc, "SQLCopyDesc"},
		{&sqlGetDescField, "SQLGetDescField"},
		{&sqlSetDescField, "SQLSetDescField"},
		{&sqlGetDescRec, "SQLGetDescRec"},
		{&sqlSetDescRec, "SQLSetDescRec"},
	}

	for _, fn := range fns {
		purego.RegisterLibFunc(fn.ptr, libHandle, fn.name)
	}

	return nil
}

// Public wrapper functions that call the registered function pointers.
// Each wrapper loads the DB2 CLI shared library on first call and returns
// SQL_ERROR immediately when the library is unavailable, so callers do not
// need a separate pre-flight check.

// SQLAllocHandle allocates a new DB2 CLI handle of the type given by handleType
// (SQL_HANDLE_ENV, SQL_HANDLE_DBC, SQL_HANDLE_STMT, or SQL_HANDLE_DESC).
// For SQL_HANDLE_ENV pass SQL_NULL_HANDLE as inputHandle. For SQL_HANDLE_DBC
// pass a valid SQLHENV cast to SQLHANDLE. For SQL_HANDLE_STMT pass a valid
// SQLHDBC cast to SQLHANDLE. The allocated handle is written to *outputHandle.
// Every allocated handle must eventually be released with SQLFreeHandle to
// avoid resource leaks inside the DB2 client library.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlallochandle
// ODBC equivalent: SQLAllocHandle
func SQLAllocHandle(handleType SQLSMALLINT, inputHandle SQLHANDLE, outputHandle *SQLHANDLE) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlAllocHandle(handleType, inputHandle, outputHandle)
}

// SQLFreeHandle releases a DB2 CLI handle previously allocated by SQLAllocHandle.
// For SQL_HANDLE_STMT it also closes any open cursor and discards pending results.
// For SQL_HANDLE_DBC the connection must already be disconnected via SQLDisconnect.
// For SQL_HANDLE_ENV all child connection handles must already be freed.
// Returns SQL_INVALID_HANDLE if the handle has already been freed or was never allocated.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlfreehandle
// ODBC equivalent: SQLFreeHandle
func SQLFreeHandle(handleType SQLSMALLINT, handle SQLHANDLE) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlFreeHandle(handleType, handle)
}

// SQLConnect establishes a connection to a DB2 data source identified by dsn
// (a data-source name registered in db2dsdriver.cfg or odbc.ini). uid and pwd
// are the user ID and password. SQLDriverConnect is preferred for
// keyword=value connection strings; SQLConnect requires a pre-configured DSN.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlconnect
// ODBC equivalent: SQLConnect
func SQLConnect(hdbc SQLHDBC, dsn, uid, pwd string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}

	dsnBytes := append([]byte(dsn), 0)
	uidBytes := append([]byte(uid), 0)
	pwdBytes := append([]byte(pwd), 0)

	ret := sqlConnect(
		hdbc,
		(*SQLCHAR)(unsafe.Pointer(&dsnBytes[0])), SQL_NTS,
		(*SQLCHAR)(unsafe.Pointer(&uidBytes[0])), SQL_NTS,
		(*SQLCHAR)(unsafe.Pointer(&pwdBytes[0])), SQL_NTS,
	)
	runtime.KeepAlive(dsnBytes)
	runtime.KeepAlive(uidBytes)
	runtime.KeepAlive(pwdBytes)
	// Zero credential buffers to shrink the window during which plaintext
	// credentials linger as GC-reachable memory.
	clear(pwdBytes)
	clear(uidBytes)
	return ret
}

// SQLDriverConnect establishes a DB2 connection using a keyword=value connection
// string (e.g. "DATABASE=SAMPLE;HOSTNAME=host;PORT=50000;PROTOCOL=TCPIP;UID=u;PWD=p").
// It is the preferred connection function because it supports all connection
// parameters without requiring a pre-configured data-source name. The completed
// connection string (with driver-supplied defaults) is returned as the first
// result value and may be longer than the input.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqldriverconnect
// ODBC equivalent: SQLDriverConnect
func SQLDriverConnect(hdbc SQLHDBC, connStr string) (string, SQLRETURN) {
	if err := LoadLibrary(); err != nil {
		return "", SQL_ERROR
	}

	connStrBytes := append([]byte(connStr), 0)
	outConnStr := make([]byte, 4096) // 4 KB — connection strings can exceed 1 KB
	var outConnStrLen SQLSMALLINT

	ret := sqlDriverConnect(
		hdbc,
		0, // No window handle
		(*SQLCHAR)(unsafe.Pointer(&connStrBytes[0])), SQL_NTS,
		(*SQLCHAR)(unsafe.Pointer(&outConnStr[0])), 4096,
		&outConnStrLen,
		0, // SQL_DRIVER_NOPROMPT
	)
	runtime.KeepAlive(connStrBytes)
	runtime.KeepAlive(outConnStr)
	// Zero the input connection string to reduce the window during which
	// plaintext PWD= credentials linger as GC-reachable memory.
	clear(connStrBytes)

	// Clamp output length: per ODBC spec outConnStrLen is the required length
	// before truncation and may exceed buffer capacity. A misbehaving driver
	// returning a negative length would cause a runtime panic without the
	// max(0, …) guard — see the same pattern in driver.go:Columns().
	n := max(0, min(int(outConnStrLen), len(outConnStr)))
	result := string(outConnStr[:n])
	// Zero the output buffer to reduce the window during which the plaintext
	// PWD= keyword lingers as GC-reachable memory. Note: result already holds
	// a copy of those bytes and will persist until the caller releases it.
	clear(outConnStr)
	return result, ret
}

// SQLDisconnect closes an active connection to a DB2 data source. After
// SQLDisconnect returns SQL_SUCCESS, all open statement handles on hdbc are
// invalid and must be freed before the connection handle itself is freed.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqldisconnect
// ODBC equivalent: SQLDisconnect
func SQLDisconnect(hdbc SQLHDBC) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlDisconnect(hdbc)
}

// SQLSetEnvAttr sets an attribute on an environment handle. The most important
// attribute is SQL_ATTR_ODBC_VERSION, which must be set to SQL_OV_ODBC3 before
// allocating connection handles. strLen is the byte length when value is a
// string pointer; use 0 for integer attributes.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlsetenvattr
// ODBC equivalent: SQLSetEnvAttr
func SQLSetEnvAttr(henv SQLHENV, attr SQLINTEGER, value uintptr, strLen SQLINTEGER) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlSetEnvAttr(henv, attr, value, strLen)
}

// SQLSetConnectAttr sets an attribute on a connection handle. Commonly used to
// enable or disable autocommit (SQL_ATTR_AUTOCOMMIT) and to set the transaction
// isolation level (SQL_ATTR_TXN_ISOLATION). strLen is ignored for integer
// attributes; pass SQL_NTS for string attributes.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlsetconnectattr
// ODBC equivalent: SQLSetConnectAttr
func SQLSetConnectAttr(hdbc SQLHDBC, attr SQLINTEGER, value uintptr, strLen SQLINTEGER) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlSetConnectAttr(hdbc, attr, value, strLen)
}

// SQLGetInfo retrieves general information about the connected DB2 driver and
// data source. infoType selects the property (e.g. SQL_DBMS_VER, SQL_DBMS_NAME).
// infoValue is a caller-allocated buffer; strLen receives the actual byte length
// of string results (may exceed bufLen, indicating truncation).
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlgetinfo
// ODBC equivalent: SQLGetInfo
func SQLGetInfo(hdbc SQLHDBC, infoType SQLUSMALLINT, infoValue SQLPOINTER, bufLen SQLSMALLINT, strLen *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetInfo(hdbc, infoType, infoValue, bufLen, strLen)
}

// SQLExecDirect prepares and executes a SQL statement in a single call.
// Use SQLPrepare + SQLExecute when the same statement will be executed multiple
// times with different parameter values. SQLExecDirect is convenient for
// one-shot DDL (CREATE TABLE, DROP TABLE) where parse overhead is acceptable.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlexecdirect
// ODBC equivalent: SQLExecDirect
func SQLExecDirect(hstmt SQLHSTMT, sql string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}

	sqlBytes := append([]byte(sql), 0)
	ret := sqlExecDirect(hstmt, (*SQLCHAR)(unsafe.Pointer(&sqlBytes[0])), SQLINTEGER(len(sql)))
	runtime.KeepAlive(sqlBytes)
	return ret
}

// SQLPrepare parses and compiles a SQL statement on the DB2 server and stores
// the access plan in hstmt. After SQLPrepare succeeds, call SQLBindParameter
// to bind input parameters (if any), then SQLExecute to run the statement.
// Reusing a prepared statement for repeated executions avoids re-parse overhead.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlprepare
// ODBC equivalent: SQLPrepare
func SQLPrepare(hstmt SQLHSTMT, sql string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}

	sqlBytes := append([]byte(sql), 0)
	ret := sqlPrepare(hstmt, (*SQLCHAR)(unsafe.Pointer(&sqlBytes[0])), SQLINTEGER(len(sql)))
	runtime.KeepAlive(sqlBytes)
	return ret
}

// SQLExecute executes a statement previously prepared by SQLPrepare. All
// input parameters must be bound via SQLBindParameter before this call.
// For SELECT statements, the result set cursor is positioned before the first
// row; call SQLFetch to advance it.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlexecute
// ODBC equivalent: SQLExecute
func SQLExecute(hstmt SQLHSTMT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlExecute(hstmt)
}

// SQLFetch advances the cursor to the next row in the result set and retrieves
// data for all bound columns (those bound via SQLBindCol). Returns SQL_NO_DATA
// when all rows have been consumed. For unbound columns, call SQLGetData after
// SQLFetch to retrieve each column value individually.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlfetch
// ODBC equivalent: SQLFetch
func SQLFetch(hstmt SQLHSTMT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlFetch(hstmt)
}

// SQLFetchScroll positions the cursor using fetchOrientation (SQL_FETCH_NEXT,
// SQL_FETCH_FIRST, SQL_FETCH_PRIOR, SQL_FETCH_ABSOLUTE, SQL_FETCH_RELATIVE)
// and retrieves the rowset. Requires a scrollable cursor
// (SQL_ATTR_CURSOR_TYPE != SQL_CURSOR_FORWARD_ONLY). For forward-only cursors
// use SQLFetch instead.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlfetchscroll
// ODBC equivalent: SQLFetchScroll
func SQLFetchScroll(hstmt SQLHSTMT, fetchOrientation SQLSMALLINT, fetchOffset SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlFetchScroll(hstmt, fetchOrientation, fetchOffset)
}

// SQLCloseCursor closes the cursor associated with hstmt and discards any
// unfetched rows. The statement handle remains allocated and may be re-executed.
// Returns SQLSTATE 24000 (invalid cursor state) if no cursor is open; the
// driver's db2Rows.Close method treats 24000 as a no-op.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlclosecursor
// ODBC equivalent: SQLCloseCursor
func SQLCloseCursor(hstmt SQLHSTMT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlCloseCursor(hstmt)
}

// SQLFreeStmt performs one of several statement-handle cleanup operations
// selected by option: SQL_CLOSE (close cursor), SQL_DROP (close and free handle),
// SQL_UNBIND (release SQLBindCol bindings), or SQL_RESET_PARAMS (release
// SQLBindParameter bindings). Prefer SQLFreeHandle(SQL_HANDLE_STMT, hstmt) for
// full handle deallocation; SQLFreeStmt(SQL_DROP) is an alias.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlfreestmt
// ODBC equivalent: SQLFreeStmt
func SQLFreeStmt(hstmt SQLHSTMT, option SQLUSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlFreeStmt(hstmt, option)
}

// SQLNumResultCols returns the number of columns in the result set produced by
// a prepared or executed SELECT statement. *colCount is set to 0 if the
// statement is not a SELECT (INSERT/UPDATE/DELETE). Call after SQLPrepare or
// SQLExecute, before the first SQLFetch.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlnumresultcols
// ODBC equivalent: SQLNumResultCols
func SQLNumResultCols(hstmt SQLHSTMT, colCount *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlNumResultCols(hstmt, colCount)
}

// SQLDescribeCol returns the name, data type, column size, decimal digits, and
// nullability of column colNum (1-based) in the result set. colName is a
// caller-allocated buffer of colNameMax bytes; colNameLen receives the actual
// name length (may exceed colNameMax, indicating truncation).
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqldescribecol
// ODBC equivalent: SQLDescribeCol
func SQLDescribeCol(hstmt SQLHSTMT, colNum SQLUSMALLINT, colName *SQLCHAR, colNameMax SQLSMALLINT, colNameLen, dataType *SQLSMALLINT, colSize *SQLULEN, decimalDigits, nullable *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlDescribeCol(hstmt, colNum, colName, colNameMax, colNameLen, dataType, colSize, decimalDigits, nullable)
}

// SQLGetData retrieves the value of column colNum (1-based) in the current row
// into targetValue, converting it to targetType (e.g. SQL_C_CHAR). If the value
// is longer than bufLen, DB2 returns SQL_SUCCESS_WITH_INFO and writes partial data;
// call SQLGetData again to retrieve the remainder. *indicator receives the total
// length of the value (SQL_NULL_DATA if the column is NULL, SQL_NO_TOTAL if the
// length is unknown). The driver's readColumnValue method loops on this behaviour.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlgetdata
// ODBC equivalent: SQLGetData
func SQLGetData(hstmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetData(hstmt, colNum, targetType, targetValue, bufLen, indicator)
}

// SQLBindCol associates column colNum (1-based) with the application buffer at
// targetValue. On each SQLFetch, DB2 writes the converted column value directly
// into the bound buffer. This package uses SQLGetData exclusively (unbound
// columns) so SQLBindCol is provided for completeness; it is not called in the
// CDC streaming path.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlbindcol
// ODBC equivalent: SQLBindCol
func SQLBindCol(hstmt SQLHSTMT, colNum SQLUSMALLINT, targetType SQLSMALLINT, targetValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlBindCol(hstmt, colNum, targetType, targetValue, bufLen, indicator)
}

// SQLRowCount returns the number of rows affected by the most recent INSERT,
// UPDATE, DELETE, or MERGE statement. For SELECT statements the value is -1
// (unknown) until all rows have been fetched. *rowCount is populated on
// SQL_SUCCESS.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlrowcount
// ODBC equivalent: SQLRowCount
func SQLRowCount(hstmt SQLHSTMT, rowCount *SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlRowCount(hstmt, rowCount)
}

// SQLEndTran commits or rolls back a transaction on the given handle.
// Pass SQL_HANDLE_DBC + the connection handle to commit/rollback all statements
// opened on that connection. completionType must be SQL_COMMIT or SQL_ROLLBACK.
// After SQLEndTran, autocommit should be restored via SQLSetConnectAttr if it
// was disabled for the transaction (the db2Tx methods handle this automatically).
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlendtran
// ODBC equivalent: SQLEndTran
func SQLEndTran(handleType SQLSMALLINT, handle SQLHANDLE, completionType SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlEndTran(handleType, handle, completionType)
}

// SQLGetDiagRec retrieves the recNum-th (1-based) diagnostic record associated
// with handle after a CLI function failure. sqlState receives the five-character
// SQLSTATE string. nativeError receives the DB2-specific error code. messageText
// receives the human-readable message (bufLen bytes max); textLen receives the
// actual message length. Returns SQL_NO_DATA when all records have been read.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlgetdiagrec
// ODBC equivalent: SQLGetDiagRec
func SQLGetDiagRec(handleType SQLSMALLINT, handle SQLHANDLE, recNum SQLSMALLINT, sqlState *SQLCHAR, nativeError *SQLINTEGER, messageText *SQLCHAR, bufLen SQLSMALLINT, textLen *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetDiagRec(handleType, handle, recNum, sqlState, nativeError, messageText, bufLen, textLen)
}

// SQLBindParameter binds the paramNum-th (1-based) parameter marker in the
// prepared statement to paramValue. inputOutputType is SQL_PARAM_INPUT for
// normal query parameters. valueType and paramType select the C buffer type and
// SQL column type respectively (SQL_C_CHAR / SQL_VARCHAR for all parameters in
// this driver). colSize is the column width; bufLen is the buffer capacity.
// The indicator argument must point to a SQLLEN that is kept alive until after
// SQLExecute returns (see bindParams in driver.go for the KeepAlive pattern).
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlbindparameter
// ODBC equivalent: SQLBindParameter
func SQLBindParameter(hstmt SQLHSTMT, paramNum SQLUSMALLINT, inputOutputType, valueType, paramType SQLSMALLINT, colSize SQLULEN, decimalDigits SQLSMALLINT, paramValue SQLPOINTER, bufLen SQLLEN, indicator *SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlBindParameter(hstmt, paramNum, inputOutputType, valueType, paramType, colSize, decimalDigits, paramValue, bufLen, indicator)
}

// SQLCancel requests cancellation of a SQL statement that is currently executing
// on hstmt. It is safe to call from a different goroutine while another goroutine
// is blocked inside SQLExecute or SQLFetch — the DB2 CLI library signals an
// interrupt and the blocked call returns SQL_ERROR (SQLSTATE HY008). The
// QueryContext implementation in driver.go uses this to honour context cancellation.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlcancel
// ODBC equivalent: SQLCancel
func SQLCancel(hstmt SQLHSTMT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlCancel(hstmt)
}

// SQLGetConnectAttr retrieves the current value of a connection attribute set
// by SQLSetConnectAttr. For integer attributes value should point to a uintptr;
// for string attributes it should point to a byte buffer of bufLen bytes.
// strLen receives the actual byte length of string results.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlgetconnectattr
// ODBC equivalent: SQLGetConnectAttr
func SQLGetConnectAttr(hdbc SQLHDBC, attr SQLINTEGER, value SQLPOINTER, bufLen SQLINTEGER, strLen *SQLINTEGER) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetConnectAttr(hdbc, attr, value, bufLen, strLen)
}

// --- Metadata / Catalog wrappers ---

// SQLTables returns a list of tables stored in the data source.
func SQLTables(hstmt SQLHSTMT, catalogName, schemaName, tableName, tableType string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	cat, catLen, b0 := sqlStrPtr(catalogName)
	sch, schLen, b1 := sqlStrPtr(schemaName)
	tbl, tblLen, b2 := sqlStrPtr(tableName)
	typ, typLen, b3 := sqlStrPtr(tableType)
	ret := sqlTables(hstmt, cat, catLen, sch, schLen, tbl, tblLen, typ, typLen)
	runtime.KeepAlive(b0)
	runtime.KeepAlive(b1)
	runtime.KeepAlive(b2)
	runtime.KeepAlive(b3)
	return ret
}

// SQLColumns returns a list of columns in the specified tables.
func SQLColumns(hstmt SQLHSTMT, catalogName, schemaName, tableName, columnName string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	cat, catLen, b0 := sqlStrPtr(catalogName)
	sch, schLen, b1 := sqlStrPtr(schemaName)
	tbl, tblLen, b2 := sqlStrPtr(tableName)
	col, colLen, b3 := sqlStrPtr(columnName)
	ret := sqlColumns(hstmt, cat, catLen, sch, schLen, tbl, tblLen, col, colLen)
	runtime.KeepAlive(b0)
	runtime.KeepAlive(b1)
	runtime.KeepAlive(b2)
	runtime.KeepAlive(b3)
	return ret
}

// SQLPrimaryKeys returns the column names that make up the primary key of a table.
func SQLPrimaryKeys(hstmt SQLHSTMT, catalogName, schemaName, tableName string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	cat, catLen, b0 := sqlStrPtr(catalogName)
	sch, schLen, b1 := sqlStrPtr(schemaName)
	tbl, tblLen, b2 := sqlStrPtr(tableName)
	ret := sqlPrimaryKeys(hstmt, cat, catLen, sch, schLen, tbl, tblLen)
	runtime.KeepAlive(b0)
	runtime.KeepAlive(b1)
	runtime.KeepAlive(b2)
	return ret
}

// SQLForeignKeys returns the foreign key columns of a table and the primary key columns they reference.
func SQLForeignKeys(hstmt SQLHSTMT, pkCatalogName, pkSchemaName, pkTableName, fkCatalogName, fkSchemaName, fkTableName string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	pkCat, pkCatLen, b0 := sqlStrPtr(pkCatalogName)
	pkSch, pkSchLen, b1 := sqlStrPtr(pkSchemaName)
	pkTbl, pkTblLen, b2 := sqlStrPtr(pkTableName)
	fkCat, fkCatLen, b3 := sqlStrPtr(fkCatalogName)
	fkSch, fkSchLen, b4 := sqlStrPtr(fkSchemaName)
	fkTbl, fkTblLen, b5 := sqlStrPtr(fkTableName)
	ret := sqlForeignKeys(hstmt, pkCat, pkCatLen, pkSch, pkSchLen, pkTbl, pkTblLen, fkCat, fkCatLen, fkSch, fkSchLen, fkTbl, fkTblLen)
	runtime.KeepAlive(b0)
	runtime.KeepAlive(b1)
	runtime.KeepAlive(b2)
	runtime.KeepAlive(b3)
	runtime.KeepAlive(b4)
	runtime.KeepAlive(b5)
	return ret
}

// SQLStatistics returns statistics about a table and the indexes on it.
func SQLStatistics(hstmt SQLHSTMT, catalogName, schemaName, tableName string, unique, accuracy SQLUSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	cat, catLen, b0 := sqlStrPtr(catalogName)
	sch, schLen, b1 := sqlStrPtr(schemaName)
	tbl, tblLen, b2 := sqlStrPtr(tableName)
	ret := sqlStatistics(hstmt, cat, catLen, sch, schLen, tbl, tblLen, unique, accuracy)
	runtime.KeepAlive(b0)
	runtime.KeepAlive(b1)
	runtime.KeepAlive(b2)
	return ret
}

// SQLSpecialColumns returns information about the optimal set of columns that uniquely identify a row.
func SQLSpecialColumns(hstmt SQLHSTMT, colType SQLUSMALLINT, catalogName, schemaName, tableName string, scope, nullable SQLUSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	cat, catLen, b0 := sqlStrPtr(catalogName)
	sch, schLen, b1 := sqlStrPtr(schemaName)
	tbl, tblLen, b2 := sqlStrPtr(tableName)
	ret := sqlSpecialColumns(hstmt, colType, cat, catLen, sch, schLen, tbl, tblLen, scope, nullable)
	runtime.KeepAlive(b0)
	runtime.KeepAlive(b1)
	runtime.KeepAlive(b2)
	return ret
}

// SQLTablePrivileges returns the table privileges for one or more tables.
func SQLTablePrivileges(hstmt SQLHSTMT, catalogName, schemaName, tableName string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	cat, catLen, b0 := sqlStrPtr(catalogName)
	sch, schLen, b1 := sqlStrPtr(schemaName)
	tbl, tblLen, b2 := sqlStrPtr(tableName)
	ret := sqlTablePrivileges(hstmt, cat, catLen, sch, schLen, tbl, tblLen)
	runtime.KeepAlive(b0)
	runtime.KeepAlive(b1)
	runtime.KeepAlive(b2)
	return ret
}

// SQLColumnPrivileges returns the column privileges for one or more columns in a table.
func SQLColumnPrivileges(hstmt SQLHSTMT, catalogName, schemaName, tableName, columnName string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	cat, catLen, b0 := sqlStrPtr(catalogName)
	sch, schLen, b1 := sqlStrPtr(schemaName)
	tbl, tblLen, b2 := sqlStrPtr(tableName)
	col, colLen, b3 := sqlStrPtr(columnName)
	ret := sqlColumnPrivileges(hstmt, cat, catLen, sch, schLen, tbl, tblLen, col, colLen)
	runtime.KeepAlive(b0)
	runtime.KeepAlive(b1)
	runtime.KeepAlive(b2)
	runtime.KeepAlive(b3)
	return ret
}

// SQLProcedures returns the list of procedure names stored in the data source.
func SQLProcedures(hstmt SQLHSTMT, catalogName, schemaName, procName string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	cat, catLen, b0 := sqlStrPtr(catalogName)
	sch, schLen, b1 := sqlStrPtr(schemaName)
	prc, prcLen, b2 := sqlStrPtr(procName)
	ret := sqlProcedures(hstmt, cat, catLen, sch, schLen, prc, prcLen)
	runtime.KeepAlive(b0)
	runtime.KeepAlive(b1)
	runtime.KeepAlive(b2)
	return ret
}

// SQLProcedureColumns returns the list of input and output parameters for a procedure.
func SQLProcedureColumns(hstmt SQLHSTMT, catalogName, schemaName, procName, columnName string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	cat, catLen, b0 := sqlStrPtr(catalogName)
	sch, schLen, b1 := sqlStrPtr(schemaName)
	prc, prcLen, b2 := sqlStrPtr(procName)
	col, colLen, b3 := sqlStrPtr(columnName)
	ret := sqlProcedureColumns(hstmt, cat, catLen, sch, schLen, prc, prcLen, col, colLen)
	runtime.KeepAlive(b0)
	runtime.KeepAlive(b1)
	runtime.KeepAlive(b2)
	runtime.KeepAlive(b3)
	return ret
}

// SQLGetTypeInfo returns information about data types supported by the data source.
func SQLGetTypeInfo(hstmt SQLHSTMT, dataType SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetTypeInfo(hstmt, dataType)
}

// SQLGetFunctions returns information about whether a driver supports a specific ODBC function.
func SQLGetFunctions(hdbc SQLHDBC, function SQLUSMALLINT, supported *SQLUSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetFunctions(hdbc, function, supported)
}

// SQLDataSources returns a list of data source names.
func SQLDataSources(henv SQLHENV, direction SQLUSMALLINT, serverName *SQLCHAR, serverNameMax SQLSMALLINT, serverNameLen *SQLSMALLINT, description *SQLCHAR, descriptionMax SQLSMALLINT, descriptionLen *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlDataSources(henv, direction, serverName, serverNameMax, serverNameLen, description, descriptionMax, descriptionLen)
}

// SQLDescribeParam returns the description of a parameter marker in an SQL statement.
func SQLDescribeParam(hstmt SQLHSTMT, paramNum SQLUSMALLINT, dataType *SQLSMALLINT, paramSize *SQLULEN, decimalDigits *SQLSMALLINT, nullable *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlDescribeParam(hstmt, paramNum, dataType, paramSize, decimalDigits, nullable)
}

// --- Cursor / Extended Fetch wrappers ---

// SQLExtendedFetch fetches the specified rowset of data from the result set.
//
// Deprecated: superseded by SQLFetchScroll; retained for legacy code.
func SQLExtendedFetch(hstmt SQLHSTMT, fetchType SQLUSMALLINT, irow SQLLEN, rowCountPtr *SQLULEN, rowStatusArray *SQLUSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlExtendedFetch(hstmt, fetchType, irow, rowCountPtr, rowStatusArray)
}

// SQLSetPos sets the cursor position within a rowset and allows an application to refresh, update, or delete data.
func SQLSetPos(hstmt SQLHSTMT, rowNum SQLULEN, operation SQLUSMALLINT, lockType SQLUSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlSetPos(hstmt, rowNum, operation, lockType)
}

// SQLBulkOperations performs bulk insertions and bulk bookmark operations.
func SQLBulkOperations(hstmt SQLHSTMT, operation SQLUSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlBulkOperations(hstmt, operation)
}

// SQLNextResult moves to the next result set when a stored procedure returns multiple result sets.
func SQLNextResult(hstmtSource SQLHSTMT, hstmtTarget SQLHSTMT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlNextResult(hstmtSource, hstmtTarget)
}

// --- Data-at-Execution wrappers ---

// SQLParamData is used in conjunction with SQLPutData to supply parameter data at execution time.
func SQLParamData(hstmt SQLHSTMT, value *SQLPOINTER) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlParamData(hstmt, value)
}

// SQLPutData allows an application to send data for a parameter or column at statement execution time.
func SQLPutData(hstmt SQLHSTMT, data SQLPOINTER, strLen SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlPutData(hstmt, data, strLen)
}

// --- Extended Connection / Utility wrappers ---

// SQLBrowseConnect returns successive levels of connection attributes and valid attribute values.
func SQLBrowseConnect(hdbc SQLHDBC, connStrIn string) (string, SQLRETURN) {
	if err := LoadLibrary(); err != nil {
		return "", SQL_ERROR
	}
	connStrInBytes := append([]byte(connStrIn), 0)
	outBuf := make([]byte, 4096)
	var outLen SQLSMALLINT
	ret := sqlBrowseConnect(
		hdbc,
		(*SQLCHAR)(unsafe.Pointer(&connStrInBytes[0])), SQL_NTS,
		(*SQLCHAR)(unsafe.Pointer(&outBuf[0])), 4096,
		&outLen,
	)
	runtime.KeepAlive(connStrInBytes)
	// Zero input connection string (may contain PWD= credentials) and output buffer
	// to reduce the window during which plaintext credentials linger as GC-reachable
	// memory. Note: result already holds a copy of outBuf bytes and persists until
	// the caller releases its reference.
	clear(connStrInBytes)
	// max(0, …): ODBC spec allows the driver to write SQL_NO_TOTAL (-4) into
	// outLen to indicate truncation; guard against a negative slice index.
	n := max(0, min(int(outLen), len(outBuf)))
	result := string(outBuf[:n])
	clear(outBuf)
	return result, ret
}

// SQLNativeSql translates an SQL string containing escape sequences into the native SQL grammar of the data source.
func SQLNativeSql(hdbc SQLHDBC, sqlStrIn string) (string, SQLRETURN) {
	if err := LoadLibrary(); err != nil {
		return "", SQL_ERROR
	}
	inBytes := append([]byte(sqlStrIn), 0)
	outBuf := make([]byte, 4096)
	var outLen SQLINTEGER
	ret := sqlNativeSql(
		hdbc,
		(*SQLCHAR)(unsafe.Pointer(&inBytes[0])), SQL_NTS,
		(*SQLCHAR)(unsafe.Pointer(&outBuf[0])), 4096,
		&outLen,
	)
	// max(0, …): guard against SQL_NO_TOTAL (-4) or other negative indicator values.
	n := max(0, min(int(outLen), len(outBuf)))
	return string(outBuf[:n]), ret
}

// SQLGetEnvAttr returns the current setting of an environment attribute.
func SQLGetEnvAttr(henv SQLHENV, attr SQLINTEGER, value SQLPOINTER, bufLen SQLINTEGER, strLen *SQLINTEGER) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetEnvAttr(henv, attr, value, bufLen, strLen)
}

// SQLGetCursorName returns the cursor name associated with a statement handle.
func SQLGetCursorName(hstmt SQLHSTMT, cursor *SQLCHAR, cursorMax SQLSMALLINT, cursorLen *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetCursorName(hstmt, cursor, cursorMax, cursorLen)
}

// SQLSetCursorName associates a cursor name with a statement handle.
func SQLSetCursorName(hstmt SQLHSTMT, cursorName string) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	b := append([]byte(cursorName), 0)
	return sqlSetCursorName(hstmt, (*SQLCHAR)(unsafe.Pointer(&b[0])), SQL_NTS)
}

// --- Descriptor wrappers ---

// SQLCopyDesc copies descriptor information from one descriptor handle to another.
func SQLCopyDesc(srcDesc SQLHDESC, dstDesc SQLHDESC) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlCopyDesc(srcDesc, dstDesc)
}

// SQLGetDescField returns the current setting or value of a single field of a descriptor record.
func SQLGetDescField(descHandle SQLHDESC, recNum SQLSMALLINT, fieldID SQLSMALLINT, value SQLPOINTER, bufLen SQLINTEGER, strLen *SQLINTEGER) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetDescField(descHandle, recNum, fieldID, value, bufLen, strLen)
}

// SQLSetDescField sets the value of a single field of a descriptor record.
func SQLSetDescField(descHandle SQLHDESC, recNum SQLSMALLINT, fieldID SQLSMALLINT, value SQLPOINTER, bufLen SQLINTEGER) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlSetDescField(descHandle, recNum, fieldID, value, bufLen)
}

// SQLGetDescRec returns the current settings or values of the multiple fields of a descriptor record.
func SQLGetDescRec(descHandle SQLHDESC, recNum SQLSMALLINT, name *SQLCHAR, nameMax SQLSMALLINT, nameLen *SQLSMALLINT, dataType *SQLSMALLINT, subType *SQLSMALLINT, length *SQLLEN, precision *SQLSMALLINT, scale *SQLSMALLINT, nullable *SQLSMALLINT) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlGetDescRec(descHandle, recNum, name, nameMax, nameLen, dataType, subType, length, precision, scale, nullable)
}

// SQLSetDescRec sets multiple descriptor fields that affect the data type and buffer bound to a column or parameter data.
func SQLSetDescRec(descHandle SQLHDESC, recNum SQLSMALLINT, dataType SQLSMALLINT, subType SQLSMALLINT, length SQLLEN, precision SQLSMALLINT, scale SQLSMALLINT, data SQLPOINTER, strLen *SQLLEN, indicator *SQLLEN) SQLRETURN {
	if err := LoadLibrary(); err != nil {
		return SQL_ERROR
	}
	return sqlSetDescRec(descHandle, recNum, dataType, subType, length, precision, scale, data, strLen, indicator)
}

// sqlStrPtr converts a Go string to a *SQLCHAR pointer, length sentinel, and
// the backing []byte buffer. Callers MUST call runtime.KeepAlive on the returned
// buffer after the native ODBC function returns — the GC does not trace
// unsafe.Pointer and could collect the buffer mid-call without the keepalive.
// An empty string returns (nil, 0, nil) — the ODBC convention for "no filter".
//
// SQL_NTS (-3) is the ODBC sentinel for "null-terminated string": it instructs
// the driver to measure the length itself. This avoids a SQLSMALLINT (16-bit)
// overflow that would silently truncate or sign-flip any string ≥ 32 KiB.
func sqlStrPtr(s string) (*SQLCHAR, SQLSMALLINT, []byte) {
	if s == "" {
		return nil, 0, nil
	}
	b := append([]byte(s), 0)
	return (*SQLCHAR)(unsafe.Pointer(&b[0])), SQL_NTS, b
}
