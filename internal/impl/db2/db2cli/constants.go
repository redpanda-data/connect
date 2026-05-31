// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package db2cli implements IBM DB2 Call Level Interface (CLI) bindings via
// purego. Using purego instead of CGO means the DB2 driver compiles without a
// C toolchain and without linking against libdb2.so at build time — the shared
// library is loaded at runtime via dlopen/LoadLibrary. This shifts the IBM DB2
// client runtime requirement from the build machine to the deployment
// environment, enabling a single pre-built binary that works wherever
// libdb2.so (Linux) or libdb2.dylib (macOS) is installed.
//
// Why purego instead of github.com/ibmdb/go_ibm_db?
//
// The IBM-owned go_ibm_db driver works via CGO, embedding a dependency on
// gcc and the DB2 C++ runtime (libdb2.so / clidriver) at compile time.
// This makes cross-compilation and pre-built binary distribution impossible:
// every build machine needs the full IBM Data Server Client installed.
// purego loads the same shared library at runtime, keeping the build entirely
// pure Go — no C toolchain, no CGO, no build-time linking against IBM code.
// The tradeoff is that the IBM CLI library must be present on the deployment
// host (not the build host), which matches how other Redpanda Connect
// enterprise connectors handle optional native dependencies.
//
// DB2 CLI is IBM's C-based call-level interface that mirrors the ODBC API.
// Every exported function in this package is a thin Go wrapper around the
// corresponding C function loaded from the DB2 client shared library. The
// constant names, numeric values, and semantics are identical to ODBC and to
// the IBM DB2 CLI reference documentation at
// https://www.ibm.com/docs/en/db2/11.5?topic=interfaces-db2-cli
package db2cli

// DB2 CLI constants used throughout the IBM DB2 CLI API.
// Full constant reference: https://www.ibm.com/docs/en/db2/11.5?topic=reference-cli-constants

// Handle type constants passed as the first argument to SQLAllocHandle and
// SQLFreeHandle to indicate which kind of handle to allocate or release.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=reference-handle-types
const (
	// SQL_HANDLE_ENV selects an environment handle, which holds global state
	// (ODBC version, connection pooling mode) shared across all connections.
	SQL_HANDLE_ENV = 1

	// SQL_HANDLE_DBC selects a connection handle, which represents one
	// physical TCP/IP connection to a DB2 instance.
	SQL_HANDLE_DBC = 2

	// SQL_HANDLE_STMT selects a statement handle, which represents one
	// prepared or executed SQL statement and its associated cursor.
	SQL_HANDLE_STMT = 3

	// SQL_HANDLE_DESC selects a descriptor handle, which describes the
	// structure of a parameter or column (type, length, precision, etc.).
	SQL_HANDLE_DESC = 4
)

// Return codes returned by every DB2 CLI function call.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=cli-return-codes
// ODBC reference: https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/return-codes-odbc
const (
	// SQL_SUCCESS indicates the function completed without warnings or errors.
	SQL_SUCCESS SQLRETURN = 0

	// SQL_SUCCESS_WITH_INFO indicates success, but diagnostic records are
	// available via SQLGetDiagRec (e.g. string truncation warnings).
	SQL_SUCCESS_WITH_INFO SQLRETURN = 1

	// SQL_NO_DATA indicates that no more rows are available in the result set
	// (returned by SQLFetch when the cursor is exhausted) or that no data was
	// found for a positioned operation.
	SQL_NO_DATA SQLRETURN = 100

	// SQL_ERROR indicates a non-recoverable failure. Call SQLGetDiagRec with
	// the appropriate handle to retrieve the SQLSTATE and message text.
	SQL_ERROR SQLRETURN = -1

	// SQL_INVALID_HANDLE indicates the handle passed to the function is not
	// a valid allocated handle. This is a programming error; the handle was
	// never allocated, has already been freed, or is the wrong type.
	SQL_INVALID_HANDLE SQLRETURN = -2

	// SQL_STILL_EXECUTING is returned when a function called in asynchronous
	// mode has not yet completed. Poll again or call SQLCancel.
	SQL_STILL_EXECUTING SQLRETURN = 2

	// SQL_NEED_DATA indicates that the application must supply parameter data
	// at execution time via SQLPutData. Used with SQL_DATA_AT_EXEC.
	SQL_NEED_DATA SQLRETURN = 99
)

// Null handle sentinels used as the inputHandle argument to SQLAllocHandle
// when allocating the first (environment) handle, and in diagnostic calls
// where no handle is available.
const (
	SQL_NULL_HANDLE SQLHANDLE = 0
	SQL_NULL_HENV   SQLHENV   = 0
	SQL_NULL_HDBC   SQLHDBC   = 0
	SQL_NULL_HSTMT  SQLHSTMT  = 0
	SQL_NULL_HDESC  SQLHDESC  = 0
)

// Special length and indicator values used in SQLBindCol, SQLBindParameter,
// and SQLGetData indicator arguments.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=reference-length-indicator-values
const (
	// SQL_NTS means the corresponding data value is a null-terminated C string;
	// its length is determined by strlen(). Pass as the StrLen_or_Ind argument
	// when the string buffer ends with a zero byte.
	SQL_NTS = -3

	// SQL_NULL_DATA in an indicator variable signals that the column or
	// parameter value is SQL NULL (not an empty string or zero).
	SQL_NULL_DATA = -1

	// SQL_DATA_AT_EXEC in an indicator variable signals that the parameter
	// data will be supplied at execution time via SQLPutData.
	SQL_DATA_AT_EXEC = -2

	// SQL_NO_TOTAL is returned in an indicator variable by SQLGetData when the
	// total length of a large-object column is not known in advance.
	SQL_NO_TOTAL = -4

	// SQL_DEFAULT_PARAM signals that the default value defined in the procedure
	// should be used for this parameter.
	SQL_DEFAULT_PARAM = -5

	// SQL_IGNORE signals that the parameter should be ignored (used with
	// bulk operations).
	SQL_IGNORE = -6
)

// Environment attribute constants for SQLSetEnvAttr / SQLGetEnvAttr.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlsetenvattr
const (
	// SQL_ATTR_ODBC_VERSION sets the ODBC version the application conforms to.
	// Must be set on the environment handle before allocating connection handles.
	// Use SQL_OV_ODBC3 for ODBC 3.x behaviour (recommended).
	SQL_ATTR_ODBC_VERSION = 200

	// SQL_ATTR_CONNECTION_POOLING controls whether connection pooling is enabled
	// at the environment level. Disabled by default; not used in this driver.
	SQL_ATTR_CONNECTION_POOLING = 201

	// SQL_ATTR_CP_MATCH controls how pooled connections are matched to requests.
	SQL_ATTR_CP_MATCH = 202

	// SQL_ATTR_OUTPUT_NTS controls whether character data returned by CLI
	// functions is null-terminated. Default is SQL_TRUE.
	SQL_ATTR_OUTPUT_NTS = 10001

	// SQL_OV_ODBC3 is the value for SQL_ATTR_ODBC_VERSION that enables
	// ODBC 3.x semantics (recommended for all new applications).
	SQL_OV_ODBC3 = 3

	// SQL_OV_ODBC3_80 is the value for SQL_ATTR_ODBC_VERSION that enables
	// ODBC 3.80 semantics (Windows 8 / SQL Server 2012 additions).
	SQL_OV_ODBC3_80 = 380
)

// Connection attribute constants for SQLSetConnectAttr / SQLGetConnectAttr.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlsetconnectattr
const (
	// SQL_ATTR_ACCESS_MODE controls whether the connection is read-only.
	SQL_ATTR_ACCESS_MODE = 101

	// SQL_ATTR_AUTOCOMMIT controls whether each SQL statement is automatically
	// committed. Set to SQL_AUTOCOMMIT_OFF to begin an explicit transaction;
	// restore to SQL_AUTOCOMMIT_ON after SQLEndTran.
	SQL_ATTR_AUTOCOMMIT = 102

	// SQL_ATTR_CONNECTION_TIMEOUT sets the number of seconds to wait for any
	// request on the connection before returning SQL_ERROR.
	SQL_ATTR_CONNECTION_TIMEOUT = 113

	// SQL_ATTR_CURRENT_CATALOG sets the default schema (catalog) for the connection.
	SQL_ATTR_CURRENT_CATALOG = 109

	// SQL_ATTR_LOGIN_TIMEOUT sets the number of seconds to wait for a login
	// (SQLDriverConnect / SQLConnect) to complete.
	SQL_ATTR_LOGIN_TIMEOUT = 103

	// SQL_ATTR_TXN_ISOLATION sets the transaction isolation level. Use the
	// SQL_TXN_* constants as the value.
	SQL_ATTR_TXN_ISOLATION = 108

	// SQL_AUTOCOMMIT_OFF disables autocommit; the application must call
	// SQLEndTran(SQL_COMMIT) or SQLEndTran(SQL_ROLLBACK) explicitly.
	SQL_AUTOCOMMIT_OFF = 0

	// SQL_AUTOCOMMIT_ON enables autocommit (default). Each SQL statement is
	// committed automatically upon successful completion.
	SQL_AUTOCOMMIT_ON = 1
)

// Statement attribute constants for SQLSetStmtAttr / SQLGetStmtAttr.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlsetstmtattr
const (
	SQL_ATTR_APP_ROW_DESC       = 10010
	SQL_ATTR_APP_PARAM_DESC     = 10011
	SQL_ATTR_IMP_ROW_DESC       = 10012
	SQL_ATTR_IMP_PARAM_DESC     = 10013
	SQL_ATTR_CURSOR_SCROLLABLE  = -1
	SQL_ATTR_CURSOR_SENSITIVITY = -2

	// SQL_ATTR_QUERY_TIMEOUT sets the maximum number of seconds a statement
	// may execute before it is cancelled and SQL_ERROR is returned. Zero
	// disables the timeout (default).
	SQL_ATTR_QUERY_TIMEOUT = 0

	// SQL_ATTR_MAX_ROWS limits the number of rows returned by a SELECT. Zero
	// means no limit (default).
	SQL_ATTR_MAX_ROWS      = 1
	SQL_ATTR_NOSCAN        = 2
	SQL_ATTR_MAX_LENGTH    = 3
	SQL_ATTR_ASYNC_ENABLE  = 4
	SQL_ATTR_ROW_BIND_TYPE = 5

	// SQL_ATTR_CURSOR_TYPE controls the cursor model. Use SQL_CURSOR_FORWARD_ONLY
	// (default, most efficient) for read-once result sets.
	SQL_ATTR_CURSOR_TYPE      = 6
	SQL_ATTR_CONCURRENCY      = 7
	SQL_ATTR_ROW_ARRAY_SIZE   = 27
	SQL_ATTR_ROW_STATUS_PTR   = 25
	SQL_ATTR_ROWS_FETCHED_PTR = 26
)

// Transaction completion type constants passed to SQLEndTran.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlendtran
const (
	// SQL_COMMIT durably commits all changes made in the current transaction.
	SQL_COMMIT = 0

	// SQL_ROLLBACK undoes all changes made in the current transaction.
	SQL_ROLLBACK = 1
)

// Transaction isolation level constants used with SQL_ATTR_TXN_ISOLATION.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=concepts-isolation-levels
//
// DB2 uses different names than ODBC: Read Uncommitted = UR, Read Committed = CS
// (Cursor Stability), Repeatable Read = RS, Serializable = RR (Repeatable Read).
// Note: DB2's "Repeatable Read" (RR, serializable) is stricter than SQL standard
// REPEATABLE READ — it prevents phantom rows. Use SQL_TXN_SERIALIZABLE for DB2 RR.
const (
	// SQL_TXN_READ_UNCOMMITTED maps to DB2 "Uncommitted Read" (UR). Dirty reads
	// are allowed; used only for analytics where consistency is not required.
	SQL_TXN_READ_UNCOMMITTED = 1

	// SQL_TXN_READ_COMMITTED maps to DB2 "Cursor Stability" (CS). A shared lock
	// is held only while the cursor is positioned on a row. Default isolation level.
	SQL_TXN_READ_COMMITTED = 2

	// SQL_TXN_REPEATABLE_READ maps to DB2 "Read Stability" (RS). Rows read in
	// the transaction cannot be modified by others, but phantom rows are possible.
	SQL_TXN_REPEATABLE_READ = 4

	// SQL_TXN_SERIALIZABLE maps to DB2 "Repeatable Read" (RR). No dirty reads,
	// non-repeatable reads, or phantom rows. This is the strongest isolation level
	// and is used for snapshot transactions to ensure consistent table reads.
	SQL_TXN_SERIALIZABLE = 8
)

// C data type constants used in the targetType argument of SQLBindCol and
// SQLGetData to specify how DB2 should convert the column value into the
// application buffer.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=data-c-types
const (
	// SQL_C_CHAR binds the column as a null-terminated char string. All DB2
	// column types can be retrieved as SQL_C_CHAR (with appropriate conversion).
	// This is the only target type used by the db2 driver in this package.
	SQL_C_CHAR = 1

	SQL_C_LONG      = 4
	SQL_C_SHORT     = 5
	SQL_C_FLOAT     = 7
	SQL_C_DOUBLE    = 8
	SQL_C_NUMERIC   = 2
	SQL_C_DEFAULT   = 99
	SQL_C_DATE      = 9
	SQL_C_TIME      = 10
	SQL_C_TIMESTAMP = 11

	// SQL_C_BINARY retrieves the column as raw bytes. Used for CHAR FOR BIT DATA
	// columns (such as IBMSNAP_COMMITSEQ) when exact byte representation matters.
	SQL_C_BINARY   = -2
	SQL_C_BIT      = -7
	SQL_C_SBIGINT  = -25
	SQL_C_UBIGINT  = -27
	SQL_C_TINYINT  = -6
	SQL_C_SLONG    = -16
	SQL_C_SSHORT   = -15
	SQL_C_STINYINT = -26
	SQL_C_ULONG    = -18
	SQL_C_USHORT   = -17
	SQL_C_UTINYINT = -28
	SQL_C_WCHAR    = -8
)

// SQL data type constants that identify the native DB2 column type in
// SQLDescribeCol, SQLBindParameter, and column-type metadata queries.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=data-sql-types
const (
	SQL_CHAR        = 1
	SQL_NUMERIC     = 2
	SQL_DECIMAL     = 3
	SQL_INTEGER     = 4
	SQL_SMALLINT    = 5
	SQL_FLOAT       = 6
	SQL_REAL        = 7
	SQL_DOUBLE      = 8
	SQL_VARCHAR     = 12
	SQL_DATE        = 9
	SQL_TIME        = 10
	SQL_TIMESTAMP   = 11
	SQL_LONGVARCHAR = -1

	// SQL_BINARY and SQL_VARBINARY map to CHAR FOR BIT DATA / VARCHAR FOR BIT DATA.
	// IBMSNAP_COMMITSEQ is typed CHAR(10) FOR BIT DATA; its SQL type is SQL_BINARY.
	SQL_BINARY         = -2
	SQL_VARBINARY      = -3
	SQL_LONGVARBINARY  = -4
	SQL_BIGINT         = -5
	SQL_TINYINT        = -6
	SQL_BIT            = -7
	SQL_WCHAR          = -8
	SQL_WVARCHAR       = -9
	SQL_WLONGVARCHAR   = -10
	SQL_GUID           = -11
	SQL_TYPE_DATE      = 91
	SQL_TYPE_TIME      = 92
	SQL_TYPE_TIMESTAMP = 93

	// SQL_GRAPHIC, SQL_VARGRAPHIC, SQL_LONGVARGRAPHIC are IBM DB2 extensions for
	// double-byte character set (DBCS) columns (GRAPHIC, VARGRAPHIC, DBCLOB variants).
	// These require SQL_C_WCHAR retrieval to avoid UTF-16LE/UTF-8 misinterpretation.
	SQL_GRAPHIC        = -95
	SQL_VARGRAPHIC     = -96
	SQL_LONGVARGRAPHIC = -97

	// DB2-specific extended SQL types not present in the ODBC specification.
	SQL_BLOB     = -98  // DB2 BLOB (Binary Large Object)
	SQL_CLOB     = -99  // DB2 CLOB (Character Large Object)
	SQL_DBCLOB   = -350 // DB2 DBCLOB (Double-Byte CLOB)
	SQL_XML      = -370 // DB2 XML (stored as hierarchical data)
	SQL_DECFLOAT = -360 // DB2 DECFLOAT (IEEE 754 decimal floating-point)
	SQL_BOOLEAN  = 16   // DB2 BOOLEAN (DB2 11.1+)
)

// Nullable information constants returned by SQLDescribeCol in the nullable
// output argument.
const (
	// SQL_NO_NULLS means the column is defined NOT NULL.
	SQL_NO_NULLS = 0

	// SQL_NULLABLE means the column may contain SQL NULL values.
	SQL_NULLABLE = 1

	// SQL_NULLABLE_UNKNOWN means nullability cannot be determined (e.g. for
	// expressions in the select list).
	SQL_NULLABLE_UNKNOWN = 2
)

// Fetch orientation constants for SQLFetchScroll's fetchOrientation argument.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlfetchscroll
const (
	SQL_FETCH_NEXT     = 1 // Advance one row forward (same as SQLFetch)
	SQL_FETCH_FIRST    = 2 // Move to the first row of the result set
	SQL_FETCH_LAST     = 3 // Move to the last row
	SQL_FETCH_PRIOR    = 4 // Move one row backward
	SQL_FETCH_ABSOLUTE = 5 // Move to the row at the absolute row number in fetchOffset
	SQL_FETCH_RELATIVE = 6 // Move fetchOffset rows relative to current position
)

// SQLGetInfo information type constants. Pass as the infoType argument to
// retrieve driver and data-source properties.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlgetinfo
const (
	SQL_DBMS_NAME            = 17 // Name of the DBMS product (e.g. "DB2/LINUXX8664")
	SQL_DBMS_VER             = 18 // DBMS version string (e.g. "11.05.0007")
	SQL_DATABASE_NAME        = 16 // Name of the currently connected database
	SQL_DRIVER_NAME          = 6  // Name of the CLI driver file
	SQL_DRIVER_VER           = 7  // Version of the CLI driver
	SQL_SERVER_NAME          = 13 // Name of the DB2 server
	SQL_MAX_CATALOG_NAME_LEN = 34
	SQL_MAX_COLUMN_NAME_LEN  = 30
	SQL_MAX_CURSOR_NAME_LEN  = 31
	SQL_MAX_SCHEMA_NAME_LEN  = 32
	SQL_MAX_TABLE_NAME_LEN   = 35
	SQL_TXN_CAPABLE          = 46
	SQL_GETDATA_EXTENSIONS   = 81
)

// Diagnostic field constants for SQLGetDiagField. These identify which field
// of a diagnostic record to retrieve.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlgetdiagfield
const (
	SQL_DIAG_RETURNCODE       = 1  // Return code of the function that created the record
	SQL_DIAG_NUMBER           = 2  // Number of diagnostic records available
	SQL_DIAG_ROW_COUNT        = 3  // Number of rows affected by INSERT/UPDATE/DELETE
	SQL_DIAG_SQLSTATE         = 4  // Five-character SQLSTATE string
	SQL_DIAG_NATIVE           = 5  // Driver-specific native error code
	SQL_DIAG_MESSAGE_TEXT     = 6  // Human-readable error message
	SQL_DIAG_DYNAMIC_FUNCTION = 7  // SQL statement type that produced the record
	SQL_DIAG_CLASS_ORIGIN     = 8  // Origin of the SQLSTATE class
	SQL_DIAG_SUBCLASS_ORIGIN  = 9  // Origin of the SQLSTATE subclass
	SQL_DIAG_CONNECTION_NAME  = 10 // Name of the connection
	SQL_DIAG_SERVER_NAME      = 11 // Name of the server
)

// Column descriptor field constants for SQLColAttribute and descriptor handles.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlcolattribute
const (
	SQL_DESC_COUNT                  = 1001
	SQL_DESC_TYPE                   = 1002
	SQL_DESC_LENGTH                 = 1003
	SQL_DESC_OCTET_LENGTH_PTR       = 1004
	SQL_DESC_PRECISION              = 1005
	SQL_DESC_SCALE                  = 1006
	SQL_DESC_DATETIME_INTERVAL_CODE = 1007
	SQL_DESC_NULLABLE               = 1008
	SQL_DESC_INDICATOR_PTR          = 1009
	SQL_DESC_DATA_PTR               = 1010
	SQL_DESC_NAME                   = 1011
	SQL_DESC_UNNAMED                = 1012
	SQL_DESC_OCTET_LENGTH           = 1013
	SQL_DESC_ALLOC_TYPE             = 1099
)

// Cursor concurrency constants for SQL_ATTR_CONCURRENCY. Controls whether the
// cursor can modify rows in the result set.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlsetstmtattr
const (
	// SQL_CONCUR_READ_ONLY makes the cursor read-only; no positioned updates or
	// deletes are allowed. This is the most efficient concurrency mode and is
	// required for the CDC streaming queries in this package.
	SQL_CONCUR_READ_ONLY = 1
	SQL_CONCUR_LOCK      = 2 // Lowest sufficient locking for updateability
	SQL_CONCUR_ROWVER    = 3 // Optimistic concurrency using row version
	SQL_CONCUR_VALUES    = 4 // Optimistic concurrency using row values
)

// Cursor type constants for SQL_ATTR_CURSOR_TYPE.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlsetstmtattr
const (
	// SQL_CURSOR_FORWARD_ONLY is the default and most efficient cursor type.
	// Rows are fetched in order; only SQLFetch (not SQLFetchScroll) is supported.
	// All CDC and snapshot queries use forward-only cursors.
	SQL_CURSOR_FORWARD_ONLY = 0

	// SQL_CURSOR_KEYSET_DRIVEN is a scrollable cursor that uses a keyset to
	// identify rows. Supports SQLFetchScroll; more expensive than forward-only.
	SQL_CURSOR_KEYSET_DRIVEN = 1

	// SQL_CURSOR_DYNAMIC is a fully scrollable cursor; membership and order can
	// change between fetches. Most expensive; not used in this package.
	SQL_CURSOR_DYNAMIC = 2

	// SQL_CURSOR_STATIC is a scrollable cursor with a fixed result set snapshot.
	SQL_CURSOR_STATIC = 3
)

// Free statement option constants for SQLFreeStmt's option argument.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlfreestmt
const (
	// SQL_CLOSE closes the cursor associated with the statement handle and
	// discards any pending results. The statement handle remains allocated.
	SQL_CLOSE = 0

	// SQL_DROP closes the cursor and frees the statement handle. Equivalent to
	// calling SQLFreeHandle(SQL_HANDLE_STMT, hstmt).
	SQL_DROP = 1

	// SQL_UNBIND releases all column bindings set by SQLBindCol.
	SQL_UNBIND = 2

	// SQL_RESET_PARAMS releases all parameter bindings set by SQLBindParameter.
	SQL_RESET_PARAMS = 3
)

// Parameter input/output type constants for the inputOutputType argument of
// SQLBindParameter.
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=functions-sqlbindparameter
const (
	// SQL_PARAM_INPUT marks the parameter as an input-only parameter (value
	// flows from application to DB2). This is the only type used by this driver
	// since all CDC queries use read-only prepared statements.
	SQL_PARAM_INPUT = 1

	// SQL_PARAM_INPUT_OUTPUT marks the parameter as both input and output.
	// Used with stored procedures.
	SQL_PARAM_INPUT_OUTPUT = 2

	// SQL_PARAM_OUTPUT marks the parameter as output-only (value flows from DB2
	// to the application). Used with stored procedure OUT parameters.
	SQL_PARAM_OUTPUT = 4
)
