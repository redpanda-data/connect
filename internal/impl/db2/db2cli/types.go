// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2cli

import "unsafe"

// DB2 CLI type aliases — Go equivalents of the C types used in the IBM DB2
// CLI (Call Level Interface) API. All handle types are opaque pointer-sized
// values allocated by SQLAllocHandle and freed by SQLFreeHandle.
//
// IBM DB2 CLI type reference:
// https://www.ibm.com/docs/en/db2/11.5?topic=reference-cli-data-types
//
// ODBC equivalent reference (DB2 CLI mirrors ODBC):
// https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types

// SQLHANDLE is the generic opaque handle type, equivalent to C SQLHANDLE
// (void*). All specific handle types (SQLHENV, SQLHDBC, SQLHSTMT, SQLHDESC)
// are named subtypes of SQLHANDLE. The zero value represents SQL_NULL_HANDLE.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=reference-handle-types
type SQLHANDLE uintptr

// SQLHENV is an environment handle allocated by
// SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv). It holds global
// state including the ODBC version (SQL_ATTR_ODBC_VERSION) and must be
// allocated before any connection handles. One environment handle per process
// is the typical pattern. Equivalent to C SQLHENV.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=handles-environment
type SQLHENV SQLHANDLE

// SQLHDBC is a connection handle allocated by
// SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc). It represents a single
// physical TCP/IP connection to a DB2 database. Transaction state
// (SQL_ATTR_AUTOCOMMIT, SQL_ATTR_TXN_ISOLATION) is per-connection. Equivalent
// to C SQLHDBC.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=handles-connection
type SQLHDBC SQLHANDLE

// SQLHSTMT is a statement handle allocated by
// SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt). It represents one prepared
// or executed SQL statement and owns the associated result-set cursor. Multiple
// statement handles may share one connection handle; each has its own cursor
// state. Equivalent to C SQLHSTMT.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=handles-statement
type SQLHSTMT SQLHANDLE

// SQLHDESC is a descriptor handle that describes the attributes (type, length,
// precision, nullability) of a parameter or column. Descriptor handles are
// implicitly allocated when a statement handle is created; they can also be
// explicitly allocated for advanced use cases. Equivalent to C SQLHDESC.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=handles-descriptor
type SQLHDESC SQLHANDLE

// SQLRETURN is the return code type for all DB2 CLI functions. Every function
// returns one of: SQL_SUCCESS, SQL_SUCCESS_WITH_INFO, SQL_NO_DATA,
// SQL_ERROR, SQL_INVALID_HANDLE, SQL_STILL_EXECUTING, or SQL_NEED_DATA.
// Equivalent to C SQLRETURN (signed 16-bit integer).
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=cli-return-codes
type SQLRETURN int16

// SQLSMALLINT is a signed 16-bit integer used for handle type selectors,
// column counts, and small integer parameters. Equivalent to C SQLSMALLINT.
type SQLSMALLINT int16

// SQLUSMALLINT is an unsigned 16-bit integer used for column numbers (1-based)
// and unsigned small integer parameters. Equivalent to C SQLUSMALLINT.
type SQLUSMALLINT uint16

// SQLINTEGER is a signed 32-bit integer used for attribute values, string
// lengths, and large integer parameters. Equivalent to C SQLINTEGER.
type SQLINTEGER int32

// SQLUINTEGER is an unsigned 32-bit integer. Equivalent to C SQLUINTEGER.
type SQLUINTEGER uint32

// SQLLEN is a signed 64-bit integer on 64-bit platforms (32-bit on 32-bit
// platforms). Used for buffer lengths, row counts, and indicator variables.
// This package targets 64-bit only; the type is always int64. Equivalent to
// C SQLLEN.
type SQLLEN int64

// SQLULEN is an unsigned 64-bit integer on 64-bit platforms. Used for column
// sizes and unsigned length values. Equivalent to C SQLULEN.
type SQLULEN uint64

// SQLPOINTER is a generic void pointer for buffers passed to SQLGetData,
// SQLBindCol, and SQLBindParameter. Equivalent to C SQLPOINTER (void*).
// The caller is responsible for keeping the underlying allocation alive via
// runtime.KeepAlive until the CLI function returns.
type SQLPOINTER unsafe.Pointer

// SQLCHAR is a single unsigned byte, equivalent to C SQLCHAR (unsigned char).
// Strings are represented as *SQLCHAR pointing to a null-terminated byte slice.
type SQLCHAR byte

// SQLWCHAR is a two-byte wide character for Unicode (UCS-2) string parameters.
// Equivalent to C SQLWCHAR. Not used in this package; all parameters use
// SQL_C_CHAR (single-byte) to avoid the complexity of UTF-16 conversion.
type SQLWCHAR uint16

// SQLBigInt is a signed 64-bit integer. Equivalent to C SQLBIGINT.
type SQLBigInt int64

// SQLUBigInt is an unsigned 64-bit integer. Equivalent to C SQLUBIGINT.
type SQLUBigInt uint64

// SQLReal is a 32-bit IEEE 754 single-precision floating-point value.
// Equivalent to C SQLREAL (float).
type SQLReal float32

// SQLDouble is a 64-bit IEEE 754 double-precision floating-point value.
// Equivalent to C SQLDOUBLE (double).
type SQLDouble float64

// SQLDate represents a DB2 DATE value (year, month, day). The layout matches
// the C DATE_STRUCT used with SQL_C_DATE in SQLBindCol / SQLGetData.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=data-date-time-timestamp-types
type SQLDate struct {
	Year  SQLSMALLINT  // e.g. 2024
	Month SQLUSMALLINT // 1–12
	Day   SQLUSMALLINT // 1–31
}

// SQLTime represents a DB2 TIME value (hour, minute, second). The layout
// matches the C TIME_STRUCT used with SQL_C_TIME in SQLBindCol / SQLGetData.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=data-date-time-timestamp-types
type SQLTime struct {
	Hour   SQLUSMALLINT // 0–23
	Minute SQLUSMALLINT // 0–59
	Second SQLUSMALLINT // 0–59
}

// SQLTimestamp represents a DB2 TIMESTAMP value. The layout matches the C
// TIMESTAMP_STRUCT used with SQL_C_TIMESTAMP in SQLBindCol / SQLGetData.
// Fraction holds the sub-second part in nanoseconds (0–999,999,999), even
// though DB2 TIMESTAMP has only microsecond precision (0–999,999 µs).
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=data-date-time-timestamp-types
type SQLTimestamp struct {
	Year     SQLSMALLINT
	Month    SQLUSMALLINT
	Day      SQLUSMALLINT
	Hour     SQLUSMALLINT
	Minute   SQLUSMALLINT
	Second   SQLUSMALLINT
	Fraction SQLUINTEGER // sub-second fraction in nanoseconds
}

// SQLNumeric represents a DB2 NUMERIC / DECIMAL value in the binary SQL_C_NUMERIC
// format used by SQLBindCol. Precision is the total number of significant digits;
// Scale is the number of digits to the right of the decimal point; Sign is 1 for
// positive and 0 for negative; Val holds the absolute value in little-endian binary.
//
// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=data-numeric-decimal-types
type SQLNumeric struct {
	Precision SQLCHAR     // total significant digits
	Scale     SQLCHAR     // digits after the decimal point
	Sign      SQLCHAR     // 1 = positive, 0 = negative
	Val       [16]SQLCHAR // absolute value, little-endian binary
}
