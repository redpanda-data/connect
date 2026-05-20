// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2cli

import (
	"errors"
	"fmt"
	"unsafe"
)

// DB2Error represents a single DB2 CLI diagnostic record retrieved by
// SQLGetDiagRec. It implements the error interface and carries the five-character
// SQLSTATE code, a DB2-specific native error code, and the human-readable message.
//
// SQLSTATE is a standard five-character code defined by the SQL standard and
// extended by IBM. The first two characters identify the error class; the last
// three identify the sub-condition. IBM SQLSTATE reference:
// https://www.ibm.com/docs/en/db2/11.5?topic=messages-sqlstate
type DB2Error struct {
	// SQLState is the five-character SQLSTATE code (e.g. "42710", "08006").
	SQLState string
	// NativeError is the DB2-specific error code (SQLCODE). Negative values
	// are errors; positive values are warnings.
	NativeError int32
	// Message is the human-readable error message text from the DB2 message catalog.
	Message string
	// HandleType and Handle identify the handle on which the error occurred.
	// Useful for routing the error to additional diagnostic queries.
	HandleType SQLSMALLINT
	Handle     SQLHANDLE
}

// Error implements the error interface, returning a formatted string containing
// the SQLSTATE, native error code, and message text.
func (e *DB2Error) Error() string {
	return fmt.Sprintf("DB2 Error [%s] (Native: %d): %s", e.SQLState, e.NativeError, e.Message)
}

// IsConnectionError reports whether this error is a connection exception.
// SQLSTATE class 08 covers all connection-related failures (e.g. 08001 = client
// unable to establish connection, 08006 = connection failure during transaction).
func (e *DB2Error) IsConnectionError() bool {
	// SQLSTATE class 08 = Connection Exception
	return len(e.SQLState) >= 2 && e.SQLState[:2] == "08"
}

// IsIntegrityConstraintViolation reports whether this error is an integrity
// constraint violation. SQLSTATE class 23 covers unique-key violations (23505),
// foreign-key violations (23503), and other constraint errors.
func (e *DB2Error) IsIntegrityConstraintViolation() bool {
	// SQLSTATE class 23 = Integrity Constraint Violation
	return len(e.SQLState) >= 2 && e.SQLState[:2] == "23"
}

// IsDeadlock reports whether this error is a deadlock or serialization failure.
// SQLSTATE 40001 is the standard code for deadlock detection; DB2 raises it when
// the lock manager detects a cycle and rolls back one of the involved transactions.
func (e *DB2Error) IsDeadlock() bool {
	// SQLSTATE 40001 = Serialization failure (deadlock)
	return e.SQLState == "40001"
}

// IsTimeout reports whether this error is a query or connection timeout.
// SQLSTATE HYT00 = query timeout (SQL_ATTR_QUERY_TIMEOUT elapsed).
// SQLSTATE HYT01 = connection timeout (SQL_ATTR_CONNECTION_TIMEOUT elapsed).
func (e *DB2Error) IsTimeout() bool {
	return e.SQLState == "HYT00" || e.SQLState == "HYT01"
}

// GetDiagnostics collects all diagnostic records associated with handle after a
// CLI function failure. It calls SQLGetDiagRec in a loop (up to 64 records) and
// returns the accumulated records as a slice of DB2Error. The caller typically
// calls GetLastError when only the first (most specific) error is needed.
func GetDiagnostics(handleType SQLSMALLINT, handle SQLHANDLE) []DB2Error {
	var errors []DB2Error

	for recNum := SQLSMALLINT(1); recNum <= 64; recNum++ {
		sqlState := make([]byte, 6)
		messageText := make([]byte, 1024)
		var nativeError SQLINTEGER
		var textLen SQLSMALLINT

		ret := SQLGetDiagRec(
			handleType,
			handle,
			recNum,
			(*SQLCHAR)(unsafe.Pointer(&sqlState[0])),
			&nativeError,
			(*SQLCHAR)(unsafe.Pointer(&messageText[0])),
			1024,
			&textLen,
		)

		if ret == SQL_NO_DATA {
			break
		}

		if ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO {
			break
		}

		sqlStateStr := string(sqlState[:5]) // SQLSTATE is always 5 characters
		// Clamp textLen: per ODBC spec textLen may be SQL_NO_TOTAL (-4) or any
		// negative value when the driver cannot determine the true length.
		// max(0, …) guards against a negative slice index panic.
		n := max(0, min(int(textLen), len(messageText)))
		messageStr := string(messageText[:n])

		errors = append(errors, DB2Error{
			SQLState:    sqlStateStr,
			NativeError: int32(nativeError),
			Message:     messageStr,
			HandleType:  handleType,
			Handle:      handle,
		})
	}

	return errors
}

// GetLastError retrieves the first (primary) diagnostic record for handle and
// returns it as a Go error. When no diagnostic records are available (e.g. if
// the library is not loaded), a generic message is returned instead of nil to
// prevent silent failures. Callers use this after any CLI function returns
// SQL_ERROR to obtain a descriptive error value.
func GetLastError(handleType SQLSMALLINT, handle SQLHANDLE) error {
	diags := GetDiagnostics(handleType, handle)
	if len(diags) == 0 {
		return errors.New("unknown DB2 error (no diagnostics available)")
	}
	return &diags[0]
}

// CheckReturn maps a SQLRETURN value to a Go error. SQL_SUCCESS and
// SQL_SUCCESS_WITH_INFO both return nil (warnings are not surfaced as errors).
// SQL_NO_DATA returns nil because callers treat it as end-of-results, not an
// error. For SQL_ERROR and SQL_INVALID_HANDLE the diagnostic records are fetched
// via GetLastError and wrapped with the operation name for context.
func CheckReturn(ret SQLRETURN, handleType SQLSMALLINT, handle SQLHANDLE, operation string) error {
	switch ret {
	case SQL_SUCCESS:
		return nil
	case SQL_SUCCESS_WITH_INFO:
		// Success with warnings - not an error, but diagnostics may be available
		return nil
	case SQL_NO_DATA:
		// No data is not an error for fetch operations
		return nil
	case SQL_INVALID_HANDLE:
		return fmt.Errorf("%s: invalid handle", operation)
	case SQL_ERROR:
		err := GetLastError(handleType, handle)
		return fmt.Errorf("%s: %w", operation, err)
	case SQL_STILL_EXECUTING:
		return fmt.Errorf("%s: operation still executing", operation)
	default:
		return fmt.Errorf("%s: unexpected return code %d", operation, ret)
	}
}

// Common SQLSTATE constants for programmatic error classification. These are
// the five-character codes returned in the sqlState buffer by SQLGetDiagRec.
// The first two characters identify the error class; the last three identify
// the specific sub-condition. IBM SQLSTATE reference:
// https://www.ibm.com/docs/en/db2/11.5?topic=messages-sqlstate
const (
	// SQLSTATE_SUCCESS (class 00) indicates successful completion with no warnings.
	SQLSTATE_SUCCESS = "00000"

	// Class 01: Warning
	SQLSTATE_WARNING = "01000"

	// Class 02: No Data
	SQLSTATE_NO_DATA = "02000"

	// Class 08: Connection Exception
	SQLSTATE_CONNECTION_EXCEPTION      = "08000"
	SQLSTATE_CONNECTION_DOES_NOT_EXIST = "08003"
	SQLSTATE_CONNECTION_FAILURE        = "08006"
	SQLSTATE_CONNECTION_NAME_IN_USE    = "08002"
	SQLSTATE_TRANSACTION_RESOLUTION    = "08007"

	// Class 21: Cardinality Violation
	SQLSTATE_CARDINALITY_VIOLATION = "21000"

	// Class 22: Data Exception
	SQLSTATE_DATA_EXCEPTION          = "22000"
	SQLSTATE_STRING_DATA_RIGHT_TRUNC = "22001"
	SQLSTATE_NUMERIC_VALUE_OUT_RANGE = "22003"
	SQLSTATE_INVALID_DATETIME_FORMAT = "22007"
	SQLSTATE_DIVISION_BY_ZERO        = "22012"

	// Class 23: Integrity Constraint Violation
	SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION = "23000"
	SQLSTATE_UNIQUE_VIOLATION               = "23505"
	SQLSTATE_FOREIGN_KEY_VIOLATION          = "23503"

	// Class 24: Invalid Cursor State
	SQLSTATE_INVALID_CURSOR_STATE = "24000"

	// Class 25: Invalid Transaction State
	SQLSTATE_INVALID_TRANSACTION_STATE = "25000"

	// Class 40: Transaction Rollback
	SQLSTATE_SERIALIZATION_FAILURE = "40001" // Deadlock
	SQLSTATE_TRANSACTION_ROLLBACK  = "40000"

	// Class 42: Syntax Error or Access Rule Violation
	SQLSTATE_SYNTAX_ERROR            = "42000"
	SQLSTATE_TABLE_OR_VIEW_NOT_FOUND = "42S02"
	SQLSTATE_COLUMN_NOT_FOUND        = "42S22"
	SQLSTATE_DUPLICATE_COLUMN        = "42S21"
	SQLSTATE_PERMISSION_DENIED       = "42501"

	// Class HY: CLI-specific errors
	SQLSTATE_GENERAL_ERROR           = "HY000"
	SQLSTATE_MEMORY_ALLOCATION_ERROR = "HY001"
	SQLSTATE_INVALID_HANDLE_TYPE     = "HY092"
	SQLSTATE_TIMEOUT_EXPIRED         = "HYT00"
	SQLSTATE_CONNECTION_TIMEOUT      = "HYT01"
)
