// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"errors"

	goora "github.com/sijms/go-ora/v2/network"
)

const (
	// errCodeInsufficientPrivileges is ORA-01031 (insufficient privileges).
	// See https://docs.oracle.com/en/error-help/db/ora-01031/
	errCodeInsufficientPrivileges = 1031
	// errCodeMissingPrivilege is ORA-41900 (missing <privilege> privilege on
	// <object>). See https://docs.oracle.com/en/error-help/db/ora-41900/
	//
	// The Oracle docs list it for 26ai only, and do not say that it replaces
	// ORA-01031. Oracle Free 23ai, the test container, returns it for a
	// missing object privilege (INSERT, FLASHBACK).
	errCodeMissingPrivilege = 41900
)

// IsMissingPrivilege reports whether err is an Oracle error for a missing
// privilege, and returns that error. Newer versions return ORA-41900 and
// older versions return ORA-01031, so a check for one code only misses the
// other.
func IsMissingPrivilege(err error) (*goora.OracleError, bool) {
	var oraErr *goora.OracleError
	if !errors.As(err, &oraErr) {
		return nil, false
	}
	return oraErr, oraErr.ErrCode == errCodeInsufficientPrivileges || oraErr.ErrCode == errCodeMissingPrivilege
}
