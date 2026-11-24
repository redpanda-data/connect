// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"database/sql"
	"errors"
	"net"

	go_ora "github.com/sijms/go-ora/v2"
)

// IsSocketTimeout reports whether err comes from a network read or write that
// timed out, meaning the database has stopped responding on that connection
// or took longer than the socket timeout to reply. Dial timeouts are excluded:
// they mean the database could not be reached, which socket_timeout does not
// govern.
func IsSocketTimeout(err error) bool {
	var opErr *net.OpError
	if !errors.As(err, &opErr) || !opErr.Timeout() {
		return false
	}
	return opErr.Op == "read" || opErr.Op == "write"
}

// abandonConn marks conn's driver connection as closed, so that statement
// closes on it return driver.ErrBadConn immediately instead of making a
// network round trip. The pool then drops the connection the next time it is
// checked out, or when the pool itself closes.
func abandonConn(conn *sql.Conn) {
	_ = conn.Raw(func(driverConn any) error {
		if c, ok := driverConn.(*go_ora.Connection); ok {
			c.State = go_ora.Closed
		}
		return nil
	})
}
