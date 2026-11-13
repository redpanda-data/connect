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

// isSocketTimeout reports whether err comes from a network read or write that
// timed out, meaning the database has stopped responding on that connection.
func isSocketTimeout(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Timeout()
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
