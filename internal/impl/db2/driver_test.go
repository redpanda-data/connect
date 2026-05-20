// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2

import (
	"database/sql/driver"
	"testing"
)

// Compile-time assertions: db2Conn must implement both context-aware interfaces
// so that database/sql routes QueryContext and ExecContext through our
// SQLCancel-based cancel path instead of falling back to blocking Prepare+Exec.
var (
	_ driver.QueryerContext = (*db2Conn)(nil)
	_ driver.ExecerContext  = (*db2Conn)(nil) // fails to compile until ExecContext is added
)

// TestDB2ConnImplementsContextInterfaces is a placeholder that documents the
// compile-time assertions above; the real test is whether the package builds.
func TestDB2ConnImplementsContextInterfaces(t *testing.T) {
	t.Parallel()
	// Both assertions are verified at compile time via the var block above.
	// If db2Conn is missing QueryContext or ExecContext the build fails here.
}
