// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver_test

import (
	"os"
	"testing"

	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/mssqlservertest"
)

// TestMain stops the Microsoft SQL Server container that the integration tests share, after all tests complete.
func TestMain(m *testing.M) {
	code := m.Run()
	mssqlservertest.TerminateSharedContainer()
	os.Exit(code)
}
