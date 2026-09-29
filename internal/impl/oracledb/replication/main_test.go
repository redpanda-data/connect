// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication_test

import (
	"log"
	"os"
	"testing"

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
)

// TestMain terminates the shared Oracle Free container (if one was started)
// after the package's tests complete.
func TestMain(m *testing.M) {
	code := m.Run()
	if err := oracledbtest.TerminateShared(); err != nil {
		log.Printf("failed to terminate shared oracledb container: %v", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}
