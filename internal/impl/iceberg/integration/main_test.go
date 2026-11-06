// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"context"
	"log"
	"os"
	"testing"
)

// TestMain terminates the shared test infrastructure, if a test started it,
// after the package's tests complete. It must live in a _test.go file: the go
// test harness only looks for TestMain in test files.
func TestMain(m *testing.M) {
	code := m.Run()
	if sharedInfra != nil {
		if err := sharedInfra.Terminate(context.Background()); err != nil {
			log.Printf("terminating shared test infrastructure: %v", err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}
