// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"os"
	"testing"
)

// TestMain terminates the shared MongoDB containers (if either was started)
// after every test in the package has finished.
func TestMain(m *testing.M) {
	code := m.Run()
	if err := sharedMongoAuth.terminate(); err != nil {
		os.Stderr.WriteString("failed to terminate shared auth MongoDB container: " + err.Error() + "\n")
		if code == 0 {
			code = 1
		}
	}
	if err := sharedMongoNoAuth.terminate(); err != nil {
		os.Stderr.WriteString("failed to terminate shared no-auth MongoDB container: " + err.Error() + "\n")
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}
