// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain verifies that no goroutines are leaked by the tests in this
// package (CON-179 R2).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreCurrent(),
		// internal/license: InjectTestService starts an hourly expiry-metric
		// loop whose cancel func is not reachable from tests.
		goleak.IgnoreTopFunction("github.com/redpanda-data/connect/v4/internal/license.(*Service).updateExpiryMetricLoop"),
	)
}
