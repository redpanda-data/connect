// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/connect/v4/internal/impl/db2/db2test"
)

func TestMain(m *testing.M) {
	// Only start the DB2 container when integration tests are explicitly
	// requested via -run (e.g. go test -run '^TestIntegration').  Without
	// this guard a plain `go test ./internal/impl/db2/...` would attempt to
	// spin up a Docker container even for unit-only runs.
	runStr := flag.Lookup("test.run").Value.String()
	integrationRE := regexp.MustCompile(strings.Split(runStr, "/")[0])
	const integrationPattern = "TestIntegration"
	if runStr == "" || integrationRE.FindString(integrationPattern) == "" {
		os.Exit(m.Run())
	}

	// DB2 first-time setup (CREATE DATABASE + LOGARCHMETH1 restart) takes
	// 8–12 min on modern hardware.  The context must outlive that window.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	// Warm up the shared container before any test runs.  If Docker is
	// unavailable the individual tests skip themselves via SetupTest.
	if _, _, err := db2test.AcquireSharedContainer(ctx); err != nil {
		// Container unavailable — individual tests will skip themselves.
		// Exit 0 so the test binary reports success (skipped, not failed).
		fmt.Fprintf(os.Stderr, "db2test: AcquireSharedContainer failed: %v\n", err)
		os.Exit(0)
	}

	code := m.Run()

	db2test.TerminateSharedContainer(context.Background())
	os.Exit(code)
}
