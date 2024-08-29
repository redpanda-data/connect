// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package schema_test

import (
	// Only import a subset of components for execution.
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"

	_ "embed"

	_ "github.com/redpanda-data/connect/v4/public/components/cloud"
)

//go:embed cloud_allow_list.txt
var allowList string

func TestImportsMatch(t *testing.T) {
	t.Skip("Skipping until we have a structured allow list")

	var allowSlice []string
	for _, s := range strings.Split(allowList, "\n") {
		s = strings.TrimSpace(s)
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		allowSlice = append(allowSlice, s)
	}

	env := service.GlobalEnvironment()

	seen := map[string]struct{}{}

	env.WalkBuffers(func(name string, config *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkCaches(func(name string, config *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkInputs(func(name string, config *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkMetrics(func(name string, config *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkOutputs(func(name string, config *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkProcessors(func(name string, config *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkRateLimits(func(name string, config *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkScanners(func(name string, config *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkTracers(func(name string, config *service.ConfigView) {
		seen[name] = struct{}{}
	})

	for _, k := range allowSlice {
		assert.Contains(t, seen, k)
	}
}
