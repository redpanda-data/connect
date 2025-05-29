// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cloudaitest_test

import (
	"testing"

	"github.com/redpanda-data/connect/v4/internal/plugins"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "embed"

	_ "github.com/redpanda-data/connect/v4/public/components/cloud"
	_ "github.com/redpanda-data/connect/v4/public/components/ollama"
)

func TestImportsMatch(t *testing.T) {
	allowSlice := plugins.PluginNamesForCloudAI(plugins.TypeNone)

	env := service.GlobalEnvironment()

	seen := map[string]struct{}{}

	env.WalkBuffers(func(name string, _ *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkCaches(func(name string, _ *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkInputs(func(name string, _ *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkMetrics(func(name string, _ *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkOutputs(func(name string, _ *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkProcessors(func(name string, _ *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkRateLimits(func(name string, _ *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkScanners(func(name string, _ *service.ConfigView) {
		seen[name] = struct{}{}
	})

	env.WalkTracers(func(name string, _ *service.ConfigView) {
		seen[name] = struct{}{}
	})

	for _, k := range allowSlice {
		if _, exists := seen[k]; !exists {
			t.Errorf("plugin '%v' referenced within internal/plugins/info.csv is not imported by this product", k)
		}
	}
}
