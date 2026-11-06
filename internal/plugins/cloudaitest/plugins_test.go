// Copyright 2026 Redpanda Data, Inc.
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

	_ "github.com/redpanda-data/connect/v4/public/components/cloud"
	_ "github.com/redpanda-data/connect/v4/public/components/ollama"
)

func TestImportsMatch(t *testing.T) {
	missing := plugins.BaseInfo.Unregistered(service.GlobalEnvironment(), func(info plugins.PluginInfo) bool {
		return info.CloudWithGPU
	})
	for _, k := range missing {
		t.Errorf("plugin '%v' is marked cloud_with_gpu within internal/plugins/info.csv but is not imported by this product", k)
	}
}
