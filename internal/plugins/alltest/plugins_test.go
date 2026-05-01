// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package alltest_test

import (
	"fmt"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/plugins"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

// TestAllPluginsInInfoCSV ensures that every registered plugin in the "all"
// distribution has a corresponding entry in internal/plugins/info.csv. If this
// test fails, run: go run ./cmd/tools/plugins_csv_fmt
func TestAllPluginsInInfoCSV(t *testing.T) {
	env := service.GlobalEnvironment()

	check := func(name string, typeName plugins.TypeName) {
		t.Helper()
		key := fmt.Sprintf("%v-%v", name, typeName)
		if _, exists := plugins.BaseInfo[key]; !exists {
			t.Errorf("plugin %q (type %q) is registered but missing from internal/plugins/info.csv; run: go run ./cmd/tools/plugins_csv_fmt", name, typeName)
		}
	}

	env.WalkBuffers(func(name string, _ *service.ConfigView) {
		check(name, plugins.TypeBuffer)
	})
	env.WalkCaches(func(name string, _ *service.ConfigView) {
		check(name, plugins.TypeCache)
	})
	env.WalkInputs(func(name string, _ *service.ConfigView) {
		check(name, plugins.TypeInput)
	})
	env.WalkMetrics(func(name string, _ *service.ConfigView) {
		check(name, plugins.TypeMetric)
	})
	env.WalkOutputs(func(name string, _ *service.ConfigView) {
		check(name, plugins.TypeOutput)
	})
	env.WalkProcessors(func(name string, _ *service.ConfigView) {
		check(name, plugins.TypeProcessor)
	})
	env.WalkRateLimits(func(name string, _ *service.ConfigView) {
		check(name, plugins.TypeRateLimit)
	})
	env.WalkScanners(func(name string, _ *service.ConfigView) {
		check(name, plugins.TypeScanner)
	})
	env.WalkTracers(func(name string, _ *service.ConfigView) {
		check(name, plugins.TypeTracer)
	})
}

// TestPluginCloudEnablement requires each plugin to either be enabled in the
// cloud distribution or to document why it is not, via the
// cloud_unsupported_reason column in internal/plugins/info.csv.
func TestPluginCloudEnablement(t *testing.T) {
	for key, info := range plugins.BaseInfo {
		if info.Cloud {
			if info.CloudUnsupportedReason != "" {
				t.Errorf("plugin %q is cloud-enabled but has cloud_unsupported_reason %q set; clear that column", key, info.CloudUnsupportedReason)
			}
			continue
		}
		if info.CloudUnsupportedReason == "" {
			t.Errorf("plugin %q is not cloud-enabled and is missing a cloud_unsupported_reason in internal/plugins/info.csv; either enable it for cloud or document why it cannot be", key)
		}
	}
}
