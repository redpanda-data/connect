// Copyright 2024 Redpanda Data, Inc.
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

package schema

import (
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
	"github.com/redpanda-data/connect/v4/internal/plugins"
)

func redpandaTopLevelConfigField() *service.ConfigField {
	return service.NewObjectField("redpanda", enterprise.TopicLoggerFields()...)
}

// Standard returns the config schema of a standard build of Redpanda Connect.
func Standard(version, dateBuilt string) *service.ConfigSchema {
	env := service.NewEnvironment()

	s := env.FullConfigSchema(version, dateBuilt)
	s.SetFieldDefault(map[string]any{
		"@service": "redpanda-connect",
	}, "logger", "static_fields")
	s = s.Field(redpandaTopLevelConfigField())
	return s
}

// Cloud returns the config schema of a cloud build of Redpanda Connect.
func Cloud(version, dateBuilt string) *service.ConfigSchema {
	// Observability and scanner plugins aren't necessarily present in our
	// internal lists and so we allow everything that's imported
	env := service.GlobalEnvironment().
		WithBuffers(plugins.PluginNamesForCloud(plugins.TypeBuffer)...).
		WithCaches(plugins.PluginNamesForCloud(plugins.TypeCache)...).
		WithInputs(plugins.PluginNamesForCloud(plugins.TypeInput)...).
		WithMetrics(plugins.PluginNamesForCloud(plugins.TypeMetric)...).
		WithOutputs(plugins.PluginNamesForCloud(plugins.TypeOutput)...).
		WithProcessors(plugins.PluginNamesForCloud(plugins.TypeProcessor)...).
		WithRateLimits(plugins.PluginNamesForCloud(plugins.TypeRateLimit)...).
		WithScanners(plugins.PluginNamesForCloud(plugins.TypeScanner)...).
		WithTracers(plugins.PluginNamesForCloud(plugins.TypeTracer)...)

	// Allow only pure methods and functions within Bloblang.
	benv := bloblang.GlobalEnvironment()
	env.UseBloblangEnvironment(benv.OnlyPure())

	s := env.FullConfigSchema(version, dateBuilt)
	s.SetFieldDefault(map[string]any{}, "input")
	s.SetFieldDefault(map[string]any{}, "output")
	s.SetFieldDefault(map[string]any{
		"@service": "redpanda-connect",
	}, "logger", "static_fields")
	s = s.Field(redpandaTopLevelConfigField())
	return s
}

// CloudAI returns the config schema of a cloud AI build of Redpanda Connect.
func CloudAI(version, dateBuilt string) *service.ConfigSchema {
	// Observability and scanner plugins aren't necessarily present in our
	// internal lists and so we allow everything that's imported
	env := service.GlobalEnvironment().
		WithBuffers(plugins.PluginNamesForCloudAI(plugins.TypeBuffer)...).
		WithCaches(plugins.PluginNamesForCloudAI(plugins.TypeCache)...).
		WithInputs(plugins.PluginNamesForCloudAI(plugins.TypeInput)...).
		WithMetrics(plugins.PluginNamesForCloudAI(plugins.TypeMetric)...).
		WithOutputs(plugins.PluginNamesForCloudAI(plugins.TypeOutput)...).
		WithProcessors(plugins.PluginNamesForCloudAI(plugins.TypeProcessor)...).
		WithRateLimits(plugins.PluginNamesForCloudAI(plugins.TypeRateLimit)...).
		WithScanners(plugins.PluginNamesForCloudAI(plugins.TypeScanner)...).
		WithTracers(plugins.PluginNamesForCloudAI(plugins.TypeTracer)...)

	// Allow only pure methods and functions within Bloblang.
	benv := bloblang.GlobalEnvironment()
	env.UseBloblangEnvironment(benv.OnlyPure())

	s := env.FullConfigSchema(version, dateBuilt)
	s.SetFieldDefault(map[string]any{}, "input")
	s.SetFieldDefault(map[string]any{}, "output")
	s.SetFieldDefault(map[string]any{
		"@service": "redpanda-connect",
	}, "logger", "static_fields")
	s = s.Field(redpandaTopLevelConfigField())
	return s
}
