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

package exports_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/exports"
	"github.com/redpanda-data/connect/v4/internal/plugins"

	_ "github.com/redpanda-data/connect/v4/public/components/aws"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
	_ "github.com/redpanda-data/connect/v4/public/components/pure"
)

// requiredInputs is the minimum a production pipeline is given: the variables
// Redpanda Cloud injects into every pipeline pod plus the secrets created
// for the export, with the S3 credential mechanism added per test case. Everything
// else in the config has a default.
var requiredInputs = map[string]string{
	"REDPANDA_BROKERS":                        "broker-0.example.com:9092,broker-1.example.com:9092",
	"REDPANDA_SCHEMA_REGISTRY_URL":            "https://schema-registry.example.com",
	"REDPANDA_TLS_ENABLED":                    "true",
	"REDPANDA_PIPELINE_ID":                    "cv8n2k4m6p8r0t2v4x6z",
	"secrets.AUDIT_LOG_EXPORT_TOPIC":          "audit-log",
	"secrets.AUDIT_LOG_EXPORT_KAFKA_USERNAME": "u",
	"secrets.AUDIT_LOG_EXPORT_KAFKA_PASSWORD": "p",
	"secrets.AUDIT_LOG_EXPORT_S3_BUCKET":      "acme-corp-siem-ingest",
	"secrets.AUDIT_LOG_EXPORT_S3_REGION":      "eu-central-1",
}

// TestConfigBuildsWithRequiredInputsOnly lints and builds the pipeline with
// nothing but the required inputs and one credential mechanism, so every
// optional ${secrets.X:default} falls back to its default and the resulting
// config is one Connect accepts.
func TestConfigBuildsWithRequiredInputsOnly(t *testing.T) {
	assert.NotContains(t, exports.Config, "{{", "config must be static, not a template")

	for name, creds := range map[string]map[string]string{
		"static keys": {
			"secrets.AUDIT_LOG_EXPORT_AWS_ACCESS_KEY_ID":     "AKIA",
			"secrets.AUDIT_LOG_EXPORT_AWS_SECRET_ACCESS_KEY": "s",
		},
		"assumed role": {
			"secrets.AUDIT_LOG_EXPORT_AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/redpanda-audit-export",
		},
		"assumed role with external id": {
			"secrets.AUDIT_LOG_EXPORT_AWS_ROLE_ARN":         "arn:aws:iam::123456789012:role/redpanda-audit-export",
			"secrets.AUDIT_LOG_EXPORT_AWS_ROLE_EXTERNAL_ID": "d3k9q0s1f2g3h4j5k6l0",
		},
	} {
		t.Run(name, func(t *testing.T) {
			sb := service.NewStreamBuilder()
			sb.SetEnvVarLookupFunc(func(key string) (string, bool) {
				if v, ok := creds[key]; ok {
					return v, true
				}
				v, ok := requiredInputs[key]
				return v, ok
			})
			require.NoError(t, sb.SetYAML(exports.Config))

			_, err := sb.Build()
			require.NoError(t, err)
		})
	}
}

// TestConfigUsesOnlyCloudComponents checks every component the config uses is
// available in the Redpanda Cloud distribution, per internal/plugins/info.csv,
// since that is where it runs.
func TestConfigUsesOnlyCloudComponents(t *testing.T) {
	used := map[plugins.TypeName][]string{
		plugins.TypeInput:     {"redpanda"},
		plugins.TypeProcessor: {"schema_registry_decode", "mapping", "switch", "log", "archive", "compress"},
		plugins.TypeOutput:    {"reject_errored", "aws_s3"},
	}
	for typ, names := range used {
		cloud := plugins.PluginNamesForCloud(typ)
		for _, name := range names {
			// Keep the list above honest: each name must actually appear in
			// the config as a component key.
			assert.Contains(t, exports.Config, name+":", "%s %s is listed here but not used by the config", typ, name)
			assert.True(t, slices.Contains(cloud, name), "%s %s is not available in the cloud distribution", typ, name)
		}
	}
	// And the reverse: nothing in the config's component positions is missing
	// from the list above. Component keys are the only keys in the config that
	// end a line with ":" at these indentations and are not well-known
	// structural keys.
	for line := range strings.SplitSeq(exports.Config, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasSuffix(trimmed, ":") || strings.HasPrefix(trimmed, "#") {
			continue
		}
		key := strings.TrimSuffix(strings.TrimPrefix(trimmed, "- "), ":")
		if _, known := structuralKeys[key]; known {
			continue
		}
		found := false
		for _, names := range used {
			found = found || slices.Contains(names, key)
		}
		assert.True(t, found, "component %q in the config is not covered by TestConfigUsesOnlyCloudComponents", key)
	}
}

// structuralKeys are the non-component keys in the config that end a line.
var structuralKeys = map[string]struct{}{
	"input": {}, "pipeline": {}, "processors": {}, "output": {}, "tls": {}, "sasl": {},
	"basic_auth": {}, "protobuf": {}, "credentials": {}, "batching": {}, "seed_brokers": {},
	"unordered_processing": {},
}
