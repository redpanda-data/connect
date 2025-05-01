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

package jaeger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	//nolint:staticcheck
	"go.opentelemetry.io/otel/exporters/jaeger"
)

func TestGetAgentOps(t *testing.T) {
	tests := []struct {
		name         string
		agentAddress string
		want         []jaeger.AgentEndpointOption
	}{
		{
			name:         "address with port",
			agentAddress: "localhost:5775",
			want: []jaeger.AgentEndpointOption{
				jaeger.WithAgentHost("localhost"),
				jaeger.WithAgentPort("5775"),
			},
		},
		{
			name:         "address without port",
			agentAddress: "jaeger",
			want: []jaeger.AgentEndpointOption{
				jaeger.WithAgentHost("jaeger"),
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			opts, err := getAgentOpts(testCase.agentAddress)

			// We can't check for equality because they are functions, so we just check that the length is the same
			assert.Len(t, opts, len(testCase.want))
			assert.NoError(t, err)
		})
	}
}

func TestNewJaeger(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	exporterInitFn = func(_ jaeger.EndpointOption) (tracesdk.SpanExporter, error) {
		return exporter, nil
	}

	dummyVersion := "v1.0"

	tests := []struct {
		Name           string
		ServiceName    string
		ServiceVersion string
		Tags           map[string]string
	}{
		{
			Name:           "no tags",
			ServiceName:    "benthos",
			ServiceVersion: dummyVersion,
		},
		{
			Name:           "tags can overwrite service name and version",
			ServiceName:    "foobar",
			ServiceVersion: "6.6.6",
			Tags: map[string]string{
				string(semconv.ServiceNameKey):    "foobar",
				string(semconv.ServiceVersionKey): "6.6.6",
			},
		},
		{
			Name: "supports extra arbitrary tags",
			Tags: map[string]string{
				"foo": "bar",
			},
		},
	}

	for _, test := range tests {
		exporter.Reset()

		jaegerProvider, err := NewJaeger(jaegerConfig{
			engineVersion: dummyVersion,
			Tags:          test.Tags,
		})
		require.NoError(t, err, test.Name)

		// Add a span and flush it
		_, span := jaegerProvider.Tracer("testProvider").Start(t.Context(), "testSpan")
		span.AddEvent("testEvent")
		span.End()
		jaegerProvider.(*tracesdk.TracerProvider).ForceFlush(t.Context())

		snapshots := exporter.GetSpans().Snapshots()
		require.Len(t, snapshots, 1, test.Name)
		resource := snapshots[0].Resource()
		require.NotNil(t, resource, test.Name)
		attrs := resource.Attributes()

		if len(test.Tags) != 1 {
			require.Len(t, attrs, 2, test.Name)
			require.Equal(t, semconv.ServiceNameKey.String(test.ServiceName), attrs[0], test.Name)
			require.Equal(t, semconv.ServiceVersionKey.String(test.ServiceVersion), attrs[1], test.Name)
		} else {
			require.Len(t, attrs, 3, test.Name)
			require.Equal(t, attribute.Key("foo").String("bar"), attrs[0], test.Name)
		}
	}
}
