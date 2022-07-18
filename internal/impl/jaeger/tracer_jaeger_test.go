package jaeger

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
