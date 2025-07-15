package cassandra

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHostSelectionPolicy(t *testing.T) {
	testCases := []struct {
		name               string
		localDC            string
		localRack          string
		expectedPolicyType interface{}
	}{
		{
			name:               "Rack Aware - Both DC and Rack provided",
			localDC:            "us-east-1",
			localRack:          "rack1",
			expectedPolicyType: gocql.RackAwareRoundRobinPolicy("us-east-1", "rack1"),
		},
		{
			name:               "DC Aware - Only DC provided",
			localDC:            "us-west-2",
			localRack:          "",
			expectedPolicyType: gocql.DCAwareRoundRobinPolicy("us-west-2"),
		},
		{
			name:               "Round Robin - Neither DC nor Rack provided",
			localDC:            "",
			localRack:          "",
			expectedPolicyType: gocql.RoundRobinHostPolicy(),
		},
		{
			name:               "Round Robin - Only Rack provided",
			localDC:            "",
			localRack:          "rack2",
			expectedPolicyType: gocql.RoundRobinHostPolicy(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy := newHostSelectionPolicy(tc.localDC, tc.localRack)

			require.NotNil(t, policy, "Expected a policy but got nil")
			assert.IsType(t, tc.expectedPolicyType, policy, "Returned policy has an unexpected type")
		})
	}
}
