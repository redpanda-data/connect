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
		expectedError      bool
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
			name:               "Error - Only Rack provided, no DC",
			localDC:            "",
			localRack:          "rack2",
			expectedPolicyType: nil,
			expectedError:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy, err := newHostSelectionPolicy(tc.localDC, tc.localRack)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				require.NotNil(t, policy, "Expected a policy but got nil")
				assert.IsType(t, tc.expectedPolicyType, policy, "Returned policy has an unexpected type")
			}
		})
	}
}
