package cassandra

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHostSelectionPolicy(t *testing.T) {
	// testCases defines the test scenarios for newHostSelectionPolicy.
	testCases := []struct {
		name                string
		policy              primaryHostSelection
		fallback            fallbackHostSelection
		localDC             string
		localRack           string
		expectedPolicyType  interface{}
		expectError         bool
		expectedErrorSubstr string
	}{
		{
			name:               "Success: Round Robin Primary Policy",
			policy:             roundRobinPrimaryHostSelection,
			expectedPolicyType: gocql.RoundRobinHostPolicy(),
			expectError:        false,
		},
		{
			name:               "Token Aware with Round Robin Fallback",
			policy:             tokenAwarePrimaryHostSelection,
			fallback:           roundRobin,
			expectedPolicyType: gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy()),
			expectError:        false,
		},
		{
			name:               "Token Aware with DC Aware Fallback",
			policy:             tokenAwarePrimaryHostSelection,
			fallback:           dcAware,
			localDC:            "us-east-1",
			expectedPolicyType: gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy("us-east-1")),
			expectError:        false,
		},
		{
			name:               "Success: Token Aware with Rack Aware Fallback",
			policy:             tokenAwarePrimaryHostSelection,
			fallback:           rackAware,
			localDC:            "us-east-1",
			localRack:          "rack1",
			expectedPolicyType: gocql.TokenAwareHostPolicy(gocql.RackAwareRoundRobinPolicy("us-east-1", "rack1")),
			expectError:        false,
		},
		{
			name:                "Failure: Token Aware with Fallback Error (Missing DC)",
			policy:              tokenAwarePrimaryHostSelection,
			fallback:            dcAware, // dcAware requires a localDC
			localDC:             "",
			expectError:         true,
			expectedErrorSubstr: "unable to create token-aware policy with fallback: dc-aware drivers require a local DC",
		},
		{
			name:                "Failure: Unsupported Primary Policy",
			policy:              "invalid_policy",
			expectError:         true,
			expectedErrorSubstr: "unsupported host selection policy: invalid_policy",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy, err := newHostSelectionPolicy(tc.policy, tc.fallback, tc.localDC, tc.localRack)

			if tc.expectError {
				require.Error(t, err, "Expected an error but got none")
				assert.Contains(t, err.Error(), tc.expectedErrorSubstr, "Error message does not match expected substring")
				assert.Nil(t, policy, "Policy should be nil on error")
			} else {
				require.NoError(t, err, "Expected no error but got one")
				require.NotNil(t, policy, "Expected a policy but got nil")
				assert.IsType(t, tc.expectedPolicyType, policy, "Returned policy has an unexpected type")
			}
		})
	}
}

func TestHostSelectionPolicy(t *testing.T) {
	testCases := []struct {
		name               string
		policy             fallbackHostSelection
		localDC            string
		localRack          string
		expectedPolicyType interface{}
		expectError        bool
	}{
		{
			name:               "Rack Aware",
			policy:             rackAware,
			localDC:            "us-west-2",
			localRack:          "rack2",
			expectedPolicyType: gocql.RackAwareRoundRobinPolicy("us-west-2", "rack2"),
			expectError:        false,
		},
		{
			name:        "Rack Aware - Missing DC",
			policy:      rackAware,
			localDC:     "",
			localRack:   "rack2",
			expectError: true,
		},
		{
			name:        "Rack Aware - Missing Rack",
			policy:      rackAware,
			localDC:     "us-west-2",
			localRack:   "",
			expectError: true,
		},
		{
			name:               "DC Aware",
			policy:             dcAware,
			localDC:            "eu-central-1",
			expectedPolicyType: gocql.DCAwareRoundRobinPolicy("eu-central-1"),
			expectError:        false,
		},
		{
			name:        "DC Aware - Missing DC",
			policy:      dcAware,
			localDC:     "",
			expectError: true,
		},
		{
			name:               "Success: Round Robin",
			policy:             roundRobin,
			expectedPolicyType: gocql.RoundRobinHostPolicy(),
			expectError:        false,
		},
		{
			name:        "Failure: Unknown Policy",
			policy:      "invalid_fallback_policy",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy, err := fallbackPolicy(tc.policy, tc.localDC, tc.localRack)

			if tc.expectError {
				require.Error(t, err, "Expected an error but got none")
				assert.Nil(t, policy, "Policy should be nil on error")
			} else {
				require.NoError(t, err, "Expected no error but got one")
				require.NotNil(t, policy, "Expected a policy but got nil")
				assert.IsType(t, tc.expectedPolicyType, policy, "Returned policy has an unexpected type")
			}
		})
	}
}
