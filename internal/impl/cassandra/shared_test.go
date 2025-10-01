package cassandra

import (
	"reflect"
	"testing"
	"time"

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

func Test_newReconnectionPolicy(t *testing.T) {
	defaultPolicy := &gocql.ConstantReconnectionPolicy{MaxRetries: 3, Interval: 1 * time.Second}

	testCases := []struct {
		name              string
		initialInterval   time.Duration
		maxRetries        int
		maxInterval       time.Duration
		expectedPolicy    gocql.ReconnectionPolicy
		expectExponential bool
	}{
		{
			name:              "Valid Exponential",
			initialInterval:   2 * time.Second,
			maxRetries:        5,
			maxInterval:       60 * time.Second,
			expectedPolicy:    &gocql.ExponentialReconnectionPolicy{MaxRetries: 5, InitialInterval: 2 * time.Second, MaxInterval: 60 * time.Second},
			expectExponential: true,
		},
		{
			name:              "Zero InitialInterval",
			initialInterval:   0,
			maxRetries:        5,
			maxInterval:       60 * time.Second,
			expectedPolicy:    defaultPolicy,
			expectExponential: false,
		},
		{
			name:              "Zero MaxRetries",
			initialInterval:   2 * time.Second,
			maxRetries:        0,
			maxInterval:       60 * time.Second,
			expectedPolicy:    defaultPolicy,
			expectExponential: false,
		},
		{
			name:              "Zero MaxInterval",
			initialInterval:   2 * time.Second,
			maxRetries:        5,
			maxInterval:       0,
			expectedPolicy:    defaultPolicy,
			expectExponential: false,
		},
		{
			name:              "All Zero- Fallback to Constant",
			initialInterval:   0,
			maxRetries:        0,
			maxInterval:       0,
			expectedPolicy:    defaultPolicy,
			expectExponential: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy := newReconnectionPolicy(tc.initialInterval, tc.maxRetries, tc.maxInterval)

			_, isExponential := policy.(*gocql.ExponentialReconnectionPolicy)
			if isExponential != tc.expectExponential {
				t.Errorf("Expected exponential policy: %v, but got: %v", tc.expectExponential, isExponential)
			}

			_, isConstant := policy.(*gocql.ConstantReconnectionPolicy)
			if isConstant == tc.expectExponential {
				t.Errorf("Expected constant policy: %v, but got: %v", !tc.expectExponential, isConstant)
			}

			if !reflect.DeepEqual(policy, tc.expectedPolicy) {
				t.Errorf("newReconnectionPolicy() = %v, want %v", policy, tc.expectedPolicy)
			}
		})
	}
}
