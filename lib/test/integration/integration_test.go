// +build integration

package integration

import (
	"testing"
)

// Placing this in its own function allows us to only execute under the
// integration build tag, but the tests themselves are always built.
func TestIntegration(t *testing.T) {
	for k, test := range registeredIntegrationTests {
		test := test
		t.Run(k, test)
	}
}
