// +build integration

package integration

import (
	"testing"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

// Placing this in its own function allows us to only execute under the
// integration build tag, but the tests themselves are always built.
func TestIntegration(t *testing.T) {
	for k, test := range registeredIntegrationTests {
		test := test
		t.Run(k, test)
	}
}
