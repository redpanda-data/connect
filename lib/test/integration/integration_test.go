package integration

import (
	"flag"
	"regexp"
	"testing"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

// Placing this in its own function allows us to only execute under the
// integration build tag, but the tests themselves are always built.
func TestIntegration(t *testing.T) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || !regexp.MustCompile(m).MatchString(t.Name()) {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^TestIntegration$")
	}

	for k, test := range registeredIntegrationTests {
		test := test
		t.Run(k, test)
	}
}
