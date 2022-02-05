package cache

import (
	"flag"
	"fmt"
	"regexp"
	"testing"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

var registeredIntegrationTests = map[string]func(*testing.T){}

// register an integration test that should only execute under the `integration`
// build tag. Returns an empty struct so that it can be called at a file root.
func registerIntegrationTest(name string, fn func(*testing.T)) struct{} {
	if _, exists := registeredIntegrationTests[name]; exists {
		panic(fmt.Sprintf("integration test double registered: %v", name))
	}
	registeredIntegrationTests[name] = fn
	return struct{}{}
}

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
