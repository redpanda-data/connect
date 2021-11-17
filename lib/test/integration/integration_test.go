package integration

import (
	"flag"
	"fmt"
	"net"
	"regexp"
	"strings"
	"testing"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

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
	if m := flag.Lookup("test.run").Value.String(); m == "" || regexp.MustCompile(strings.Split(m, "/")[0]).FindString(t.Name()) == "" {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^TestIntegration$")
	}

	for k, test := range registeredIntegrationTests {
		test := test
		t.Run(k, test)
	}
}

var registeredIntegrationBenchmarks = map[string]func(*testing.B){}

// register an integration test that should only execute under the `integration`
// build tag. Returns an empty struct so that it can be called at a file root.
func registerIntegrationBench(name string, fn func(*testing.B)) struct{} {
	if _, exists := registeredIntegrationBenchmarks[name]; exists {
		panic(fmt.Sprintf("integration benchmark double registered: %v", name))
	}
	registeredIntegrationBenchmarks[name] = fn
	return struct{}{}
}

func BenchmarkIntegration(b *testing.B) {
	for k, test := range registeredIntegrationBenchmarks {
		test := test
		b.Run(k, test)
	}
}
