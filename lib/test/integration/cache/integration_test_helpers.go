package cache

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"
)

type testConfigVars struct {
	// A unique identifier for separating this test configuration from others.
	// Usually used to access a different topic, consumer group, directory, etc.
	id string

	// A port to use in connector URLs. Allowing tests to override this
	// potentially enables tests that check for faulty connections by bridging.
	port string

	// Generic variables.
	var1 string
}

type testEnvironment struct {
	configTemplate string
	configVars     testConfigVars

	preTest func(*testing.T, *testEnvironment)

	timeout time.Duration
	ctx     context.Context
	log     log.Modular
	stats   metrics.Type
}

func newTestEnvironment(t *testing.T, confTemplate string) testEnvironment {
	t.Helper()

	u4, err := uuid.NewV4()
	require.NoError(t, err)

	return testEnvironment{
		configTemplate: confTemplate,
		configVars: testConfigVars{
			id: u4.String(),
		},
		timeout: time.Second * 90,
		ctx:     context.Background(),
		log:     log.Noop(),
		stats:   metrics.Noop(),
	}
}

func (e testEnvironment) RenderConfig() string {
	return strings.NewReplacer(
		"$ID", e.configVars.id,
		"$PORT", e.configVars.port,
		"$VAR1", e.configVars.var1,
	).Replace(e.configTemplate)
}

//------------------------------------------------------------------------------

type testOptFunc func(*testEnvironment)

func testOptTimeout(timeout time.Duration) testOptFunc {
	return func(env *testEnvironment) {
		env.timeout = timeout
	}
}

func testOptLogging(level string) testOptFunc {
	return func(env *testEnvironment) {
		logConf := log.NewConfig()
		logConf.LogLevel = level
		env.log = log.New(os.Stdout, logConf)
	}
}

func testOptPort(port string) testOptFunc {
	return func(env *testEnvironment) {
		env.configVars.port = port
	}
}

func testOptVarOne(v string) testOptFunc {
	return func(env *testEnvironment) {
		env.configVars.var1 = v
	}
}

func testOptPreTest(fn func(*testing.T, *testEnvironment)) testOptFunc {
	return func(env *testEnvironment) {
		env.preTest = fn
	}
}

//------------------------------------------------------------------------------

type testDefinition func(*testing.T, *testEnvironment)

type integrationTestList []testDefinition

func integrationTests(tests ...testDefinition) integrationTestList {
	return tests
}

func (i integrationTestList) Run(t *testing.T, configTemplate string, opts ...testOptFunc) {
	for _, test := range i {
		env := newTestEnvironment(t, configTemplate)
		for _, opt := range opts {
			opt(&env)
		}

		var done func()
		env.ctx, done = context.WithTimeout(env.ctx, env.timeout)
		t.Cleanup(done)

		if env.preTest != nil {
			env.preTest(t, &env)
		}
		test(t, &env)
	}
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

//------------------------------------------------------------------------------

func namedTest(name string, test testDefinition) testDefinition {
	return func(t *testing.T, env *testEnvironment) {
		t.Run(name, func(t *testing.T) {
			test(t, env)
		})
	}
}

//------------------------------------------------------------------------------

func initCache(t *testing.T, env *testEnvironment) types.Cache {
	t.Helper()

	confBytes := []byte(env.RenderConfig())

	s := config.New()
	dec := yaml.NewDecoder(bytes.NewReader(confBytes))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&s))

	lints, err := config.Lint(confBytes, s)
	require.NoError(t, err)
	assert.Empty(t, lints)

	manager, err := manager.NewV2(s.ResourceConfig, types.NoopMgr(), env.log, env.stats)
	require.NoError(t, err)

	cache, err := manager.GetCache("testcache")
	require.NoError(t, err)

	return cache
}

func closeCache(t *testing.T, cache types.Cache) {
	cache.CloseAsync()
	require.NoError(t, cache.WaitForClose(time.Second*10))
}
