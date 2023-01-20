package integration

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
)

// CacheTestConfigVars exposes some variables injected into template configs for
// cache unit tests.
type CacheTestConfigVars struct {
	// A unique identifier for separating this test configuration from others.
	// Usually used to access a different topic, consumer group, directory, etc.
	ID string

	// A Port to use in connector URLs. Allowing tests to override this
	// potentially enables tests that check for faulty connections by bridging.
	Port string

	// Generic variables.
	Var1 string
	Var2 string
}

// CachePreTestFn is an optional closure to be called before tests are run, this
// is an opportunity to mutate test config variables and mess with the
// environment.
type CachePreTestFn func(t testing.TB, ctx context.Context, testID string, vars *CacheTestConfigVars)

type cacheTestEnvironment struct {
	configTemplate string
	configVars     CacheTestConfigVars

	preTest CachePreTestFn

	timeout time.Duration
	ctx     context.Context
	log     log.Modular
	stats   *metrics.Namespaced
}

func newCacheTestEnvironment(t *testing.T, confTemplate string) cacheTestEnvironment {
	t.Helper()

	u4, err := uuid.NewV4()
	require.NoError(t, err)

	return cacheTestEnvironment{
		configTemplate: confTemplate,
		configVars: CacheTestConfigVars{
			ID: u4.String(),
		},
		timeout: time.Second * 90,
		ctx:     context.Background(),
		log:     log.Noop(),
		stats:   metrics.Noop(),
	}
}

func (e cacheTestEnvironment) RenderConfig() string {
	return strings.NewReplacer(
		"$ID", e.configVars.ID,
		"$PORT", e.configVars.Port,
		"$VAR1", e.configVars.Var1,
		"$VAR2", e.configVars.Var2,
	).Replace(e.configTemplate)
}

//------------------------------------------------------------------------------

// CacheTestOptFunc is an opt func for customizing the behaviour of cache tests,
// these are useful for things that are integration environment specific, such
// as the port of the service being interacted with.
type CacheTestOptFunc func(*cacheTestEnvironment)

// CacheTestOptTimeout describes an optional timeout spanning the entirety of
// the test suite.
func CacheTestOptTimeout(timeout time.Duration) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		env.timeout = timeout
	}
}

// CacheTestOptLogging allows components to log with the given log level. This
// is useful for diagnosing issues.
func CacheTestOptLogging(level string) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		logConf := log.NewConfig()
		logConf.LogLevel = level
		var err error
		env.log, err = log.NewV2(os.Stdout, logConf)
		if err != nil {
			panic(err)
		}
	}
}

// CacheTestOptPort defines the port of the integration service.
func CacheTestOptPort(port string) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		env.configVars.Port = port
	}
}

// CacheTestOptVarOne sets an arbitrary variable for the test that can be
// injected into templated configs.
func CacheTestOptVarOne(v string) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		env.configVars.Var1 = v
	}
}

// CacheTestOptVarTwo sets an arbitrary variable for the test that can be
// injected into templated configs.
func CacheTestOptVarTwo(v string) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		env.configVars.Var2 = v
	}
}

// CacheTestOptPreTest adds a closure to be executed before each test.
func CacheTestOptPreTest(fn CachePreTestFn) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		env.preTest = fn
	}
}

//------------------------------------------------------------------------------

type cacheTestDefinitionFn func(*testing.T, *cacheTestEnvironment)

// CacheTestDefinition encompasses a unit test to be executed against an
// integration environment. These tests are generic and can be run against any
// configuration containing an input and an output that are connected.
type CacheTestDefinition struct {
	fn func(*testing.T, *cacheTestEnvironment)
}

// CacheTestList is a list of cache test definitions that can be run with a
// single template and function args.
type CacheTestList []CacheTestDefinition

// CacheTests creates a list of tests from variadic arguments.
func CacheTests(tests ...CacheTestDefinition) CacheTestList {
	return tests
}

// Run all the tests against a config template. Tests are run in parallel.
func (i CacheTestList) Run(t *testing.T, configTemplate string, opts ...CacheTestOptFunc) {
	for _, test := range i {
		env := newCacheTestEnvironment(t, configTemplate)
		for _, opt := range opts {
			opt(&env)
		}

		var done func()
		env.ctx, done = context.WithTimeout(env.ctx, env.timeout)
		t.Cleanup(done)

		test.fn(t, &env)
	}
}

//------------------------------------------------------------------------------

func namedCacheTest(name string, test cacheTestDefinitionFn) CacheTestDefinition {
	return CacheTestDefinition{
		fn: func(t *testing.T, env *cacheTestEnvironment) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				if env.preTest != nil {
					env.preTest(t, env.ctx, env.configVars.ID, &env.configVars)
				}
				test(t, env)
			})
		},
	}
}

//------------------------------------------------------------------------------

func initCache(t *testing.T, env *cacheTestEnvironment) cache.V1 {
	t.Helper()

	confBytes := []byte(env.RenderConfig())

	s := config.New()
	dec := yaml.NewDecoder(bytes.NewReader(confBytes))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&s))

	lints, err := config.LintBytes(config.LintOptions{}, confBytes)
	require.NoError(t, err)
	assert.Empty(t, lints)

	manager, err := manager.New(s.ResourceConfig, manager.OptSetLogger(env.log), manager.OptSetMetrics(env.stats))
	require.NoError(t, err)

	var c cache.V1
	require.NoError(t, manager.AccessCache(env.ctx, "testcache", func(v cache.V1) {
		c = v
	}))
	return c
}

func closeCache(t *testing.T, cache cache.V1) {
	require.NoError(t, cache.Close(context.Background()))
}
