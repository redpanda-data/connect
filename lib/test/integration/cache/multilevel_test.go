package cache

import (
	"testing"

	"github.com/Jeffail/benthos/v3/internal/integration"
)

var _ = registerIntegrationTest("multilevel", func(t *testing.T) {
	t.Parallel()

	template := `
cache_resources:
  - label: testcache
    multilevel: [ first, second ]
  - label: first
    memory: {}
  - label: second
    memory: {}
`
	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
	)
	suite.Run(t, template)
})
