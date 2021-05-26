package cache

import (
	"testing"
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
	suite := integrationTests(
		integrationTestOpenClose(),
		integrationTestMissingKey(),
		integrationTestDoubleAdd(),
		integrationTestDelete(),
		integrationTestGetAndSet(50),
	)
	suite.Run(t, template)
})
