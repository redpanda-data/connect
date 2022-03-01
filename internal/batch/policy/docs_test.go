package policy_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/internal/batch/policy"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestBatchPolicySanit(t *testing.T) {
	conf := policy.NewConfig()

	var node yaml.Node
	require.NoError(t, node.Encode(conf))
	require.NoError(t, policy.FieldSpec().SanitiseYAML(&node, docs.SanitiseConfig{
		RemoveTypeField: true,
	}))

	expSanit := `count: 0
byte_size: 0
period: ""
check: ""
processors: []
`

	b, err := yaml.Marshal(node)
	require.NoError(t, err)
	assert.Equal(t, expSanit, string(b))
}
