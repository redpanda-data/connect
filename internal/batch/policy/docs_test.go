package policy_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

func TestBatchPolicySanit(t *testing.T) {
	conf := batchconfig.NewConfig()

	var node yaml.Node
	require.NoError(t, node.Encode(conf))

	sanitConf := docs.NewSanitiseConfig()
	sanitConf.RemoveTypeField = true
	require.NoError(t, policy.FieldSpec().SanitiseYAML(&node, sanitConf))

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
