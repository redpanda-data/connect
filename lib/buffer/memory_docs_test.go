package buffer_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"
)

func TestMemorySanit(t *testing.T) {
	conf := buffer.NewConfig()
	conf.Type = buffer.TypeMemory

	var node yaml.Node
	require.NoError(t, node.Encode(conf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeBuffer, &node, docs.SanitiseConfig{
		RemoveTypeField: true,
	}))

	expSanit := `memory:
    limit: 524288000
    batch_policy:
        enabled: false
        count: 0
        byte_size: 0
        period: ""
        inactivity_period: ""
        check: ""
        processors: []
`

	b, err := yaml.Marshal(node)
	require.NoError(t, err)
	assert.Equal(t, expSanit, string(b))
}
