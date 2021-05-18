package docs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestFieldsFromNode(t *testing.T) {
	tests := []struct {
		name   string
		yaml   string
		fields FieldSpecs
	}{
		{
			name: "flat object",
			yaml: `a: foo
b: bar
c: 21`,
			fields: FieldSpecs{
				FieldCommon("a", ""),
				FieldCommon("b", ""),
				FieldCommon("c", ""),
			},
		},
		{
			name: "nested object",
			yaml: `a: foo
b:
  d: bar
  e: 22
c: 21`,
			fields: FieldSpecs{
				FieldCommon("a", ""),
				FieldCommon("b", "").WithChildren(
					FieldCommon("d", ""),
					FieldCommon("e", ""),
				),
				FieldCommon("c", ""),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			confBytes := []byte(test.yaml)

			var node yaml.Node
			require.NoError(t, yaml.Unmarshal(confBytes, &node))

			if node.Kind == yaml.DocumentNode && node.Content[0].Kind == yaml.MappingNode {
				node = *node.Content[0]
			}

			assert.Equal(t, test.fields, FieldsFromNode(&node))
		})
	}
}
