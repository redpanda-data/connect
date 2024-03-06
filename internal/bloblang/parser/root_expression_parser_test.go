package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestRootExpressionMappings(t *testing.T) {
	type part struct {
		Content string
		Meta    map[string]any
	}

	tests := map[string]struct {
		index   int
		mapping string
		io      [][2]part
	}{
		"root level if statement": {
			mapping: `
if this.foo > this.bar {
  root.a = "foo was bigger than bar"
  root.b = "yep, agreed"
}
root.c = "idk"
`,
			io: [][2]part{
				{
					{Content: `{"foo":5,"bar":3}`},
					{Content: `{"a":"foo was bigger than bar","b":"yep, agreed","c":"idk"}`},
				},
				{
					{Content: `{"foo":2,"bar":3}`},
					{Content: `{"c":"idk"}`},
				},
			},
		},
		"root level if/else statement": {
			mapping: `
if this.foo > this.bar {

root.a = "foo was bigger than bar"

root.b = "yep, agreed"

} else { root.c = "idk" }
`,
			io: [][2]part{
				{
					{Content: `{"foo":5,"bar":3}`},
					{Content: `{"a":"foo was bigger than bar","b":"yep, agreed"}`},
				},
				{
					{Content: `{"foo":2,"bar":3}`},
					{Content: `{"c":"idk"}`},
				},
			},
		},
		"root level if/elseif/else statement": {
			mapping: `
if this.foo > this.bar {
  root.a = "foo was bigger than bar"
  root.b = "yep, agreed"
} else if this.foo == this.bar {
  root.c = "idk"
} else {
  root.d = "heh, nice"
}
`,
			io: [][2]part{
				{
					{Content: `{"foo":5,"bar":3}`},
					{Content: `{"a":"foo was bigger than bar","b":"yep, agreed"}`},
				},
				{
					{Content: `{"foo":2,"bar":2}`},
					{Content: `{"c":"idk"}`},
				},
				{
					{Content: `{"foo":2,"bar":3}`},
					{Content: `{"d":"heh, nice"}`},
				},
			},
		},
		"root level meta assignments": {
			mapping: `
root = ""
if this.foo > this.bar {
  meta a = "foo was bigger than bar"
  meta b = "yep, agreed"
} else if this.foo == this.bar {
  meta c = "idk"
} else {
  meta = {"d": "heh, nice"}
}
`,
			io: [][2]part{
				{
					{Content: `{"foo":5,"bar":3}`},
					{Meta: map[string]any{"a": "foo was bigger than bar", "b": "yep, agreed"}},
				},
				{
					{Content: `{"foo":2,"bar":2}`},
					{Meta: map[string]any{"c": "idk"}},
				},
				{
					{Content: `{"foo":2,"bar":3}`},
					{Meta: map[string]any{"d": "heh, nice"}},
				},
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			for _, io := range test.io {
				inPart := message.NewPart([]byte(io[0].Content))
				for k, v := range io[0].Meta {
					inPart.MetaSetMut(k, v)
				}

				if io[1].Meta == nil {
					io[1].Meta = map[string]any{}
				}

				exec, perr := ParseMapping(GlobalContext(), test.mapping)
				require.Nil(t, perr)

				resPart, err := exec.MapPart(test.index, message.Batch{inPart})
				require.NoError(t, err)

				outPart := part{
					Content: string(resPart.AsBytes()),
					Meta:    map[string]any{},
				}
				_ = resPart.MetaIterMut(func(k string, v any) error {
					outPart.Meta[k] = v
					return nil
				})
				assert.Equal(t, io[1], outPart)
			}
		})
	}
}
