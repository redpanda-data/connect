package docs_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

func TestInference(t *testing.T) {
	docsProv := docs.NewMappedDocsProvider()
	docsProv.RegisterDocs(docs.ComponentSpec{
		Name: "stdin",
		Type: docs.TypeInput,
	})
	for _, t := range docs.Types() {
		docsProv.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testfoo%v", string(t)),
			Type: t,
		})
		docsProv.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testbar%v", string(t)),
			Type: t,
		})
	}

	type testCase struct {
		inputType docs.Type
		inputConf any

		res string
		err string
	}

	tests := []testCase{
		{
			inputType: docs.TypeOutput,
			inputConf: map[string]any{
				"processors": "yep",
			},
			err: "an explicit output type must be specified",
		},
		{
			inputType: docs.TypeOutput,
			inputConf: map[string]any{
				"foo":        "yep",
				"bar":        "yep",
				"processors": "yep",
			},
			err: "unable to infer output type from candidates: [bar foo]",
		},
		{
			inputType: docs.TypeInput,
			inputConf: map[string]any{
				"foo":        "yep",
				"bar":        "yep",
				"processors": "yep",
			},
			err: "unable to infer input type from candidates: [bar foo]",
		},
		{
			inputType: docs.TypeTracer,
			inputConf: map[string]any{
				"testbartracer": "baz",
				"testbarbuffer": "baz",
			},
			res: "testbartracer",
		},
		{
			inputType: docs.TypeRateLimit,
			inputConf: map[string]any{
				"testbarrate_limit": "baz",
				"testbarbuffer":     "baz",
			},
			res: "testbarrate_limit",
		},
		{
			inputType: docs.TypeProcessor,
			inputConf: map[string]any{
				"testbarprocessor": "baz",
				"testbarbuffer":    "baz",
			},
			res: "testbarprocessor",
		},
		{
			inputType: docs.TypeOutput,
			inputConf: map[string]any{
				"testbaroutput": "baz",
				"testbarbuffer": "baz",
			},
			res: "testbaroutput",
		},
		{
			inputType: docs.TypeMetrics,
			inputConf: map[string]any{
				"testfoometrics": "baz",
				"testbarbuffer":  "baz",
			},
			res: "testfoometrics",
		},
		{
			inputType: docs.TypeInput,
			inputConf: map[string]any{
				"testfooinput":  "baz",
				"testbarbuffer": "baz",
			},
			res: "testfooinput",
		},
		{
			inputType: docs.TypeCache,
			inputConf: map[string]any{
				"testfoocache":  "baz",
				"testbarbuffer": "baz",
			},
			res: "testfoocache",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]any{
				"testfoobuffer": "baz",
				"testbarbuffer": "baz",
			},
			err: "unable to infer buffer type, multiple candidates 'testbarbuffer' and 'testfoobuffer'",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]any{
				"testfoobuffer": "baz",
			},
			res: "testfoobuffer",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]any{
				"type":   "testfoobuffer",
				"foobar": "baz",
			},
			res: "testfoobuffer",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]any{
				"type":   "notreal",
				"foobar": "baz",
			},
			err: "buffer type 'notreal' was not recognised",
		},
		{
			inputType: docs.TypeBuffer,
			err:       "invalid config value <nil>, expected object",
		},
	}

	for i, test := range tests {
		var node yaml.Node
		require.NoError(t, node.Encode(test.inputConf))
		res, spec, err := docs.GetInferenceCandidateFromYAML(docsProv, test.inputType, &node)
		if test.err != "" {
			assert.Error(t, err, "test: %v", i)
		} else {
			assert.Equal(t, test.res, spec.Name, "test: %v", i)
			assert.Equal(t, test.inputType, spec.Type, "test: %v", i)
			assert.NoError(t, err, "test: %v", i)
			assert.Equal(t, test.res, res, "test: %v", i)
		}
	}
}
