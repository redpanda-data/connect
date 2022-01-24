package docs_test

import (
	"fmt"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestInference(t *testing.T) {
	docsProv := docs.NewMappedDocsProvider()
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
		inputType    docs.Type
		inputConf    interface{}
		inputDefault string

		res string
		err string
	}

	tests := []testCase{
		{
			inputType: docs.TypeInput,
			inputConf: map[string]interface{}{
				"processors": "yep",
			},
			inputDefault: "testfooinput",
			res:          "testfooinput",
		},
		{
			inputType: docs.TypeOutput,
			inputConf: map[string]interface{}{
				"foo":        "yep",
				"bar":        "yep",
				"processors": "yep",
			},
			err: "unable to infer output type, candidates were: [bar foo]",
		},
		{
			inputType: docs.TypeInput,
			inputConf: map[string]interface{}{
				"foo":        "yep",
				"bar":        "yep",
				"processors": "yep",
			},
			err: "unable to infer input type, candidates were: [bar foo]",
		},
		{
			inputType: docs.TypeTracer,
			inputConf: map[string]interface{}{
				"testbartracer": "baz",
				"testbarbuffer": "baz",
			},
			res: "testbartracer",
		},
		{
			inputType: docs.TypeRateLimit,
			inputConf: map[string]interface{}{
				"testbarrate_limit": "baz",
				"testbarbuffer":     "baz",
			},
			res: "testbarrate_limit",
		},
		{
			inputType: docs.TypeProcessor,
			inputConf: map[string]interface{}{
				"testbarprocessor": "baz",
				"testbarbuffer":    "baz",
			},
			res: "testbarprocessor",
		},
		{
			inputType: docs.TypeOutput,
			inputConf: map[string]interface{}{
				"testbaroutput": "baz",
				"testbarbuffer": "baz",
			},
			res: "testbaroutput",
		},
		{
			inputType: docs.TypeMetrics,
			inputConf: map[string]interface{}{
				"testfoometrics": "baz",
				"testbarbuffer":  "baz",
			},
			res: "testfoometrics",
		},
		{
			inputType: docs.TypeInput,
			inputConf: map[string]interface{}{
				"testfooinput":  "baz",
				"testbarbuffer": "baz",
			},
			res: "testfooinput",
		},
		{
			inputType: docs.TypeCache,
			inputConf: map[string]interface{}{
				"testfoocache":  "baz",
				"testbarbuffer": "baz",
			},
			res: "testfoocache",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]interface{}{
				"testfoobuffer": "baz",
				"testbarbuffer": "baz",
			},
			err: "unable to infer buffer type, multiple candidates 'testbarbuffer' and 'testfoobuffer'",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]interface{}{
				"testfoobuffer": "baz",
			},
			res: "testfoobuffer",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]interface{}{
				"type":   "testfoobuffer",
				"foobar": "baz",
			},
			res: "testfoobuffer",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]interface{}{
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
		res, spec, err := docs.GetInferenceCandidate(docsProv, test.inputType, test.inputDefault, test.inputConf)
		if len(test.err) > 0 {
			assert.EqualError(t, err, test.err, "test: %v", i)
		} else {
			assert.Equal(t, test.res, spec.Name)
			assert.Equal(t, test.inputType, spec.Type)
			assert.NoError(t, err)
			assert.Equal(t, test.res, res, "test: %v", i)
		}

		var node yaml.Node
		require.NoError(t, node.Encode(test.inputConf))
		res, spec, err = docs.GetInferenceCandidateFromYAML(docsProv, test.inputType, test.inputDefault, &node)
		if len(test.err) > 0 {
			assert.Error(t, err)
		} else {
			assert.Equal(t, test.res, spec.Name)
			assert.Equal(t, test.inputType, spec.Type)
			assert.NoError(t, err)
			assert.Equal(t, test.res, res, "test: %v", i)
		}
	}
}
