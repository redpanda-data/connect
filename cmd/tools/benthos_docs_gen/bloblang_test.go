package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func TestFunctionExamples(t *testing.T) {
	tmpJSONFile, err := os.CreateTemp("", "benthos_bloblang_functions_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(tmpJSONFile.Name())
	})

	_, err = tmpJSONFile.WriteString(`{"foo":"bar"}`)
	require.NoError(t, err)

	key := "BENTHOS_TEST_BLOBLANG_FILE"
	os.Setenv(key, tmpJSONFile.Name())
	t.Cleanup(func() {
		os.Unsetenv(key)
	})

	for _, spec := range query.FunctionDocs() {
		spec := spec
		t.Run(spec.Name, func(t *testing.T) {
			t.Parallel()
			for i, e := range spec.Examples {
				m, err := bloblang.GlobalEnvironment().NewMapping(e.Mapping)
				require.NoError(t, err)

				for j, io := range e.Results {
					msg := message.QuickBatch([][]byte{[]byte(io[0])})
					p, err := m.MapPart(0, msg)
					exp := io[1]
					if strings.HasPrefix(exp, "Error(") {
						exp = exp[7 : len(exp)-2]
						require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
					} else {
						require.NoError(t, err)
						assert.Equal(t, exp, string(p.Get()), fmt.Sprintf("%v-%v", i, j))
					}
				}
			}
		})
	}
}

func TestMethodExamples(t *testing.T) {
	tmpJSONFile, err := os.CreateTemp("", "benthos_bloblang_methods_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(tmpJSONFile.Name())
	})

	_, err = tmpJSONFile.WriteString(`
  "type":"object",
  "properties":{
    "foo":{
      "type":"string"
    }
  }
}`)
	require.NoError(t, err)

	key := "BENTHOS_TEST_BLOBLANG_SCHEMA_FILE"
	os.Setenv(key, tmpJSONFile.Name())
	t.Cleanup(func() {
		os.Unsetenv(key)
	})

	for _, spec := range query.MethodDocs() {
		spec := spec
		t.Run(spec.Name, func(t *testing.T) {
			t.Parallel()
			for i, e := range spec.Examples {
				m, err := bloblang.GlobalEnvironment().NewMapping(e.Mapping)
				require.NoError(t, err)

				for j, io := range e.Results {
					msg := message.QuickBatch([][]byte{[]byte(io[0])})
					p, err := m.MapPart(0, msg)
					exp := io[1]
					if strings.HasPrefix(exp, "Error(") {
						exp = exp[7 : len(exp)-2]
						require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
					} else if exp == "<Message deleted>" {
						require.NoError(t, err)
						require.Nil(t, p)
					} else {
						require.NoError(t, err)
						assert.Equal(t, exp, string(p.Get()), fmt.Sprintf("%v-%v", i, j))
					}
				}
			}
			for _, target := range spec.Categories {
				for i, e := range target.Examples {
					m, err := bloblang.GlobalEnvironment().NewMapping(e.Mapping)
					require.NoError(t, err)

					for j, io := range e.Results {
						msg := message.QuickBatch([][]byte{[]byte(io[0])})
						p, err := m.MapPart(0, msg)
						exp := io[1]
						if strings.HasPrefix(exp, "Error(") {
							exp = exp[7 : len(exp)-2]
							require.EqualError(t, err, exp, fmt.Sprintf("%v-%v-%v", target.Category, i, j))
						} else {
							require.NoError(t, err)
							assert.Equal(t, exp, string(p.Get()), fmt.Sprintf("%v-%v-%v", target.Category, i, j))
						}
					}
				}
			}
		})
	}
}
