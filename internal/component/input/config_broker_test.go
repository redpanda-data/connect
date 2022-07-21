package input_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestBrokerConfigDefaults(t *testing.T) {
	testConf := []byte(`{
		"type": "broker",
		"broker": {
			"inputs": [
				{
					"type": "generate",
					"generate": {
						"count": 2,
						"mapping": "root = {}"
					}
				},
				{
					"type": "generate",
					"generate": {
						"mapping": "root = 5"
					}
				}
			]
		}
	}`)

	var conf input.Config
	check := func() {
		t.Helper()

		inputConfs := conf.Broker.Inputs

		require.Len(t, inputConfs, 2)
		assert.Equal(t, "generate", inputConfs[0].Type)
		assert.Equal(t, "generate", inputConfs[1].Type)

		assert.Equal(t, 2, inputConfs[0].Generate.Count)
		assert.Equal(t, "root = {}", inputConfs[0].Generate.Mapping)
		assert.Equal(t, "1s", inputConfs[0].Generate.Interval)

		assert.Equal(t, 0, inputConfs[1].Generate.Count)
		assert.Equal(t, "root = 5", inputConfs[1].Generate.Mapping)
		assert.Equal(t, "1s", inputConfs[1].Generate.Interval)
	}

	conf = input.NewConfig()
	if err := yaml.Unmarshal(testConf, &conf); err != nil {
		t.Fatal(err)
	}
	check()
}
