package output_test

import (
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestOutBrokerConfigDefaults(t *testing.T) {
	testConf := []byte(`{
		"type": "broker",
		"broker": {
			"outputs": [
				{
					"type": "retry",
					"retry": {
						"backoff": {
							"initial_interval": "900ms"
						},
						"output": {
							"drop": {}
						}
					}
				},
				{
					"type": "retry",
					"retry": {
						"backoff": {
							"max_interval": "2s"
						},
						"output": {
							"drop": {}
						}
					}
				}
			]
		}
	}`)

	conf := output.NewConfig()
	if err := yaml.Unmarshal(testConf, &conf); err != nil {
		t.Error(err)
		return
	}

	outputConfs := conf.Broker.Outputs

	require.Len(t, outputConfs, 2)
	assert.Equal(t, "retry", outputConfs[0].Type)
	assert.Equal(t, "retry", outputConfs[1].Type)

	assert.Equal(t, "900ms", outputConfs[0].Retry.Backoff.InitialInterval)
	assert.Equal(t, "3s", outputConfs[0].Retry.Backoff.MaxInterval)

	assert.Equal(t, "500ms", outputConfs[1].Retry.Backoff.InitialInterval)
	assert.Equal(t, "2s", outputConfs[1].Retry.Backoff.MaxInterval)
}
