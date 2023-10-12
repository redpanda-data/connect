package kafka_test

import (
	"fmt"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

func parseYAMLInputConf(t testing.TB, formatStr string, args ...any) (conf input.Config) {
	t.Helper()
	conf = input.NewConfig()
	require.NoError(t, yaml.Unmarshal(fmt.Appendf(nil, formatStr, args...), &conf))
	return
}

func TestKafkaBadParams(t *testing.T) {
	testCases := []struct {
		name   string
		topics []string
		errStr string
	}{
		{
			name:   "mixing consumer types",
			topics: []string{"foo", "foo:1"},
			errStr: "it is not currently possible to include balanced and explicit partition topics in the same kafka input",
		},
		{
			name:   "too many partitions",
			topics: []string{"foo:1:2:3"},
			errStr: "topic 'foo:1:2:3' is invalid, only one partition and an optional offset should be specified",
		},
		{
			name:   "bad range",
			topics: []string{"foo:1-2-3"},
			errStr: "partition '1-2-3' is invalid, only one range can be specified",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			conf := parseYAMLInputConf(t, `
kafka:
  addresses: [ example.com:1234 ]
  topics: %v
`, gabs.Wrap(test.topics).String())

			_, err := mock.NewManager().NewInput(conf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.errStr)
		})
	}
}
