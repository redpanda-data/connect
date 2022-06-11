package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

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
			errStr: "topic 'foo:1:2:3' is invalid, only one partition should be specified and the same topic can be listed multiple times, e.g. use `foo:0,foo:1` not `foo:0:1`",
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
			conf := input.NewConfig()
			conf.Type = "kafka"
			conf.Kafka.Addresses = []string{"example.com:1234"}
			conf.Kafka.Topics = test.topics

			_, err := mock.NewManager().NewInput(conf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.errStr)
		})
	}
}
