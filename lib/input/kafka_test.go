package input

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
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
			errStr: "failed to create input 'kafka': it is not currently possible to include balanced and explicit partition topics in the same kafka input",
		},
		{
			name:   "too many partitions",
			topics: []string{"foo:1:2:3"},
			errStr: "failed to create input 'kafka': topic 'foo:1:2:3' is invalid, only one partition should be specified and the same topic can be listed multiple times, e.g. use `foo:0,foo:1` not `foo:0:1`",
		},
		{
			name:   "bad range",
			topics: []string{"foo:1-2-3"},
			errStr: "failed to create input 'kafka': partition '1-2-3' is invalid, only one range can be specified",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = TypeKafka
			conf.Kafka.Addresses = []string{"example.com:1234"}
			conf.Kafka.Topics = test.topics

			_, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
			assert.EqualError(t, err, test.errStr)
		})
	}
}
