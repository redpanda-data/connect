package kafka

import (
	"fmt"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
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
			pConf, err := iskConfigSpec().ParseYAML(fmt.Sprintf(`
addresses: [ example.com:1234 ]
topics: %v
`, gabs.Wrap(test.topics).String()), nil)
			require.NoError(t, err)

			_, err = newKafkaReaderFromParsed(pConf, service.MockResources())
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.errStr)
		})
	}
}
