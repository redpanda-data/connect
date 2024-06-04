package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestKafkaFranzOutputBadParams(t *testing.T) {
	testCases := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name: "manual partitioner with a partition",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  partitioner: manual
  partition: '${! meta("foo") }'
`,
		},
		{
			name: "non manual partitioner without a partition",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
`,
		},
		{
			name: "manual partitioner with no partition",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  partitioner: manual
`,
			errContains: "a partition must be specified when the partitioner is set to manual",
		},
		{
			name: "partition without manual partitioner",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  partition: '${! meta("foo") }'
`,
			errContains: "a partition cannot be specified unless the partitioner is set to manual",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			err := service.NewStreamBuilder().AddOutputYAML(test.conf)
			if test.errContains == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}
