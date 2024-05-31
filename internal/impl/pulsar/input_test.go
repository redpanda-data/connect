package pulsar

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestParseInputTopicXorPattern(t *testing.T) {
	tests := []struct {
		name, config string
		errStr       string
	}{
		{
			name:   "topics",
			config: `topics: ["my_cool_topic"]`,
		},
		{
			name:   "topics_pattern",
			config: `topics_pattern: ".*cool_topic"`,
		},
		{
			name:   "topics and topics_pattern fails",
			errStr: "exactly one of fields topics and topics_pattern must be set",
			config: `
topics: ["my_cool_topic"]
topics_pattern: ".*_cool_topic"
`,
		},
		{
			name:   "providing neither fails",
			errStr: "exactly one of fields topics and topics_pattern must be set",
			config: ``,
		},
	}

	baseConfig := `
url: pulsar://localhost:6650/
subscription_name: "sub"
`
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := service.NewEnvironment()

			conf := baseConfig + test.config
			parsed, err := inputConfigSpec().ParseYAML(conf, env)
			require.NoError(t, err, "parse config")

			reader, err := newPulsarReaderFromParsed(parsed, service.MockResources().Logger())
			if test.errStr != "" {
				require.EqualError(t, err, test.errStr)
			} else {
				require.NoError(t, err, "new reader from parsed")
				require.NoError(t, reader.Close(context.Background()))
			}
		})
	}
}
