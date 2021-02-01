package reader

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestKafkaDeprecationCheck(t *testing.T) {
	testCases := []struct {
		name string
		conf string
		exp  bool
	}{
		{
			name: "all defaults",
			conf: `{}`,
			exp:  false,
		},
		{
			name: "has both topics and topic",
			conf: `topic: foobar
topics: [ foo, bar ]`,
			exp: true,
		},
		{
			name: "has just topics",
			conf: `topics: [ foo, bar ]
key: fookey`,
			exp: false,
		},
		{
			name: "has topic and partition",
			conf: `topic: foobar
partition: 0`,
			exp: true,
		},
		{
			name: "has topic",
			conf: `topic: foobar
key: fookey`,
			exp: true,
		},
		{
			name: "has partition",
			conf: `partition: 0
key: fookey`,
			exp: true,
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var conf KafkaConfig
			require.NoError(t, yaml.Unmarshal([]byte(test.conf), &conf))
			assert.Equal(t, test.exp, conf.IsDeprecated())
		})
	}
}
