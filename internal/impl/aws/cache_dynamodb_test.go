package aws

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamoDBCacheConfig(t *testing.T) {
	durPtr := func(d time.Duration) *time.Duration {
		return &d
	}
	strPtr := func(s string) *string {
		return &s
	}

	tests := map[string]struct {
		conf        string
		errContains string
		exp         *dynamodbCache
	}{
		"missing table": {
			conf: `
hash_key: bar
data_key: baz
`,
			errContains: "field 'table' is required",
		},
		"missing hash key": {
			conf: `
table: foo
data_key: baz
`,
			errContains: "field 'hash_key' is required",
		},
		"no ttl or ttl key": {
			conf: `
table: foo
hash_key: bar
data_key: baz
`,
			exp: &dynamodbCache{
				table:          "foo",
				hashKey:        "bar",
				dataKey:        "baz",
				consistentRead: false,
			},
		},
		"ttl and ttl key": {
			conf: `
table: foo
hash_key: bar
data_key: baz
consistent_read: true
default_ttl: 1s
ttl_key: buz
`,
			exp: &dynamodbCache{
				table:          "foo",
				hashKey:        "bar",
				dataKey:        "baz",
				consistentRead: true,
				ttl:            durPtr(time.Second),
				ttlKey:         strPtr("buz"),
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			conf, err := dynCacheConfig().ParseYAML(test.conf, nil)
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
				dc, err := newDynamodbCacheFromConfig(conf)
				require.NoError(t, err)

				dc.boffPool = sync.Pool{}
				dc.client = nil
				assert.Equal(t, test.exp, dc)
			}
		})
	}
}
