package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckRedisBloomConfiguration(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label string
		conf  string

		parseErrMsg string
		errMsg      string
	}{
		{
			label:       `field 'url' is mandatory`,
			conf:        ``,
			parseErrMsg: `field 'url' is required and was not present in the config`,
		},
		{
			label:  `field 'filter_key' is mandatory`,
			conf:   `url: redis://localhost`,
			errMsg: `field 'filter_key' was not found in the config`,
		},
		{
			label: `field 'filter_key' should not be empty empty string`,
			conf: `url: redis://localhost
filter_key: ""`,
			errMsg: `field 'filter_key' should not be empty`,
		},
		{
			label: `minimal example`,
			conf: `url: redis://localhost
filter_key: "foo"`,
		},
		{
			label: `conf with empty 'insert_options' field`,
			conf: `url: redis://localhost
filter_key: "foo"
insert_options: {}`,
		},
		{
			label: `conf with some 'insert_options.capacity' field`,
			conf: `url: redis://localhost
filter_key: "foo"
insert_options:
  capacity: 50000`,
		},
		{
			label: `conf with some 'insert_options.error_rate' field`,
			conf: `url: redis://localhost
filter_key: "foo"
insert_options:
  error_rate: 0.001`,
		},
		{
			label: `conf with some 'insert_options.expansion' field`,
			conf: `url: redis://localhost
filter_key: "foo"
insert_options:
  expansion: 4`,
		},
		{
			label: `conf with some 'insert_options.no_create' field`,
			conf: `url: redis://localhost
filter_key: "foo"
insert_options:
  no_create: true`,
		},
		{
			label: `conf with some 'insert_options.non_scalling' field`,
			conf: `url: redis://localhost
filter_key: "foo"
insert_options:
  non_scalling: true`,
		},
		{
			label: `complete example`,
			conf: `url: redis://localhost
filter_key: "foo"
insert_options:
  capacity: 50000
  no_create: true`,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()

			cacheConfig, err := redisCacheConfig().ParseYAML(tc.conf, nil)
			if tc.parseErrMsg != "" {
				assert.EqualError(t, err, tc.parseErrMsg)

				return
			}

			require.NoError(t, err)

			cache, err := newRedisBloomCacheFromConfig(cacheConfig)
			if tc.errMsg != "" {
				assert.EqualError(t, err, tc.errMsg)

				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, cache)
		})
	}
}
