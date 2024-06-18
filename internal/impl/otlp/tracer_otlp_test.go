package otlp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigParsingAddresses(t *testing.T) {
	pConf, err := oltpSpec().ParseYAML(`
http:
  - address: foo:123
  - address: foo:456
    secure: true
  - {}
grpc:
  - address: bar:123
  - address: bar:456
    secure: true
  - {}
sampling:
  enabled: true
  ratio: 0.55
`, nil)
	require.NoError(t, err)

	cConf, err := oltpConfigFromParsed(pConf)
	require.NoError(t, err)

	assert.True(t, cConf.sampling.enabled)
	assert.Equal(t, 0.55, cConf.sampling.ratio)

	require.Len(t, cConf.http, 3)
	assert.Equal(t, "foo:123", cConf.http[0].address)
	assert.False(t, cConf.http[0].secure)
	assert.Equal(t, "foo:456", cConf.http[1].address)
	assert.True(t, cConf.http[1].secure)
	assert.Equal(t, "localhost:4318", cConf.http[2].address)
	assert.False(t, cConf.http[2].secure)

	require.Len(t, cConf.grpc, 3)
	assert.Equal(t, "bar:123", cConf.grpc[0].address)
	assert.False(t, cConf.grpc[0].secure)
	assert.Equal(t, "bar:456", cConf.grpc[1].address)
	assert.True(t, cConf.grpc[1].secure)
	assert.Equal(t, "localhost:4317", cConf.grpc[2].address)
	assert.False(t, cConf.grpc[2].secure)
}

func TestConfigParsingDeprecated(t *testing.T) {
	pConf, err := oltpSpec().ParseYAML(`
http:
  - url: foo:123
  - url: foo:456
    secure: true
  - {}
grpc:
  - url: bar:123
  - url: bar:456
    secure: true
  - {}
sampling:
  enabled: true
  ratio: 0.55
`, nil)
	require.NoError(t, err)

	cConf, err := oltpConfigFromParsed(pConf)
	require.NoError(t, err)

	assert.True(t, cConf.sampling.enabled)
	assert.Equal(t, 0.55, cConf.sampling.ratio)

	require.Len(t, cConf.http, 3)
	assert.Equal(t, "foo:123", cConf.http[0].address)
	assert.False(t, cConf.http[0].secure)
	assert.Equal(t, "foo:456", cConf.http[1].address)
	assert.True(t, cConf.http[1].secure)
	assert.Equal(t, "localhost:4318", cConf.http[2].address)
	assert.False(t, cConf.http[2].secure)

	require.Len(t, cConf.grpc, 3)
	assert.Equal(t, "bar:123", cConf.grpc[0].address)
	assert.False(t, cConf.grpc[0].secure)
	assert.Equal(t, "bar:456", cConf.grpc[1].address)
	assert.True(t, cConf.grpc[1].secure)
	assert.Equal(t, "localhost:4317", cConf.grpc[2].address)
	assert.False(t, cConf.grpc[2].secure)
}
