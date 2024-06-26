// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
