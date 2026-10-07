// Copyright 2026 Redpanda Data, Inc.
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

package opensearch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutputCompressRequestBody_DefaultsFalse(t *testing.T) {
	spec := OutputSpec()
	pConf, err := spec.ParseYAML(`
urls: [ "http://localhost:9200" ]
index: foo
id: '${!uuid_v4()}'
action: index
`, nil)
	require.NoError(t, err)

	conf, err := esoConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.False(t, conf.clientOpts.Client.CompressRequestBody,
		"compress_request_body should default to false to preserve existing behaviour")
}

func TestOutputCompressRequestBody_SetTrue(t *testing.T) {
	spec := OutputSpec()
	pConf, err := spec.ParseYAML(`
urls: [ "http://localhost:9200" ]
index: foo
id: '${!uuid_v4()}'
action: index
compress_request_body: true
`, nil)
	require.NoError(t, err)

	conf, err := esoConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.True(t, conf.clientOpts.Client.CompressRequestBody,
		"compress_request_body: true should propagate to the OpenSearch client Config")
}
