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

package nsq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestLookupdErrorRedactsCredentials(t *testing.T) {
	conf, err := inputConfigSpec().ParseYAML(`
nsqd_tcp_addresses: []
lookupd_http_addresses: [ "http://u:S3cretPass@bad host:4161" ]
topic: foo
channel: bar
`, nil)
	require.NoError(t, err)

	r, err := newNSQReaderFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	err = r.Connect(t.Context())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")

	for _, res := range r.ConnectionTest(t.Context()) {
		require.Error(t, res.Err)
		assert.NotContains(t, res.Err.Error(), "S3cretPass")
	}
}
