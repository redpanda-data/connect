// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elasticsearch

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestConnectErrorRedactsURL(t *testing.T) {
	pConf, err := elasticsearchConfigSpec().ParseYAML(`
urls: [ "http://u:S3cretPass@bad host:9200" ]
index: foo
id: bar
action: index
`, nil)
	require.NoError(t, err)

	out, err := outputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	err = out.Connect(context.Background())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")

	res := out.ConnectionTest(context.Background())
	require.Len(t, res, 1)
	require.Error(t, res[0].Err)
	assert.NotContains(t, res[0].Err.Error(), "S3cretPass")
}

func TestWriteBatchErrorRedactsURL(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	pConf, err := elasticsearchConfigSpec().ParseYAML(`
urls: [ "http://u:S3cretPass@`+addr+`?token=S3cretToken" ]
index: foo
id: bar
action: index
`, nil)
	require.NoError(t, err)

	out, err := outputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	err = out.WriteBatch(t.Context(), service.MessageBatch{service.NewMessage([]byte(`{"a":1}`))})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sending bulk request")
	assert.NotContains(t, err.Error(), "S3cretPass")
	assert.NotContains(t, err.Error(), "S3cretToken")
}
