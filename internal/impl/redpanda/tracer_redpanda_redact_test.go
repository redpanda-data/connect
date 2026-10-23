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

package redpanda

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const tracerTestSecret = "S3cretPass"

func TestTracerSchemaRegistryURLCredentials(t *testing.T) {
	var gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	pConf, err := tracerSpec().ParseYAML(`
seed_brokers: [ localhost:9092 ]
schema_registry:
  url: `+strings.Replace(srv.URL, "://", "://u:"+tracerTestSecret+"@", 1)+`
`, nil)
	require.NoError(t, err)

	tr, err := tracerConfigFromParsed(pConf, service.MockResources().Logger())
	require.NoError(t, err)
	assert.Nil(t, tr.srURL.User)
	assert.NotContains(t, tr.srURL.String(), tracerTestSecret)

	cl, err := sr.NewClient(append([]sr.ClientOpt{sr.URLs(tr.srURL.String())}, tr.srOpts...)...)
	require.NoError(t, err)
	_, err = cl.Subjects(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "u", gotUser)
	assert.Equal(t, tracerTestSecret, gotPass)
}

func TestTracerSchemaRegistryBadURLRedacted(t *testing.T) {
	pConf, err := tracerSpec().ParseYAML(`
seed_brokers: [ localhost:9092 ]
schema_registry:
  url: 'http://u:`+tracerTestSecret+`@bad host:8081'
`, nil)
	require.NoError(t, err)

	_, err = tracerConfigFromParsed(pConf, service.MockResources().Logger())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), tracerTestSecret)
}
