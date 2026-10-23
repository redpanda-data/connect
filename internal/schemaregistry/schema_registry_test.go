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

package schemaregistry

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const testSecret = "S3cretPass"

func parseClientConfig(t *testing.T, yaml string) *service.ParsedConfig {
	t.Helper()
	pConf, err := service.NewConfigSpec().Fields(ConfigFields()...).ParseYAML(yaml, nil)
	require.NoError(t, err)
	return pConf
}

func TestClientFromParsedURLCredentials(t *testing.T) {
	var gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	userURL := strings.Replace(srv.URL, "://", "://u:"+testSecret+"@", 1)

	cl, _, err := ClientFromParsed(parseClientConfig(t, `url: `+userURL), service.MockResources())
	require.NoError(t, err)
	_, err = cl.Subjects(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "u", gotUser)
	assert.Equal(t, testSecret, gotPass)

	// Explicit basic auth takes precedence over URL credentials.
	cl, _, err = ClientFromParsed(parseClientConfig(t, `
url: `+userURL+`
basic_auth:
  enabled: true
  username: explicit
  password: explicitpass
`), service.MockResources())
	require.NoError(t, err)
	_, err = cl.Subjects(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "explicit", gotUser)
	assert.Equal(t, "explicitpass", gotPass)

	// Request errors include the URL, which must not hold the credentials.
	srv.Close()
	_, err = cl.Subjects(t.Context())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), testSecret)
}

func TestClientFromParsedBadURLRedacted(t *testing.T) {
	_, _, err := ClientFromParsed(parseClientConfig(t, `url: 'http://u:`+testSecret+`@bad host:8081'`), service.MockResources())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), testSecret)
}

func TestBasicAuthFromURL(t *testing.T) {
	u, err := url.Parse("http://u:p@h:8081/ctx")
	require.NoError(t, err)

	got, opts := BasicAuthFromURL(u)
	assert.Equal(t, "http://h:8081/ctx", got.String())
	assert.Len(t, opts, 1)
	assert.Equal(t, "http://u:p@h:8081/ctx", u.String(), "input must not be modified")

	u, err = url.Parse("http://h:8081")
	require.NoError(t, err)
	got, opts = BasicAuthFromURL(u)
	assert.Same(t, u, got)
	assert.Empty(t, opts)
}
