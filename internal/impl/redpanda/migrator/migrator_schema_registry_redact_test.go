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

package migrator

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

const testSecret = "S3cretPass"

func parseSchemaRegistryClient(t *testing.T, yaml string) (*sr.Client, string) {
	t.Helper()
	spec := service.NewConfigSpec().Field(schemaRegistryField())
	pConf, err := spec.ParseYAML(yaml, nil)
	require.NoError(t, err)
	cl, u, err := schemaRegistryClientAndURLFromParsed(pConf, service.MockResources())
	require.NoError(t, err)
	return cl, u
}

func TestSchemaRegistryClientURLCredentials(t *testing.T) {
	var gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	userURL := strings.Replace(srv.URL, "://", "://u:"+testSecret+"@", 1)

	cl, u := parseSchemaRegistryClient(t, `
schema_registry:
  url: `+userURL+`
`)
	assert.NotContains(t, u, testSecret)
	_, err := cl.Subjects(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "u", gotUser)
	assert.Equal(t, testSecret, gotPass)

	// Explicit basic auth takes precedence over URL credentials.
	cl, _ = parseSchemaRegistryClient(t, `
schema_registry:
  url: `+userURL+`
  basic_auth:
    enabled: true
    username: explicit
    password: explicitpass
`)
	_, err = cl.Subjects(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "explicit", gotUser)
	assert.Equal(t, "explicitpass", gotPass)

	srv.Close()
	_, err = cl.Subjects(t.Context())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), testSecret)
}

func TestSchemaRegistryClientBadURLRedacted(t *testing.T) {
	spec := service.NewConfigSpec().Field(schemaRegistryField())
	pConf, err := spec.ParseYAML(`
schema_registry:
  url: 'http://u:`+testSecret+`@bad host:8081'
`, nil)
	require.NoError(t, err)
	_, _, err = schemaRegistryClientAndURLFromParsed(pConf, service.MockResources())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), testSecret)
}

func TestValidateSchemaRegistriesIgnoresCredentials(t *testing.T) {
	src, srcURL := parseSchemaRegistryClient(t, `
schema_registry:
  url: http://src:`+testSecret+`@registry:8081
`)
	dst, dstURL := parseSchemaRegistryClient(t, `
schema_registry:
  url: http://dst:other`+testSecret+`@registry:8081
`)
	m := &schemaRegistryMigrator{src: src, srcURL: srcURL, dst: dst, dstURL: dstURL}
	err := m.validateSchemaRegistries(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be different")
	assert.NotContains(t, err.Error(), testSecret)
}
