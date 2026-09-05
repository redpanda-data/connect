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

package kafka

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestOAuthSASLFromConfigOAuth2ClientCredentials(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, r.ParseForm())
		require.Equal(t, "client_credentials", r.PostForm.Get("grant_type"))

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(testServer.Close)

	mConf := parseFirstSASLObjectConfig(t, `
sasl:
  - mechanism: OAUTHBEARER
    oauth2:
      enabled: true
      client_key: test-client
      client_secret: test-secret
      token_url: `+testServer.URL+`
      scopes:
        - scope-a
      endpoint_params:
        audience: test-audience
`)

	mechanism, err := oauthSaslFromConfig(mConf)
	require.NoError(t, err)

	_, authBytes, err := mechanism.Authenticate(context.Background(), "localhost:9092")
	require.NoError(t, err)
	require.Contains(t, string(authBytes), "test-token")
}

func TestOAuthSASLFromConfigStaticTokenFallback(t *testing.T) {
	tests := []struct {
		name          string
		yaml          string
		expectedToken string
	}{
		{
			name: "oauth2 absent",
			yaml: `
sasl:
  - mechanism: OAUTHBEARER
    token: static-token-1
`,
			expectedToken: "static-token-1",
		},
		{
			name: "oauth2 disabled",
			yaml: `
sasl:
  - mechanism: OAUTHBEARER
    token: static-token-2
    oauth2:
      enabled: false
      client_key: ignored
      client_secret: ignored
      token_url: https://example.invalid/token
`,
			expectedToken: "static-token-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mConf := parseFirstSASLObjectConfig(t, tt.yaml)
			mechanism, err := oauthSaslFromConfig(mConf)
			require.NoError(t, err)

			_, authBytes, err := mechanism.Authenticate(context.Background(), "localhost:9092")
			require.NoError(t, err)
			require.Contains(t, string(authBytes), tt.expectedToken)
		})
	}
}

func parseFirstSASLObjectConfig(t *testing.T, rawYAML string) *service.ParsedConfig {
	t.Helper()

	spec := service.NewConfigSpec().Field(SASLFields())
	parsed, err := spec.ParseYAML(rawYAML, nil)
	require.NoError(t, err)

	saslObjs, err := parsed.FieldObjectList("sasl")
	require.NoError(t, err)
	require.Len(t, saslObjs, 1)

	return saslObjs[0]
}
