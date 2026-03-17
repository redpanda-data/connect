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

// Package gatewaytest provides test utilities for gateway components.
package gatewaytest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/require"
)

// RedpandaUser implements mockoidc.User with Redpanda custom claims.
type RedpandaUser struct {
	Subject string
	Email   string
	OrgID   string
}

// ID returns the user's subject identifier.
func (u *RedpandaUser) ID() string {
	return u.Subject
}

// Userinfo returns the user info claims as JSON.
func (u *RedpandaUser) Userinfo(_ []string) ([]byte, error) {
	info := map[string]any{
		"sub":   u.Subject,
		"email": u.Email,
	}
	return json.Marshal(info)
}

// Claims returns JWT claims with Redpanda custom claims.
func (u *RedpandaUser) Claims(_ []string, claims *mockoidc.IDTokenClaims) (jwt.Claims, error) {
	claims.Subject = u.Subject

	cc := map[string]any{
		"iss": claims.Issuer,
		"sub": u.Subject,
		"aud": claims.Audience,
		"exp": claims.ExpiresAt.Unix(),
		"iat": claims.IssuedAt.Unix(),
		"https://cloud.redpanda.com/organization_id": u.OrgID,
		"account_info": map[string]any{
			"email": u.Email,
		},
	}
	return jwt.MapClaims(cc), nil
}

// SetupMockOIDC creates a mockoidc server with Redpanda custom claims support.
// The server is automatically shut down when the test completes.
func SetupMockOIDC(t *testing.T) (*mockoidc.MockOIDC, string) {
	t.Helper()

	m, err := mockoidc.Run()
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := m.Shutdown(); err != nil {
			t.Log(err)
		}
	})

	return m, m.Issuer()
}

// AccessToken performs OAuth flow with mockoidc to get a valid access token.
func AccessToken(t *testing.T, m *mockoidc.MockOIDC, user mockoidc.User) string {
	t.Helper()

	m.QueueUser(user)
	claims, err := user.Claims([]string{"openid", "email"}, &mockoidc.IDTokenClaims{
		RegisteredClaims: &jwt.RegisteredClaims{
			Issuer:    m.Issuer(),
			Subject:   user.ID(),
			Audience:  jwt.ClaimStrings{"test-audience"},
			IssuedAt:  jwt.NewNumericDate(m.Now()),
			ExpiresAt: jwt.NewNumericDate(m.Now().Add(time.Hour)),
		},
	})
	require.NoError(t, err)

	token, err := m.Keypair.SignJWT(claims)
	require.NoError(t, err)

	return token
}
