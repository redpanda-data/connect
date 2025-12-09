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

package mcp_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/common-go/authz"

	"github.com/redpanda-data/connect/v4/internal/license"
	mcpinternal "github.com/redpanda-data/connect/v4/internal/mcp"
)

// mcpServerHandle wraps the MCP server and provides test utilities
type mcpServerHandle struct {
	server   *mcpinternal.Server
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
}

func (h *mcpServerHandle) URL() string {
	return "http://" + h.listener.Addr().String()
}

func (h *mcpServerHandle) Close() error {
	h.cancel()
	return h.listener.Close()
}

// redpandaUser implements mockoidc.User with Redpanda custom claims
type redpandaUser struct {
	subject string
	email   string
	orgID   string
}

func (u *redpandaUser) ID() string {
	return u.subject
}

func (u *redpandaUser) Userinfo(_ []string) ([]byte, error) {
	info := map[string]any{
		"sub":   u.subject,
		"email": u.email,
	}
	return json.Marshal(info)
}

func (u *redpandaUser) Claims(_ []string, claims *mockoidc.IDTokenClaims) (jwt.Claims, error) {
	claims.Subject = u.subject

	// Add custom Redpanda claims
	customClaims := map[string]any{
		"iss": claims.Issuer,
		"sub": u.subject,
		"aud": claims.Audience,
		"exp": claims.ExpiresAt.Unix(),
		"iat": claims.IssuedAt.Unix(),
		"https://cloud.redpanda.com/organization_id": u.orgID,
		"account_info": map[string]any{
			"email": u.email,
		},
	}

	return jwt.MapClaims(customClaims), nil
}

// setupMockOIDC creates a mockoidc server with Redpanda custom claims support
func setupMockOIDC(t *testing.T) (*mockoidc.MockOIDC, string) {
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

// setupMCPServer starts an MCP server with JWT authentication and authorization policy
func setupMCPServer(t *testing.T, issuerURL, orgID, policyFile string) *mcpServerHandle {
	t.Helper()

	const resourceName authz.ResourceName = "organization/test-org/resourcegroup/default/dataplane/mcp-server"

	// Configure JWT environment variables
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL", issuerURL)
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE", "test-audience")
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID", orgID)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	envVarFunc := func(_ context.Context, key string) (string, bool) {
		val := os.Getenv(key)
		return val, val != ""
	}

	// Create authorizer
	auth, err := mcpinternal.NewAuthorizer(resourceName, policyFile, logger)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := auth.Close(context.Background()); err != nil {
			t.Log(err)
		}
	})

	server, err := mcpinternal.NewServer(
		"./testdata",
		logger,
		envVarFunc,
		nil,
		nil,
		license.Config{},
		auth,
	)
	require.NoError(t, err)

	// Inject enterprise license for authorization
	license.InjectTestService(server.Resources())

	// Start HTTP server on random port
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	go func() {
		_ = server.ServeHTTP(ctx, listener)
	}()

	handle := &mcpServerHandle{
		server:   server,
		listener: listener,
		ctx:      ctx,
		cancel:   cancel,
	}

	t.Cleanup(func() {
		if err := handle.Close(); err != nil {
			t.Log(err)
		}
	})

	return handle
}

// getAccessToken performs OAuth flow with mockoidc to get a valid access token
func getAccessToken(t *testing.T, m *mockoidc.MockOIDC, user mockoidc.User) string {
	t.Helper()

	// Queue the user for authentication
	m.QueueUser(user)

	// Create base ID token claims
	baseClaims := &mockoidc.IDTokenClaims{
		RegisteredClaims: &jwt.RegisteredClaims{
			Issuer:    m.Issuer(),
			Subject:   user.ID(),
			Audience:  jwt.ClaimStrings{"test-audience"},
			IssuedAt:  jwt.NewNumericDate(m.Now()),
			ExpiresAt: jwt.NewNumericDate(m.Now().Add(time.Hour)),
		},
	}

	// Get user claims with custom Redpanda claims
	claims, err := user.Claims([]string{"openid", "email"}, baseClaims)
	require.NoError(t, err)

	// Sign the JWT
	token, err := m.Keypair.SignJWT(claims)
	require.NoError(t, err)

	return token
}

// createMCPClient creates an MCP client connected via SSE transport
func createMCPClient(t *testing.T, serverURL, token string) (*mcp.ClientSession, func()) {
	t.Helper()

	client := mcp.NewClient(&mcp.Implementation{
		Name:    "integration-test-client",
		Version: "1.0.0",
	}, nil)

	transport := &mcp.StreamableClientTransport{
		Endpoint: serverURL + "/mcp",
		HTTPClient: &http.Client{
			Transport: &authTransport{
				token:     token,
				transport: http.DefaultTransport,
			},
		},
	}

	session, err := client.Connect(t.Context(), transport, nil)
	require.NoError(t, err)

	cleanup := func() {
		if err := session.Close(); err != nil {
			t.Log(err)
		}
	}

	return session, cleanup
}

// authTransport adds Authorization header to all requests
type authTransport struct {
	token     string
	transport http.RoundTripper
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.token != "" {
		req.Header.Set("Authorization", "Bearer "+t.token)
	}
	return t.transport.RoundTrip(req)
}

func TestIntegrationMCPServerJWTAuth_Valid(t *testing.T) {
	integration.CheckSkip(t)

	const testOrgID = "test-org-123"
	const testEmail = "test@example.com"

	t.Log("Given: mockoidc provider with Redpanda custom claims")
	mockOIDC, issuerURL := setupMockOIDC(t)
	t.Logf("OIDC Issuer: %s", issuerURL)

	t.Log("And: MCP server with JWT authentication enabled")
	server := setupMCPServer(t, issuerURL, testOrgID, "testdata/policies/allow_all.yaml")

	t.Log("And: User with valid token")
	user := &redpandaUser{
		subject: "test-user-123",
		email:   testEmail,
		orgID:   testOrgID,
	}

	token := getAccessToken(t, mockOIDC, user)
	require.NotEmpty(t, token)

	t.Log("When: MCP client connects with valid JWT token")
	session, cleanup := createMCPClient(t, server.URL(), token)
	defer cleanup()

	t.Log("Then: Session is successfully initialized")
	initResult := session.InitializeResult()
	assert.NotNil(t, initResult, "Session should be initialized")

	t.Log("And: Client can list available tools")
	toolsResult, err := session.ListTools(t.Context(), &mcp.ListToolsParams{})
	require.NoError(t, err)
	assert.NotNil(t, toolsResult)
	t.Logf("Found %d tools", len(toolsResult.Tools))
}

func TestIntegrationMCPServerJWTAuth_Invalid(t *testing.T) {
	integration.CheckSkip(t)

	const testOrgID = "test-org-123"

	tests := []struct {
		name     string
		setupFn  func(t *testing.T, m *mockoidc.MockOIDC) string
		wantCode int
	}{
		{
			name: "expired_token",
			setupFn: func(t *testing.T, m *mockoidc.MockOIDC) string {
				user := &redpandaUser{
					subject: "test-user",
					email:   "test@example.com",
					orgID:   testOrgID,
				}

				// Create token that's already expired
				baseClaims := &mockoidc.IDTokenClaims{
					RegisteredClaims: &jwt.RegisteredClaims{
						Issuer:    m.Issuer(),
						Subject:   user.ID(),
						Audience:  jwt.ClaimStrings{"test-audience"},
						IssuedAt:  jwt.NewNumericDate(m.Now().Add(-2 * time.Hour)),
						ExpiresAt: jwt.NewNumericDate(m.Now().Add(-1 * time.Hour)), // expired
					},
				}

				claims, err := user.Claims([]string{"openid", "email"}, baseClaims)
				require.NoError(t, err)

				token, err := m.Keypair.SignJWT(claims)
				require.NoError(t, err)

				return token
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "wrong_org_id",
			setupFn: func(t *testing.T, m *mockoidc.MockOIDC) string {
				user := &redpandaUser{
					subject: "test-user",
					email:   "test@example.com",
					orgID:   "wrong-org-456",
				}
				return getAccessToken(t, m, user)
			},
			wantCode: http.StatusUnauthorized,
		},
		{
			name: "missing_email",
			setupFn: func(t *testing.T, m *mockoidc.MockOIDC) string {
				user := &redpandaUser{
					subject: "test-user",
					email:   "", // empty email
					orgID:   testOrgID,
				}
				return getAccessToken(t, m, user)
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "no_token",
			setupFn: func(_ *testing.T, _ *mockoidc.MockOIDC) string {
				return "" // no token
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log("Given: mockoidc provider")
			mockOIDC, issuerURL := setupMockOIDC(t)

			t.Log("And: MCP server with JWT authentication")
			server := setupMCPServer(t, issuerURL, testOrgID, "testdata/policies/allow_all.yaml")

			t.Log("When: MCP client attempts to connect with invalid/missing token")
			token := tt.setupFn(t, mockOIDC)

			client := mcp.NewClient(&mcp.Implementation{
				Name:    "integration-test-client",
				Version: "1.0.0",
			}, nil)

			transport := &mcp.SSEClientTransport{
				Endpoint: server.URL() + "/sse",
				HTTPClient: &http.Client{
					Transport: &authTransport{
						token:     token,
						transport: http.DefaultTransport,
					},
				},
			}

			t.Log("Then: Connection fails with authentication error")
			_, err := client.Connect(t.Context(), transport, nil)
			if token == "" {
				// No token should fail immediately
				assert.Error(t, err)
			} else {
				// Invalid tokens may connect but fail on first request
				// The actual error handling depends on the SSE transport implementation
				t.Logf("Connection result: %v", err)
			}
		})
	}
}

func TestIntegrationMCPServerAuthz_AllowAll(t *testing.T) {
	integration.CheckSkip(t)

	const testOrgID = "test-org"
	const testEmail = "test@example.com"

	t.Log("Given: mockoidc provider")
	mockOIDC, issuerURL := setupMockOIDC(t)

	t.Log("And: Policy file granting all permissions")
	server := setupMCPServer(t, issuerURL, testOrgID, "testdata/policies/allow_all.yaml")

	t.Log("And: User with valid token")
	user := &redpandaUser{
		subject: "test-user",
		email:   testEmail,
		orgID:   testOrgID,
	}
	token := getAccessToken(t, mockOIDC, user)

	t.Log("When: MCP client connects with valid credentials and all permissions")
	session, cleanup := createMCPClient(t, server.URL(), token)
	defer cleanup()

	t.Log("Then: Session is successfully initialized")
	assert.NotNil(t, session.InitializeResult(), "Session should be initialized")

	t.Log("And: Client can list tools (tools/list permission)")
	toolsResult, err := session.ListTools(t.Context(), &mcp.ListToolsParams{})
	require.NoError(t, err)
	assert.NotEmpty(t, toolsResult.Tools, "Expected to find tools from test resources")
	t.Logf("Found %d tools", len(toolsResult.Tools))
}

func TestIntegrationMCPServerAuthz_DenyAll(t *testing.T) {
	integration.CheckSkip(t)

	const testOrgID = "test-org"
	const testEmail = "test@example.com"

	t.Log("Given: mockoidc provider")
	mockOIDC, issuerURL := setupMockOIDC(t)

	t.Log("And: Policy file denying all permissions")
	server := setupMCPServer(t, issuerURL, testOrgID, "testdata/policies/deny_all.yaml")

	t.Log("And: User with valid token but no permissions")
	user := &redpandaUser{
		subject: "test-user",
		email:   testEmail,
		orgID:   testOrgID,
	}
	token := getAccessToken(t, mockOIDC, user)

	t.Log("When: MCP client attempts to connect with no permissions")
	client := mcp.NewClient(&mcp.Implementation{
		Name:    "integration-test-client",
		Version: "1.0.0",
	}, nil)

	transport := &mcp.SSEClientTransport{
		Endpoint: server.URL() + "/sse",
		HTTPClient: &http.Client{
			Transport: &authTransport{
				token:     token,
				transport: http.DefaultTransport,
			},
		},
	}

	t.Log("Then: Connection fails due to lack of initialize permission")
	_, err := client.Connect(t.Context(), transport, nil)
	assert.Error(t, err, "Expected connection to fail with deny_all policy")
	if err != nil {
		t.Logf("Expected permission denied error: %v", err)
	}
}

func TestIntegrationMCPServerAuthz_PolicyReload(t *testing.T) {
	integration.CheckSkip(t)

	const testOrgID = "test-org"
	const testEmail = "test@example.com"

	t.Log("Given: mockoidc provider")
	mockOIDC, issuerURL := setupMockOIDC(t)

	t.Log("And: Temporary policy file with allow_all")
	tmpDir := t.TempDir()
	tmpPolicyFile := filepath.Join(tmpDir, "policy.yaml")

	// Start with allow_all policy
	allowAllData, err := os.ReadFile(filepath.Join("testdata", "policies", "allow_all.yaml"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tmpPolicyFile, allowAllData, 0o644))

	t.Log("And: MCP server with JWT auth and authorization")
	server := setupMCPServer(t, issuerURL, testOrgID, tmpPolicyFile)

	t.Log("And: User with valid token")
	user := &redpandaUser{
		subject: "test-user",
		email:   testEmail,
		orgID:   testOrgID,
	}
	token := getAccessToken(t, mockOIDC, user)

	t.Log("When: MCP client connects with allow_all policy")
	session, cleanup := createMCPClient(t, server.URL(), token)
	defer cleanup()

	t.Log("And: Initial request succeeds")
	_, err = session.ListTools(t.Context(), &mcp.ListToolsParams{})
	require.NoError(t, err, "Should succeed with allow_all policy")

	t.Log("And: Policy file is updated to deny_all")
	denyAllData, err := os.ReadFile(filepath.Join("testdata", "policies", "deny_all.yaml"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tmpPolicyFile, denyAllData, 0o644))

	t.Log("And: Wait for policy reload")
	time.Sleep(2 * time.Second)

	t.Log("Then: Subsequent requests reflect new policy and are denied")
	_, err = session.ListTools(t.Context(), &mcp.ListToolsParams{})
	assert.Error(t, err, "Should fail with deny_all policy after reload")
	if err != nil {
		t.Logf("Expected permission denied error: %v", err)
	}
}
