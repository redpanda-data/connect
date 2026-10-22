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

package aws

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/mongodb"
)

func parseAWSConf(t *testing.T, yamlStr string) *service.ParsedConfig {
	t.Helper()
	spec := service.NewConfigSpec().Field(mongodb.AWSIAMAuthField())
	conf, err := spec.ParseYAML(yamlStr, service.NewEnvironment())
	require.NoError(t, err)
	return conf.Namespace(mongodb.FieldAWSIAMAuth)
}

func TestDisabledReturnsNilBuilder(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: false
`)
	builder, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
	require.NoError(t, err)
	require.Nil(t, builder)
}

func TestAmbientChainCredential(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
`)
	builder, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
	require.NoError(t, err)
	require.NotNil(t, builder)

	cred, err := builder(t.Context())
	require.NoError(t, err)
	require.NotNil(t, cred)
	assert.Equal(t, "MONGODB-AWS", cred.AuthMechanism)
	assert.Empty(t, cred.Username)
	assert.Empty(t, cred.Password)
	assert.Nil(t, cred.AuthMechanismProperties)
}

func TestStaticKeysCredential(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
  id: AKIAEXAMPLE
  secret: supersecret
  token: sessiontoken
`)
	builder, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
	require.NoError(t, err)
	require.NotNil(t, builder)

	cred, err := builder(t.Context())
	require.NoError(t, err)
	require.NotNil(t, cred)
	assert.Equal(t, "MONGODB-AWS", cred.AuthMechanism)
	assert.Equal(t, "AKIAEXAMPLE", cred.Username)
	assert.Equal(t, "supersecret", cred.Password)
	assert.Equal(t, map[string]string{"AWS_SESSION_TOKEN": "sessiontoken"}, cred.AuthMechanismProperties)
}

func TestStaticBuilderIsStable(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
  id: AKIAEXAMPLE
  secret: supersecret
`)
	builder, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
	require.NoError(t, err)
	require.NotNil(t, builder)

	first, err := builder(t.Context())
	require.NoError(t, err)
	second, err := builder(t.Context())
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

func TestPartialStaticCredentialsRejected(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
  id: AKIAEXAMPLE
`)
	_, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
	require.ErrorContains(t, err, "aws.id and aws.secret must both be set")
}

func TestSecretWithoutIDRejected(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
  secret: supersecret
`)
	_, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
	require.ErrorContains(t, err, "aws.id and aws.secret must both be set")
}

func TestTokenWithoutIDAndSecretRejected(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
  token: sessiontoken
`)
	_, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
	require.ErrorContains(t, err, "aws.token requires aws.id and aws.secret")
}

func TestRolesRequireARN(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
  roles:
    - role_external_id: eid
`)
	_, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
	require.ErrorContains(t, err, "roles[0].role is required")
}

func TestRoleAndRolesMutuallyExclusive(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
  role: arn:aws:iam::111111111111:role/first
  roles:
    - role: arn:aws:iam::222222222222:role/second
`)
	_, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
	require.ErrorContains(t, err, "aws.role and aws.roles cannot both be set")
}

func TestTooShortSessionDurationRejected(t *testing.T) {
	for _, duration := range []string{"0s", "5m"} {
		t.Run(duration, func(t *testing.T) {
			awsConf := parseAWSConf(t, `
aws:
  enabled: true
  role: arn:aws:iam::111111111111:role/first
  session_duration: `+duration+`
`)
			_, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
			require.ErrorContains(t, err, "aws.session_duration must be at least 15m")
		})
	}
}

func TestSessionDurationIgnoredWithoutRoles(t *testing.T) {
	// session_duration only applies to the role paths, so an out of range value
	// must not fail a config which never assumes a role.
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
  session_duration: 0s
`)
	builder, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
	require.NoError(t, err)
	require.NotNil(t, builder)

	cred, err := builder(t.Context())
	require.NoError(t, err)
	require.NotNil(t, cred)
	assert.Equal(t, "MONGODB-AWS", cred.AuthMechanism)
}

func TestParseRoleConfigsOrdering(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
  roles:
    - role: arn:aws:iam::222222222222:role/second
    - role: arn:aws:iam::333333333333:role/third
      role_external_id: ext3
`)
	roles, err := parseRoleConfigs(awsConf)
	require.NoError(t, err)
	assert.Equal(t, []roleConfig{
		{arn: "arn:aws:iam::222222222222:role/second", externalID: ""},
		{arn: "arn:aws:iam::333333333333:role/third", externalID: "ext3"},
	}, roles)
}

func TestParseRoleConfigsSingleRole(t *testing.T) {
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
  role: arn:aws:iam::111111111111:role/first
  role_external_id: ext1
`)
	roles, err := parseRoleConfigs(awsConf)
	require.NoError(t, err)
	assert.Equal(t, []roleConfig{
		{arn: "arn:aws:iam::111111111111:role/first", externalID: "ext1"},
	}, roles)
}

// assumeRoleCall is what the STS stub observed for one AssumeRole request. The
// signing key matters as much as the parameters: it is the only evidence that
// hop N authenticated with the credentials minted by hop N-1.
type assumeRoleCall struct {
	roleARN      string
	externalID   string
	duration     string
	signingKeyID string
}

// stsStub speaks just enough of the STS query protocol to serve AssumeRole,
// minting credentials derived from the role name so the caller can tell which
// hop produced them.
type stsStub struct {
	mu    sync.Mutex
	calls []assumeRoleCall
	// denyRole, when non-empty, makes that role ARN fail with a 403
	// AccessDenied, as an unauthorised chain hop would.
	denyRole string
}

const stsXMLNS = "https://sts.amazonaws.com/doc/2011-06-15/"

func (s *stsStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "unparseable form: "+err.Error(), http.StatusBadRequest)
		return
	}
	if action := r.PostForm.Get("Action"); action != "AssumeRole" {
		http.Error(w, "unexpected action "+action, http.StatusBadRequest)
		return
	}
	roleARN := r.PostForm.Get("RoleArn")

	s.mu.Lock()
	s.calls = append(s.calls, assumeRoleCall{
		roleARN:      roleARN,
		externalID:   r.PostForm.Get("ExternalId"),
		duration:     r.PostForm.Get("DurationSeconds"),
		signingKeyID: sigV4AccessKeyID(r.Header.Get("Authorization")),
	})
	deny := s.denyRole != "" && s.denyRole == roleARN
	s.mu.Unlock()

	w.Header().Set("Content-Type", "text/xml")
	if deny {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `<ErrorResponse xmlns="`+stsXMLNS+`">
  <Error>
    <Type>Sender</Type>
    <Code>AccessDenied</Code>
    <Message>User is not authorized to perform: sts:AssumeRole on resource: `+roleARN+`</Message>
  </Error>
  <RequestId>deny-request</RequestId>
</ErrorResponse>`)
		return
	}

	name := roleARN[strings.LastIndex(roleARN, "/")+1:]
	_, _ = io.WriteString(w, fmt.Sprintf(`<AssumeRoleResponse xmlns="%s">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>AKID-FOR-%s</AccessKeyId>
      <SecretAccessKey>SECRET-FOR-%s</SecretAccessKey>
      <SessionToken>TOKEN-FOR-%s</SessionToken>
      <Expiration>%s</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>%s/session</Arn>
      <AssumedRoleId>AROA:%s</AssumedRoleId>
    </AssumedRoleUser>
  </AssumeRoleResult>
  <ResponseMetadata>
    <RequestId>stub-request</RequestId>
  </ResponseMetadata>
</AssumeRoleResponse>`, stsXMLNS, name, name, name, time.Now().Add(time.Hour).UTC().Format(time.RFC3339), roleARN, name))
}

func (s *stsStub) observed() []assumeRoleCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]assumeRoleCall(nil), s.calls...)
}

// sigV4AccessKeyID pulls the access key out of `AWS4-HMAC-SHA256
// Credential=<key>/<date>/<region>/sts/aws4_request, SignedHeaders=..., ...`.
func sigV4AccessKeyID(authorization string) string {
	_, rest, ok := strings.Cut(authorization, "Credential=")
	if !ok {
		return ""
	}
	key, _, _ := strings.Cut(rest, "/")
	return key
}

// startSTSStub serves the stub and points the AWS SDK, plus the base credentials
// the chain starts from, at it for the duration of the test.
// AWS_ENDPOINT_URL_STS is the SDK's service-specific endpoint override, which
// keeps the whole chain hermetic without a seam in the production code.
func startSTSStub(t *testing.T, denyRole string) *stsStub {
	t.Helper()
	stub := &stsStub{denyRole: denyRole}
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)

	t.Setenv("AWS_ENDPOINT_URL_STS", srv.URL)
	t.Setenv("AWS_ACCESS_KEY_ID", "BASE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "base-secret")
	t.Setenv("AWS_REGION", "us-east-1")
	// Nothing here may reach the instance metadata service, and an ambient
	// profile or credentials file must not join the chain.
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials"))
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "config"))
	return stub
}

const (
	firstRoleARN  = "arn:aws:iam::111111111111:role/first"
	secondRoleARN = "arn:aws:iam::222222222222:role/second"

	chainConf = `
aws:
  enabled: true
  roles:
    - role: ` + firstRoleARN + `
    - role: ` + secondRoleARN + `
      role_external_id: ext-2
  session_duration: 30m
`
)

// TestRoleChainAgainstStubbedSTS drives the role-assumption path end to end
// against a stubbed STS, which is the only way to observe the chain's shape: the
// SDK's credentials cache is lazy, so every AssumeRole call happens inside the
// builder's Retrieve.
func TestRoleChainAgainstStubbedSTS(t *testing.T) {
	t.Run("chains through both roles", func(t *testing.T) {
		stub := startSTSStub(t, "")
		awsConf := parseAWSConf(t, chainConf)
		builder, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
		require.NoError(t, err)
		require.NotNil(t, builder)

		cred, err := builder(t.Context())
		require.NoError(t, err)
		require.NotNil(t, cred)

		// The credential handed to the driver is the last hop's, not an
		// intermediate one's.
		assert.Equal(t, "MONGODB-AWS", cred.AuthMechanism)
		assert.Equal(t, "AKID-FOR-second", cred.Username)
		assert.Equal(t, "SECRET-FOR-second", cred.Password)
		assert.Equal(t, map[string]string{"AWS_SESSION_TOKEN": "TOKEN-FOR-second"}, cred.AuthMechanismProperties)

		calls := stub.observed()
		require.Len(t, calls, 2, "expected exactly one AssumeRole per configured role: %+v", calls)

		// Config order is chain order, and each hop signs with the previous
		// hop's freshly minted key - the base keys are only good for the first.
		assert.Equal(t, firstRoleARN, calls[0].roleARN)
		assert.Equal(t, "BASE", calls[0].signingKeyID)
		assert.Equal(t, secondRoleARN, calls[1].roleARN)
		assert.Equal(t, "AKID-FOR-first", calls[1].signingKeyID)

		// External IDs belong to the hop that configured them, and
		// session_duration reaches every hop.
		assert.Empty(t, calls[0].externalID, "no external ID was configured for the first role")
		assert.Equal(t, "ext-2", calls[1].externalID)
		assert.Equal(t, "1800", calls[0].duration)
		assert.Equal(t, "1800", calls[1].duration)
	})

	t.Run("a rejected hop fails the builder", func(t *testing.T) {
		stub := startSTSStub(t, secondRoleARN)
		awsConf := parseAWSConf(t, chainConf)
		builder, err := awsIAMCredentials(awsConf, service.MockResources().Logger())
		require.NoError(t, err)
		require.NotNil(t, builder)

		cred, err := builder(t.Context())
		require.Nil(t, cred)
		require.ErrorContains(t, err, "retrieving assumed-role credentials")
		require.ErrorContains(t, err, "AccessDenied")

		// The failure is the second hop's: the first still had to succeed to
		// produce the credentials it was attempted with.
		calls := stub.observed()
		require.Len(t, calls, 2, "the denied hop must be reached, not short-circuited: %+v", calls)
		assert.Equal(t, secondRoleARN, calls[1].roleARN)
		assert.Equal(t, "AKID-FOR-first", calls[1].signingKeyID)
	})
}

func TestOptFnRegistered(t *testing.T) {
	// Importing this package must replace the not-imported stub.
	awsConf := parseAWSConf(t, `
aws:
  enabled: true
`)
	builder, err := mongodb.AWSOptFn(awsConf, service.MockResources().Logger())
	require.NoError(t, err)
	require.NotNil(t, builder)

	cred, err := builder(t.Context())
	require.NoError(t, err)
	require.NotNil(t, cred)
	assert.Equal(t, "MONGODB-AWS", cred.AuthMechanism)
}
