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
	"testing"

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
