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

package nats

import (
	"testing"
	"testing/fstest"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	NATSUserCreds = `-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJZMzMzT0c1SlFOVzZXU01DNUlMQjY0Uk5UR0hSRExBM1RTNFJGQ1JaMkU3NElYTzVBTU5BIiwiaWF0IjoxNjYxNzkzMjIxLCJpc3MiOiJBQTRJS1VNN0xVTlZLMlNUQ1lWN0lJWlZTWFdBWEhVUEE1RUI1SjNQQ0Y0V1pOSVFUSk1aMlpWTiIsIm5hbWUiOiJ0ZXN0Iiwic3ViIjoiVUE0RkxNRFQySVZNWEQ2SVZVRjRPRFk3UTRTSVBSU0kzVFRLN1ZMR0hFVFNDVUI0SEczQlRYWUUiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e30sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImlzc3Vlcl9hY2NvdW50IjoiQURJQjZKNk40SUNTVlZWWDMzRlc3U1FERlZaSEtLQlhJM05YUkYzWk41WEs1UDI3NVYyWFVKUU4iLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.o11HW6FXVDi8cTA2OcWzYZz3tfiFpDqRNlDEZM0nNg47klTfSBkDW9eTTUC_EsZfaEOpCcy1cafPmBo4vpw_AA
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUABRFVRZW4YPTRCQOFZKF45ISHYBPRXPUV7NHHZJVF3D3M2HLZLDKIJ2U
------END USER NKEY SEED------

*************************************************************`

	NATSUserJWT = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJZMzMzT0c1SlFOVzZXU01DNUlMQjY0Uk5UR0hSRExBM1RTNFJGQ1JaMkU3NElYTzVBTU5BIiwiaWF0IjoxNjYxNzkzMjIxLCJpc3MiOiJBQTRJS1VNN0xVTlZLMlNUQ1lWN0lJWlZTWFdBWEhVUEE1RUI1SjNQQ0Y0V1pOSVFUSk1aMlpWTiIsIm5hbWUiOiJ0ZXN0Iiwic3ViIjoiVUE0RkxNRFQySVZNWEQ2SVZVRjRPRFk3UTRTSVBSU0kzVFRLN1ZMR0hFVFNDVUI0SEczQlRYWUUiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e30sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImlzc3Vlcl9hY2NvdW50IjoiQURJQjZKNk40SUNTVlZWWDMzRlc3U1FERlZaSEtLQlhJM05YUkYzWk41WEs1UDI3NVYyWFVKUU4iLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.o11HW6FXVDi8cTA2OcWzYZz3tfiFpDqRNlDEZM0nNg47klTfSBkDW9eTTUC_EsZfaEOpCcy1cafPmBo4vpw_AA"
)

func TestNatsAuthConfToOptions(t *testing.T) {
	conf := authConfig{}
	conf.UserCredentialsFile = "user.creds"

	fs := fstest.MapFS{
		"user.creds": {
			Data: []byte(NATSUserCreds),
		},
	}

	options := &nats.Options{}
	optFns := authConfToOptions(conf, service.NewFS(fs))
	for _, fn := range optFns {
		err := fn(options)
		assert.NoError(t, err)
	}

	jwt, err := options.UserJWT()
	assert.NoError(t, err)
	assert.Equal(t, NATSUserJWT, jwt)

	nonce := []byte("that's noncense")
	kp, err := nkeys.ParseDecoratedNKey([]byte(NATSUserCreds))
	assert.NoError(t, err)

	sig, err := kp.Sign(nonce)
	assert.NoError(t, err)

	sigResult, err := options.SignatureCB(nonce)
	assert.NoError(t, err)

	assert.Equal(t, sig, sigResult)
}

func TestAuthFromParsedConfigFieldMapping(t *testing.T) {
	spec := service.NewConfigSpec().Fields(authFieldSpec())
	env := service.NewEnvironment()

	t.Run("nkey_file", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
auth:
  nkey_file: ./seed.nk
`, env)
		require.NoError(t, err)

		c, err := AuthFromParsedConfig(conf.Namespace("auth"))
		require.NoError(t, err)
		assert.Equal(t, "./seed.nk", c.NKeyFile)
		assert.Empty(t, c.NKey)
		assert.Empty(t, c.UserCredentialsFile)
		assert.Empty(t, c.UserJWT)
		assert.Empty(t, c.UserNkeySeed)
		assert.Empty(t, c.Token)
		assert.Empty(t, c.User)
		assert.Empty(t, c.Password)
	})

	t.Run("nkey", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
auth:
  nkey: UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4
`, env)
		require.NoError(t, err)

		c, err := AuthFromParsedConfig(conf.Namespace("auth"))
		require.NoError(t, err)
		assert.Empty(t, c.NKeyFile)
		assert.Equal(t, "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4", c.NKey)
		assert.Empty(t, c.UserCredentialsFile)
		assert.Empty(t, c.UserJWT)
		assert.Empty(t, c.UserNkeySeed)
		assert.Empty(t, c.Token)
		assert.Empty(t, c.User)
		assert.Empty(t, c.Password)
	})

	t.Run("user_credentials_file", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
auth:
  user_credentials_file: ./user.creds
`, env)
		require.NoError(t, err)

		c, err := AuthFromParsedConfig(conf.Namespace("auth"))
		require.NoError(t, err)
		assert.Empty(t, c.NKeyFile)
		assert.Empty(t, c.NKey)
		assert.Equal(t, "./user.creds", c.UserCredentialsFile)
		assert.Empty(t, c.UserJWT)
		assert.Empty(t, c.UserNkeySeed)
		assert.Empty(t, c.Token)
		assert.Empty(t, c.User)
		assert.Empty(t, c.Password)
	})

	t.Run("user_jwt and user_nkey_seed", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
auth:
  user_jwt: myjwt
  user_nkey_seed: myseed
`, env)
		require.NoError(t, err)

		c, err := AuthFromParsedConfig(conf.Namespace("auth"))
		require.NoError(t, err)
		assert.Empty(t, c.NKeyFile)
		assert.Empty(t, c.NKey)
		assert.Empty(t, c.UserCredentialsFile)
		assert.Equal(t, "myjwt", c.UserJWT)
		assert.Equal(t, "myseed", c.UserNkeySeed)
		assert.Empty(t, c.Token)
		assert.Empty(t, c.User)
		assert.Empty(t, c.Password)
	})

	t.Run("token", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
auth:
  token: mytoken
`, env)
		require.NoError(t, err)

		c, err := AuthFromParsedConfig(conf.Namespace("auth"))
		require.NoError(t, err)
		assert.Empty(t, c.NKeyFile)
		assert.Empty(t, c.NKey)
		assert.Empty(t, c.UserCredentialsFile)
		assert.Empty(t, c.UserJWT)
		assert.Empty(t, c.UserNkeySeed)
		assert.Equal(t, "mytoken", c.Token)
		assert.Empty(t, c.User)
		assert.Empty(t, c.Password)
	})

	t.Run("user and password", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
auth:
  user: myuser
  password: mypassword
`, env)
		require.NoError(t, err)

		c, err := AuthFromParsedConfig(conf.Namespace("auth"))
		require.NoError(t, err)
		assert.Empty(t, c.NKeyFile)
		assert.Empty(t, c.NKey)
		assert.Empty(t, c.UserCredentialsFile)
		assert.Empty(t, c.UserJWT)
		assert.Empty(t, c.UserNkeySeed)
		assert.Empty(t, c.Token)
		assert.Equal(t, "myuser", c.User)
		assert.Equal(t, "mypassword", c.Password)
	})

	t.Run("empty user with non-empty password", func(t *testing.T) {
		// NATS allows password-only auth; user can be empty.
		conf, err := spec.ParseYAML(`
auth:
  user: ""
  password: mypassword
`, env)
		require.NoError(t, err)

		c, err := AuthFromParsedConfig(conf.Namespace("auth"))
		require.NoError(t, err)
		assert.Empty(t, c.User)
		assert.Equal(t, "mypassword", c.Password)
	})

	t.Run("non-empty user with empty password", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
auth:
  user: myuser
  password: ""
`, env)
		require.NoError(t, err)

		c, err := AuthFromParsedConfig(conf.Namespace("auth"))
		require.NoError(t, err)
		assert.Equal(t, "myuser", c.User)
		assert.Empty(t, c.Password)
	})

	t.Run("both user and password empty rejects", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
auth:
  user: ""
  password: ""
`, env)
		require.NoError(t, err)

		_, err = AuthFromParsedConfig(conf.Namespace("auth"))
		require.ErrorContains(t, err, "auth.user and auth.password are both empty")
	})

	t.Run("no auth", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
auth: {}
`, env)
		require.NoError(t, err)

		c, err := AuthFromParsedConfig(conf.Namespace("auth"))
		require.NoError(t, err)
		assert.Empty(t, c.NKeyFile)
		assert.Empty(t, c.NKey)
		assert.Empty(t, c.UserCredentialsFile)
		assert.Empty(t, c.UserJWT)
		assert.Empty(t, c.UserNkeySeed)
		assert.Empty(t, c.Token)
		assert.Empty(t, c.User)
		assert.Empty(t, c.Password)
	})
}

func TestAuthFromParsedConfigMutualExclusion(t *testing.T) {
	spec := service.NewConfigSpec().Fields(authFieldSpec())
	env := service.NewEnvironment()

	tests := []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			name:    "token and user+password",
			wantErr: "multiple auth methods configured",
			config: `
auth:
  token: mytoken
  user: myuser
  password: mypassword
`,
		},
		{
			name:    "nkey_file and token",
			wantErr: "multiple auth methods configured",
			config: `
auth:
  nkey_file: ./seed.nk
  token: mytoken
`,
		},
		{
			name:    "user_credentials_file and user+password",
			wantErr: "multiple auth methods configured",
			config: `
auth:
  user_credentials_file: ./user.creds
  user: myuser
  password: mypassword
`,
		},
		{
			name:    "nkey and user_jwt+user_nkey_seed",
			wantErr: "multiple auth methods configured",
			config: `
auth:
  nkey: UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4
  user_jwt: myjwt
  user_nkey_seed: myseed
`,
		},
		{
			name:    "all methods configured",
			wantErr: "multiple auth methods configured",
			config: `
auth:
  nkey_file: ./seed.nk
  nkey: UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4
  user_credentials_file: ./user.creds
  user_jwt: myjwt
  user_nkey_seed: myseed
  token: mytoken
  user: myuser
  password: mypassword
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(tc.config, env)
			require.NoError(t, err)

			_, err = AuthFromParsedConfig(conf.Namespace("auth"))
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestAuthConfToOptionsUserPassword(t *testing.T) {
	t.Run("user with non-empty password applies UserInfo", func(t *testing.T) {
		conf := authConfig{User: "alice", Password: "s3cret"}
		opts := authConfToOptions(conf, service.NewFS(nil))
		assert.Len(t, opts, 1, "expected exactly one NATS option for user+password")
	})

	t.Run("user with empty password still applies UserInfo", func(t *testing.T) {
		conf := authConfig{User: "alice", Password: ""}
		opts := authConfToOptions(conf, service.NewFS(nil))
		assert.Len(t, opts, 1, "expected UserInfo option even with empty password")
	})

	t.Run("empty user with non-empty password applies UserInfo", func(t *testing.T) {
		// NATS allows password-only auth where user is empty.
		conf := authConfig{User: "", Password: "s3cret"}
		opts := authConfToOptions(conf, service.NewFS(nil))
		assert.Len(t, opts, 1, "expected UserInfo option even with empty user")
	})

	t.Run("no user no password produces no options", func(t *testing.T) {
		conf := authConfig{}
		opts := authConfToOptions(conf, service.NewFS(nil))
		assert.Empty(t, opts)
	})
}
