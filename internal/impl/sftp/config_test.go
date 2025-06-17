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

package sftp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestAuthConfigParse(t *testing.T) {
	spec := service.NewConfigSpec().Fields(connectionFields()...)
	env := service.NewEnvironment()

	tests := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name: "valid config",
			conf: `
address: localhost:22
credentials:
  username: blobfish
  password: secret
  host_public_key: ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDknETovnNcLdtMzYk3qj9qGmRh0NkS6i4uGc3jtBdmK
`,
		},
		{
			name: "missing credentials",
			conf: `
address: localhost:22
`,
			errContains: "at least one authentication method must be provided",
		},
		{
			name: "conflicting host public key fields",
			conf: `
address: localhost:22
credentials:
  username: blobfish
  password: secret
  host_public_key: ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDknETovnNcLdtMzYk3qj9qGmRh0NkS6i4uGc3jtBdmK
  host_public_key_file: /path/to/public/key
`,
			errContains: `failed to get host public key: both "host_public_key" and "host_public_key_file" cannot be set simultaneously`,
		},
		{
			name: "conflicting private key fields",
			conf: `
address: localhost:22
credentials:
  username: blobfish
  password: secret
  host_public_key: ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDknETovnNcLdtMzYk3qj9qGmRh0NkS6i4uGc3jtBdmK
  private_key: supersecretkey
  private_key_file: /path/to/private/key
`,
			errContains: `failed to get private key: both "private_key" and "private_key_file" cannot be set simultaneously`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(test.conf, env)
			require.NoError(t, err)

			_, err = sshAuthConfigFromParsed(pConf.Namespace(sFieldCredentials), service.MockResources())
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigLinting(t *testing.T) {
	linter := service.NewEnvironment().NewComponentConfigLinter()

	tests := []struct {
		name    string
		conf    string
		lintErr string
	}{
		{
			name: "valid config",
			conf: `
sftp:
  address: localhost:22
  credentials:
    username: blobfish
    password: secret
    host_public_key: ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDknETovnNcLdtMzYk3qj9qGmRh0NkS6i4uGc3jtBdmK
    private_key: supersecretkey
`,
		},
		{
			name: "conflicting host public key fields",
			conf: `
sftp:
  address: localhost:22
  credentials:
    username: blobfish
    password: secret
    host_public_key: ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDknETovnNcLdtMzYk3qj9qGmRh0NkS6i4uGc3jtBdmK
    host_public_key_file: /path/to/public/key
    private_key: supersecretkey
`,
			lintErr: `(5,1) both host_public_key and host_public_key_file can't be set simultaneously`,
		},
		{
			name: "conflicting private key fields",
			conf: `
sftp:
  address: localhost:22
  credentials:
    username: blobfish
    password: secret
    host_public_key: ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDknETovnNcLdtMzYk3qj9qGmRh0NkS6i4uGc3jtBdmK
    private_key: supersecretkey
    private_key_file: /path/to/private/key
`,
			lintErr: `(5,1) both private_key and private_key_file can't be set simultaneously`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lints, err := linter.LintInputYAML([]byte(test.conf))
			require.NoError(t, err)
			if test.lintErr != "" {
				assert.Len(t, lints, 1)
				assert.Equal(t, test.lintErr, lints[0].Error())
			} else {
				assert.Empty(t, lints)
			}
		})
	}
}
