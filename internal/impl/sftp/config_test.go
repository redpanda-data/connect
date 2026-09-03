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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

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
			errContains: `getting host public key: both "host_public_key" and "host_public_key_file" cannot be set simultaneously`,
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
			errContains: `getting private key: both "private_key" and "private_key_file" cannot be set simultaneously`,
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

// TestHostKeyAlgorithms covers CON-542: when a host public key is pinned via
// "host_public_key"/"host_public_key_file", the resulting ssh.ClientConfig's
// HostKeyAlgorithms must advertise the rsa-sha2-256/rsa-sha2-512 algorithms
// (in addition to ssh-rsa) for RSA host keys, so that the client can
// negotiate with modern OpenSSH servers that no longer offer the SHA-1 based
// "ssh-rsa" signature algorithm. Non-RSA key types should keep exactly their
// single reported algorithm.
func TestHostKeyAlgorithms(t *testing.T) {
	spec := service.NewConfigSpec().Fields(connectionFields()...)
	env := service.NewEnvironment()

	// A static, test-only 2048-bit RSA public key in authorized_keys format.
	const rsaPublicKey = `ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDet0eo6ERYCirrxjybU/R0p6dxfdX9OKaHQ8bgKMdkf0hVUoknXhCFP+QY56LMkzkLvRmavZEzgNUncmcHPpC25+vF1ToJqO4XyWZzE1Hq/pwSw4MNQ8Sf4wr1Iln+KCOHXFXfOAwa7i7djCSL+BxIqutfVEvSG/4ZQnwUIoHCG/XtvYSaChUm1IQokQYSczbemSTGeXmRRXDtrTKMlJyhJ3MwafoFH/nmNDO7ohcrj1a3OAI/TIwA4ASXEWvaQci8UrOBrsl7KXHjYZYeknq5tRhEKlQ2TUSwguj8RnS3gh8DN7Nj0eB875qdWOwrk3J91+tsLIGeFGJK8LX0DYFp test-key`

	tests := []struct {
		name             string
		hostPublicKey    string
		wantHostKeyAlgos []string
	}{
		{
			name:             "rsa host key advertises rsa-sha2 algorithms",
			hostPublicKey:    rsaPublicKey,
			wantHostKeyAlgos: []string{ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSA},
		},
		{
			name:             "ed25519 host key advertises only its own algorithm",
			hostPublicKey:    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDknETovnNcLdtMzYk3qj9qGmRh0NkS6i4uGc3jtBdmK",
			wantHostKeyAlgos: []string{ssh.KeyAlgoED25519},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := fmt.Sprintf(`
address: localhost:22
credentials:
  username: blobfish
  password: secret
  host_public_key: %s
`, test.hostPublicKey)

			pConf, err := spec.ParseYAML(conf, env)
			require.NoError(t, err)

			sshConf, err := sshAuthConfigFromParsed(pConf.Namespace(sFieldCredentials), service.MockResources())
			require.NoError(t, err)

			assert.ElementsMatch(t, test.wantHostKeyAlgos, sshConf.HostKeyAlgorithms)
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
