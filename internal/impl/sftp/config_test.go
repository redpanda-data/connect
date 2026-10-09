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
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
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
			name: "valid config with ssh algorithms",
			conf: `
sftp:
  address: localhost:22
  credentials:
    username: blobfish
    password: secret
    ssh_algorithms:
      additional_key_exchanges: [ diffie-hellman-group1-sha1 ]
      additional_ciphers: [ aes128-cbc ]
      additional_macs: [ hmac-sha1 ]
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

func TestSSHAlgorithmsConfig(t *testing.T) {
	spec := service.NewConfigSpec().Fields(connectionFields()...)
	env := service.NewEnvironment()

	var defaults ssh.Config
	defaults.SetDefaults()

	tests := []struct {
		name             string
		algorithms       string
		wantKeyExchanges []string
		wantCiphers      []string
		wantMACs         []string
		errContains      string
	}{
		{
			// No configuration must leave the ssh.Config algorithm lists
			// empty so that the library applies its own defaults.
			name: "no ssh_algorithms uses library defaults",
		},
		{
			name: "empty lists use library defaults",
			algorithms: `
    additional_key_exchanges: []
    additional_ciphers: []
    additional_macs: []
`,
		},
		{
			name: "legacy key exchanges are appended to defaults",
			algorithms: `
    additional_key_exchanges:
      - diffie-hellman-group1-sha1
      - diffie-hellman-group-exchange-sha1
`,
			wantKeyExchanges: append(slices.Clone(defaults.KeyExchanges), ssh.InsecureKeyExchangeDH1SHA1, ssh.InsecureKeyExchangeDHGEXSHA1),
		},
		{
			name: "configured order is preserved and duplicates are dropped",
			algorithms: `
    additional_key_exchanges:
      - diffie-hellman-group-exchange-sha1
      - diffie-hellman-group1-sha1
      - diffie-hellman-group-exchange-sha1
      - curve25519-sha256
`,
			wantKeyExchanges: append(slices.Clone(defaults.KeyExchanges), ssh.InsecureKeyExchangeDHGEXSHA1, ssh.InsecureKeyExchangeDH1SHA1),
		},
		{
			name: "legacy ciphers are appended to defaults",
			algorithms: `
    additional_ciphers:
      - aes128-cbc
      - 3des-cbc
`,
			wantCiphers: append(slices.Clone(defaults.Ciphers), ssh.InsecureCipherAES128CBC, ssh.InsecureCipherTripleDESCBC),
		},
		{
			// Every MAC implemented by the pinned x/crypto is already a
			// default, so a configured MAC is accepted without duplication.
			name: "MAC already in defaults is accepted",
			algorithms: `
    additional_macs:
      - hmac-sha1-96
`,
			wantMACs: defaults.MACs,
		},
		{
			name: "all categories together",
			algorithms: `
    additional_key_exchanges: [ diffie-hellman-group1-sha1 ]
    additional_ciphers: [ aes128-cbc ]
    additional_macs: [ hmac-sha1 ]
`,
			wantKeyExchanges: append(slices.Clone(defaults.KeyExchanges), ssh.InsecureKeyExchangeDH1SHA1),
			wantCiphers:      append(slices.Clone(defaults.Ciphers), ssh.InsecureCipherAES128CBC),
			wantMACs:         defaults.MACs,
		},
		{
			name: "unknown key exchange",
			algorithms: `
    additional_key_exchanges: [ diffie-hellman-group1-sha256 ]
`,
			errContains: `unsupported SSH key exchange algorithm "diffie-hellman-group1-sha256"`,
		},
		{
			name: "unknown cipher",
			algorithms: `
    additional_ciphers: [ aes128-cbcc ]
`,
			errContains: `unsupported SSH cipher algorithm "aes128-cbcc"`,
		},
		{
			name: "unknown MAC",
			algorithms: `
    additional_macs: [ hmac-md5 ]
`,
			errContains: `unsupported SSH MAC algorithm "hmac-md5"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := `
address: localhost:22
credentials:
  username: blobfish
  password: secret
  host_public_key: ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDknETovnNcLdtMzYk3qj9qGmRh0NkS6i4uGc3jtBdmK
`
			if test.algorithms != "" {
				conf += "  ssh_algorithms:" + test.algorithms
			}

			pConf, err := spec.ParseYAML(conf, env)
			require.NoError(t, err)

			sshConf, err := sshAuthConfigFromParsed(pConf.Namespace(sFieldCredentials), service.MockResources())
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
				return
			}
			require.NoError(t, err)

			assert.Equal(t, test.wantKeyExchanges, sshConf.KeyExchanges)
			assert.Equal(t, test.wantCiphers, sshConf.Ciphers)
			assert.Equal(t, test.wantMACs, sshConf.MACs)
		})
	}
}

// TestSSHAlgorithmsPreserveHostKeyConfig verifies that configuring additional
// transport algorithms does not alter host key verification (CON-542).
func TestSSHAlgorithmsPreserveHostKeyConfig(t *testing.T) {
	spec := service.NewConfigSpec().Fields(connectionFields()...)
	env := service.NewEnvironment()

	const rsaPublicKey = `ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDet0eo6ERYCirrxjybU/R0p6dxfdX9OKaHQ8bgKMdkf0hVUoknXhCFP+QY56LMkzkLvRmavZEzgNUncmcHPpC25+vF1ToJqO4XyWZzE1Hq/pwSw4MNQ8Sf4wr1Iln+KCOHXFXfOAwa7i7djCSL+BxIqutfVEvSG/4ZQnwUIoHCG/XtvYSaChUm1IQokQYSczbemSTGeXmRRXDtrTKMlJyhJ3MwafoFH/nmNDO7ohcrj1a3OAI/TIwA4ASXEWvaQci8UrOBrsl7KXHjYZYeknq5tRhEKlQ2TUSwguj8RnS3gh8DN7Nj0eB875qdWOwrk3J91+tsLIGeFGJK8LX0DYFp test-key`

	pConf, err := spec.ParseYAML(fmt.Sprintf(`
address: localhost:22
credentials:
  username: blobfish
  password: secret
  host_public_key: %s
  ssh_algorithms:
    additional_key_exchanges: [ diffie-hellman-group1-sha1 ]
`, rsaPublicKey), env)
	require.NoError(t, err)

	sshConf, err := sshAuthConfigFromParsed(pConf.Namespace(sFieldCredentials), service.MockResources())
	require.NoError(t, err)

	assert.Equal(t, []string{ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSA}, sshConf.HostKeyAlgorithms)
	require.NotNil(t, sshConf.HostKeyCallback)

	// The pinned key must still be enforced: any other host key is rejected.
	_, otherKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	otherSigner, err := ssh.NewSignerFromKey(otherKey)
	require.NoError(t, err)
	require.Error(t, sshConf.HostKeyCallback("localhost:22", &net.TCPAddr{}, otherSigner.PublicKey()))
}

// TestSSHAlgorithmsHandshake performs in-process SSH handshakes against a
// server restricted to a single legacy key exchange algorithm, verifying that
// the default configuration fails to negotiate and that explicitly opting in
// to the algorithm succeeds.
func TestSSHAlgorithmsHandshake(t *testing.T) {
	// loopbackConnPair returns both ends of a loopback TCP connection. net.Pipe is
	// unsuitable for SSH handshakes because it is unbuffered and both peers send
	// their version banner before reading.
	loopbackConnPair := func(t *testing.T) (client, server net.Conn) {
		t.Helper()

		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer l.Close()

		accepted := make(chan net.Conn, 1)
		go func() {
			c, err := l.Accept()
			if err != nil {
				close(accepted)
				return
			}
			accepted <- c
		}()

		client, err = net.Dial("tcp", l.Addr().String())
		require.NoError(t, err)
		server, ok := <-accepted
		require.True(t, ok, "accepting loopback connection")
		return client, server
	}

	spec := service.NewConfigSpec().Fields(connectionFields()...)
	env := service.NewEnvironment()

	_, hostKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	hostSigner, err := ssh.NewSignerFromKey(hostKey)
	require.NoError(t, err)
	hostPublicKey := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(hostSigner.PublicKey())))

	for _, kex := range []string{ssh.InsecureKeyExchangeDH1SHA1, ssh.InsecureKeyExchangeDHGEXSHA1} {
		t.Run(kex, func(t *testing.T) {
			serverConf := &ssh.ServerConfig{
				Config: ssh.Config{KeyExchanges: []string{kex}},
				PasswordCallback: func(_ ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
					if string(password) != "secret" {
						return nil, errors.New("wrong password")
					}
					return nil, nil
				},
			}
			serverConf.AddHostKey(hostSigner)

			handshake := func(t *testing.T, algorithms string) error {
				t.Helper()

				conf := fmt.Sprintf(`
address: localhost:22
credentials:
  username: blobfish
  password: secret
  host_public_key: %s
`, hostPublicKey) + algorithms

				pConf, err := spec.ParseYAML(conf, env)
				require.NoError(t, err)
				clientConf, err := sshAuthConfigFromParsed(pConf.Namespace(sFieldCredentials), service.MockResources())
				require.NoError(t, err)

				clientConn, serverConn := loopbackConnPair(t)
				t.Cleanup(func() {
					clientConn.Close()
					serverConn.Close()
				})

				serverErr := make(chan error, 1)
				go func() {
					_, _, _, err := ssh.NewServerConn(serverConn, serverConf)
					if err != nil {
						// Unblock the client if the server gives up first.
						serverConn.Close()
					}
					serverErr <- err
				}()

				c, chans, reqs, err := ssh.NewClientConn(clientConn, "localhost:22", clientConf)
				if err != nil {
					clientConn.Close()
					<-serverErr
					return err
				}
				go ssh.DiscardRequests(reqs)
				go func() {
					for ch := range chans {
						_ = ch.Reject(ssh.Prohibited, "")
					}
				}()
				require.NoError(t, <-serverErr)
				return c.Close()
			}

			err := handshake(t, "")
			require.ErrorContains(t, err, "no common algorithm for key exchange")

			require.NoError(t, handshake(t, fmt.Sprintf(`
  ssh_algorithms:
    additional_key_exchanges: [ %s ]
`, kex)))
		})
	}
}
