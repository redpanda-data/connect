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
	"errors"
	"fmt"
	"os/user"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/crypto/ssh"

	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	sFieldAddress                      = "address"
	sFieldConnectionTimeout            = "connection_timeout"
	sFieldCredentials                  = "credentials"
	sFieldCredentialsUsername          = "username"
	sFieldCredentialsPassword          = "password"
	sFieldCredentialsHostPublicKey     = "host_public_key"
	sFieldCredentialsHostPublicKeyFile = "host_public_key_file"
	sFieldCredentialsPrivateKey        = "private_key"
	sFieldCredentialsPrivateKeyFile    = "private_key_file"
	sFieldCredentialsPrivateKeyPass    = "private_key_pass"
	sFieldCredentialsSSHAlgorithms     = "ssh_algorithms"
	sFieldSSHAlgorithmsKeyExchanges    = "additional_key_exchanges"
	sFieldSSHAlgorithmsCiphers         = "additional_ciphers"
	sFieldSSHAlgorithmsMACs            = "additional_macs"
)

func connectionFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(sFieldAddress).
			Description("The address (hostname or IP address) of the SFTP server to connect to."),
		service.NewDurationField(sFieldConnectionTimeout).
			Description("The connection timeout to use when connecting to the target server.").Version("4.59.0").
			Default("30s").
			Advanced(),
		service.NewObjectField(sFieldCredentials,
			[]*service.ConfigField{
				service.NewStringField(sFieldCredentialsUsername).Description("The username required to authenticate with the SFTP server.").Default(""),
				service.NewStringField(sFieldCredentialsPassword).Description("The password to use for authentication. Used together with `username` for basic authentication or with encrypted private keys for secure access.").Secret().Default(""),
				service.NewStringField(sFieldCredentialsHostPublicKeyFile).Description("The path to the SFTP server's public key file, used for host key verification.").Version("4.59.0").Optional(),
				service.NewStringField(sFieldCredentialsHostPublicKey).Description("The raw contents of the SFTP server's public key, used for host key verification.").Version("4.59.0").Optional(),
				service.NewStringField(sFieldCredentialsPrivateKeyFile).Description("The path to a private key file used to authenticate with the SFTP server. You can also provide a private key using the `private_key` field.").Optional(),
				service.NewStringField(sFieldCredentialsPrivateKey).Description("The raw contents of the private key used to authenticate with the SFTP server. This field provides an alternative to `private_key_file`.").Version("4.51.0").Optional().Secret(),
				service.NewStringField(sFieldCredentialsPrivateKeyPass).Description("An optional passphrase for decrypting the private key, if it is encrypted.").Secret().Default(""),
				service.NewObjectField(sFieldCredentialsSSHAlgorithms,
					service.NewStringListField(sFieldSSHAlgorithmsKeyExchanges).
						Description("Key exchange algorithms to permit in addition to the defaults, for example `diffie-hellman-group1-sha1` or `diffie-hellman-group-exchange-sha1`.").
						Optional(),
					service.NewStringListField(sFieldSSHAlgorithmsCiphers).
						Description("Ciphers to permit in addition to the defaults, for example `aes128-cbc`.").
						Optional(),
					service.NewStringListField(sFieldSSHAlgorithmsMACs).
						Description("MAC algorithms to permit in addition to the defaults.").
						Optional(),
				).Description("Additional SSH transport algorithms to permit for compatibility with legacy SFTP servers. Configured algorithms are appended to the default algorithms, which remain preferred, and when unset the defaults are used unchanged. Algorithms the underlying SSH library classifies as insecure weaken transport security and should only be enabled when required by a known server. These settings do not affect host key verification.").
					Version("4.113.0").
					Advanced().
					Optional(),
			}...,
		).Description("The credentials required to log in to the SFTP server. This can include a username and password, or a private key for secure access.").
			LintRule(`
root = match {
  this.exists("host_public_key") && this.exists("host_public_key_file") => "both host_public_key and host_public_key_file can't be set simultaneously"
  this.exists("private_key") && this.exists("private_key_file") => "both private_key and private_key_file can't be set simultaneously"
}`,
			),
	}
}

func getKey(pConf *service.ParsedConfig, mgr *service.Resources, keyField, keyFileField string) ([]byte, error) {
	var keyData string
	var err error
	if pConf.Contains(keyField) {
		if keyData, err = pConf.FieldString(keyField); err != nil {
			return nil, err
		}
	}

	var keyFileData string
	if pConf.Contains(keyFileField) {
		if keyFileData, err = pConf.FieldString(keyFileField); err != nil {
			return nil, err
		}
	}

	if keyData != "" && keyFileData != "" {
		return nil, fmt.Errorf("both %q and %q cannot be set simultaneously", keyField, keyFileField)
	}

	var key []byte
	if keyData != "" {
		key = []byte(keyData)
	} else if keyFileData != "" {
		key, err = service.ReadFile(mgr.FS(), keyFileData)
		if err != nil {
			return nil, fmt.Errorf("reading key file: %s", err)
		}
	}

	return key, nil
}

// hostKeyAlgorithmsForKeyFormat maps a pinned host key's format (as reported
// by ssh.PublicKey.Type()) to the set of signature algorithms that should be
// advertised via ssh.ClientConfig.HostKeyAlgorithms. HostKeyAlgorithms takes
// signature algorithms, not key formats, and RSA keys correspond to three
// signature algorithms (the modern SHA-2 variants plus the legacy SHA-1
// "ssh-rsa"). Modern OpenSSH servers (8.8+) disable the SHA-1 variant, so all
// three must be advertised for the handshake to succeed. Since the host key
// itself is pinned via ssh.FixedHostKey, advertising the legacy algorithm
// alongside the SHA-2 ones does not weaken verification.
func hostKeyAlgorithmsForKeyFormat(keyFormat string) []string {
	switch keyFormat {
	case ssh.KeyAlgoRSA:
		return []string{ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSA}
	case ssh.CertAlgoRSAv01:
		return []string{ssh.CertAlgoRSASHA256v01, ssh.CertAlgoRSASHA512v01, ssh.CertAlgoRSAv01}
	default:
		return []string{keyFormat}
	}
}

func sshAuthConfigFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*ssh.ClientConfig, error) {
	var err error

	var username string
	if username, err = pConf.FieldString(sFieldCredentialsUsername); err != nil {
		return nil, err
	}

	var password string
	if password, err = pConf.FieldString(sFieldCredentialsPassword); err != nil {
		return nil, err
	}

	privateKey, err := getKey(pConf, mgr, sFieldCredentialsPrivateKey, sFieldCredentialsPrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("getting private key: %s", err)
	}

	var signer ssh.Signer
	if privateKey != nil {
		var privateKeyPass string
		if privateKeyPass, err = pConf.FieldString(sFieldCredentialsPrivateKeyPass); err != nil {
			return nil, err
		}

		// Check if passphrase is provided and parse private key
		if privateKeyPass == "" {
			signer, err = ssh.ParsePrivateKey(privateKey)
		} else {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(privateKey, []byte(privateKeyPass))
		}
		if err != nil {
			return nil, fmt.Errorf("parsing private key: %s", err)
		}
	}

	var auth []ssh.AuthMethod

	// Set password auth when provided
	if password != "" {
		auth = append(auth, ssh.Password(password))
	}

	// Set private key auth when provided
	if signer != nil {
		auth = append(auth, ssh.PublicKeys(signer))
	}

	if len(auth) == 0 {
		return nil, errors.New("at least one authentication method must be provided")
	}

	hostPubKey, err := getKey(pConf, mgr, sFieldCredentialsHostPublicKey, sFieldCredentialsHostPublicKeyFile)
	if err != nil {
		return nil, fmt.Errorf("getting host public key: %s", err)
	}
	var hostKeyAlgorithms []string
	var keyCallback ssh.HostKeyCallback
	if len(hostPubKey) > 0 {
		hostKey, _, _, _, err := ssh.ParseAuthorizedKey(hostPubKey)
		if err != nil {
			return nil, fmt.Errorf("error parsing host public key: %s", err)
		}
		hostKeyAlgorithms = hostKeyAlgorithmsForKeyFormat(hostKey.Type())
		keyCallback = ssh.FixedHostKey(hostKey)
	} else {
		var u *user.User
		if u, err = user.Current(); err == nil {
			keyCallback, err = knownhosts.New(filepath.Join(u.HomeDir, ".ssh", "known_hosts"))
		} else {
			keyCallback, err = knownhosts.New("/etc/ssh/known_hosts")
		}
		if err != nil {
			return nil, fmt.Errorf("reading known_hosts file: %s", err)
		}
	}

	transportConfig, err := sshTransportConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	sshConfig := ssh.ClientConfig{
		Config:            transportConfig,
		User:              username,
		Auth:              auth,
		HostKeyCallback:   keyCallback,
		HostKeyAlgorithms: hostKeyAlgorithms,
	}

	return &sshConfig, nil
}

// sshTransportConfigFromParsed builds the key exchange, cipher and MAC lists of
// an ssh.Config from the optional "ssh_algorithms" block. Setting any of
// ssh.Config's algorithm lists replaces the library defaults, so configured
// algorithms are appended to the defaults obtained from ssh.Config.SetDefaults
// rather than used on their own. When nothing is configured a zero ssh.Config
// is returned, leaving the library defaults in place.
func sshTransportConfigFromParsed(pConf *service.ParsedConfig) (ssh.Config, error) {
	var conf ssh.Config
	if !pConf.Contains(sFieldCredentialsSSHAlgorithms) {
		return conf, nil
	}
	pConf = pConf.Namespace(sFieldCredentialsSSHAlgorithms)

	var defaults ssh.Config
	defaults.SetDefaults()
	supported, insecure := ssh.SupportedAlgorithms(), ssh.InsecureAlgorithms()

	var err error
	if conf.KeyExchanges, err = additionalAlgorithms(pConf, sFieldSSHAlgorithmsKeyExchanges, "key exchange", defaults.KeyExchanges,
		availableAlgorithms(ssh.Config{KeyExchanges: slices.Concat(supported.KeyExchanges, insecure.KeyExchanges)}).KeyExchanges); err != nil {
		return conf, err
	}
	if conf.Ciphers, err = additionalAlgorithms(pConf, sFieldSSHAlgorithmsCiphers, "cipher", defaults.Ciphers,
		availableAlgorithms(ssh.Config{Ciphers: slices.Concat(supported.Ciphers, insecure.Ciphers)}).Ciphers); err != nil {
		return conf, err
	}
	if conf.MACs, err = additionalAlgorithms(pConf, sFieldSSHAlgorithmsMACs, "MAC", defaults.MACs,
		availableAlgorithms(ssh.Config{MACs: slices.Concat(supported.MACs, insecure.MACs)}).MACs); err != nil {
		return conf, err
	}
	return conf, nil
}

// availableAlgorithms filters the algorithm lists of c down to those
// implemented by the current build. ssh.Config.SetDefaults silently drops
// unimplemented algorithms (e.g. non-FIPS algorithms in FIPS 140 mode), which
// would otherwise turn a configured algorithm into a handshake failure.
func availableAlgorithms(c ssh.Config) ssh.Config {
	c.SetDefaults()
	return c
}

// additionalAlgorithms returns the default algorithms followed by any
// configured algorithms not already present, in the order configured. It
// returns nil when no algorithms are configured so that the library defaults
// apply.
func additionalAlgorithms(pConf *service.ParsedConfig, field, kind string, defaults, available []string) ([]string, error) {
	if !pConf.Contains(field) {
		return nil, nil
	}
	extra, err := pConf.FieldStringList(field)
	if err != nil {
		return nil, err
	}
	if len(extra) == 0 {
		return nil, nil
	}

	algos := slices.Clone(defaults)
	for _, a := range extra {
		if !slices.Contains(available, a) && !slices.Contains(defaults, a) {
			return nil, fmt.Errorf("unsupported SSH %s algorithm %q, available algorithms: %s", kind, a, strings.Join(available, ", "))
		}
		if !slices.Contains(algos, a) {
			algos = append(algos, a)
		}
	}
	return algos, nil
}
