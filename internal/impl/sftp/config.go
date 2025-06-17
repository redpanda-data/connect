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
)

func connectionFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(sFieldAddress).
			Description("The address of the server to connect to."),
		service.NewDurationField(sFieldConnectionTimeout).
			Description("The connection timeout to use when connecting to the target server.").
			Default("30s").
			Advanced(),
		service.NewObjectField(sFieldCredentials,
			[]*service.ConfigField{
				service.NewStringField(sFieldCredentialsUsername).Description("The username to connect to the SFTP server.").Default(""),
				service.NewStringField(sFieldCredentialsPassword).Description("The password for the username to connect to the SFTP server.").Secret().Default(""),
				service.NewStringField(sFieldCredentialsHostPublicKeyFile).Description("The public key of the SFTP server.").Optional(),
				service.NewStringField(sFieldCredentialsHostPublicKey).Description("The public key file of the SFTP server.").Optional(),
				service.NewStringField(sFieldCredentialsPrivateKeyFile).Description("The private key file for the username to connect to the SFTP server.").Optional(),
				service.NewStringField(sFieldCredentialsPrivateKey).Description("The private key for the username to connect to the SFTP server.").Optional().Secret(),
				service.NewStringField(sFieldCredentialsPrivateKeyPass).Description("Optional passphrase for private key.").Secret().Default(""),
			}...,
		).Description("The credentials to use to log into the target server.").
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
			return nil, fmt.Errorf("failed to read key file: %s", err)
		}
	}

	return key, nil
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
		return nil, fmt.Errorf("failed to get private key: %s", err)
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
			return nil, fmt.Errorf("failed to parse private key: %s", err)
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
		return nil, fmt.Errorf("failed to get host public key: %s", err)
	}
	var hostKeyAlgorithms []string
	var keyCallback ssh.HostKeyCallback
	if len(hostPubKey) > 0 {
		hostKey, _, _, _, err := ssh.ParseAuthorizedKey(hostPubKey)
		if err != nil {
			return nil, fmt.Errorf("error parsing host public key: %s", err)
		}
		hostKeyAlgorithms = []string{hostKey.Type()}
		keyCallback = ssh.FixedHostKey(hostKey)
	} else {
		var u *user.User
		if u, err = user.Current(); err == nil {
			keyCallback, err = knownhosts.New(filepath.Join(u.HomeDir, ".ssh", "known_hosts"))
		} else {
			keyCallback, err = knownhosts.New("/etc/ssh/known_hosts")
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read known_hosts file: %s", err)
		}
	}

	sshConfig := ssh.ClientConfig{
		User:              username,
		Auth:              auth,
		HostKeyCallback:   keyCallback,
		HostKeyAlgorithms: hostKeyAlgorithms,
	}

	return &sshConfig, nil
}
