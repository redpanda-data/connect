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

//go:build !arm

package sql

import (
	"crypto/rsa"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/snowflakedb/gosnowflake"
	"github.com/youmark/pkcs8"
	"golang.org/x/crypto/ssh"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	sfFieldPrivateKey     = "snowflake_private_key"
	sfFieldPrivateKeyFile = "snowflake_private_key_file"
	sfFieldPrivateKeyPass = "snowflake_private_key_pass"
)

// snowflakeAuthFields returns the config fields for Snowflake key pair authentication.
func snowflakeAuthFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(sfFieldPrivateKey).
			Description("The PEM encoded private RSA key for Snowflake key pair authentication. Either this or `snowflake_private_key_file` must be specified for key pair auth.").
			Optional().
			Secret(),
		service.NewStringField(sfFieldPrivateKeyFile).
			Description("The file path to load the private RSA key from for Snowflake key pair authentication. This should be a `.p8` PEM encoded file. Either this or `snowflake_private_key` must be specified for key pair auth.").
			Optional(),
		service.NewStringField(sfFieldPrivateKeyPass).
			Description("The RSA key passphrase if the RSA key is encrypted.").
			Optional().
			Secret(),
	}
}

// snowflakeAuthConfig holds the parsed Snowflake authentication configuration.
type snowflakeAuthConfig struct {
	privateKey     *rsa.PrivateKey
	hasKeyPairAuth bool
}

// parseSnowflakeAuthConfig parses the Snowflake auth configuration from the parsed config.
func parseSnowflakeAuthConfig(conf *service.ParsedConfig, mgr *service.Resources) (*snowflakeAuthConfig, error) {
	authConfig := &snowflakeAuthConfig{}

	var keypass string
	if conf.Contains(sfFieldPrivateKeyPass) {
		pass, err := conf.FieldString(sfFieldPrivateKeyPass)
		if err != nil {
			return nil, err
		}
		keypass = pass
	}

	if conf.Contains(sfFieldPrivateKey) {
		key, err := conf.FieldString(sfFieldPrivateKey)
		if err != nil {
			return nil, err
		}
		if key != "" {
			authConfig.privateKey, err = parseSnowflakePrivateKey([]byte(key), keypass)
			if err != nil {
				return nil, fmt.Errorf("failed to parse snowflake_private_key: %w", err)
			}
			authConfig.hasKeyPairAuth = true
		}
	}

	if !authConfig.hasKeyPairAuth && conf.Contains(sfFieldPrivateKeyFile) {
		keyFile, err := conf.FieldString(sfFieldPrivateKeyFile)
		if err != nil {
			return nil, err
		}
		if keyFile != "" {
			authConfig.privateKey, err = parseSnowflakePrivateKeyFromFile(mgr.FS(), keyFile, keypass)
			if err != nil {
				return nil, fmt.Errorf("failed to parse snowflake_private_key_file: %w", err)
			}
			authConfig.hasKeyPairAuth = true
		}
	}

	return authConfig, nil
}

// openSnowflakeWithKeyPair opens a Snowflake connection using key pair authentication.
func openSnowflakeWithKeyPair(dsn string, privateKey *rsa.PrivateKey) (*sql.DB, error) {
	// For key pair auth, we need to add a dummy password to the DSN before parsing
	// because gosnowflake.ParseDSN() validates that a password exists.
	// The password will be ignored when using JWT authentication.
	dsnWithDummyPass := addDummyPasswordToDSN(dsn)

	cfg, err := gosnowflake.ParseDSN(dsnWithDummyPass)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Snowflake DSN: %w", err)
	}

	// Set up JWT authentication with the private key
	// This overrides password-based auth
	cfg.Authenticator = gosnowflake.AuthTypeJwt
	cfg.PrivateKey = privateKey
	cfg.Password = "" // Clear the dummy password

	// Create connector and open database
	connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *cfg)
	db := sql.OpenDB(connector)

	return db, nil
}

// addDummyPasswordToDSN adds a placeholder password to a DSN that doesn't have one.
// Format: user@account/db/schema -> user:_jwt_auth_@account/db/schema
func addDummyPasswordToDSN(dsn string) string {
	// Find the @ symbol that separates user from account
	atIndex := -1
	for i, c := range dsn {
		if c == '@' {
			atIndex = i
			break
		}
	}

	if atIndex == -1 {
		return dsn // No @ found, return as-is
	}

	userPart := dsn[:atIndex]
	restPart := dsn[atIndex:]

	// Check if password already exists (contains :)
	if strings.Contains(userPart, ":") {
		return dsn // Password already present
	}

	// Add dummy password
	return userPart + ":_jwt_auth_" + restPart
}

func wipeSlice(b []byte) {
	for i := range b {
		b[i] = '~'
	}
}

// parseSnowflakePrivateKeyFromFile reads and parses the private key from a file.
func parseSnowflakePrivateKeyFromFile(f fs.FS, path, passphrase string) (*rsa.PrivateKey, error) {
	privateKeyBytes, err := service.ReadFile(f, path)
	defer wipeSlice(privateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key %s: %s", path, err)
	}
	if len(privateKeyBytes) == 0 {
		return nil, errors.New("private key is empty")
	}
	return parseSnowflakePrivateKey(privateKeyBytes, passphrase)
}

// parseSnowflakePrivateKey parses a private key from bytes.
func parseSnowflakePrivateKey(privateKeyBytes []byte, passphrase string) (*rsa.PrivateKey, error) {
	privateKeyBlock, _ := pem.Decode(privateKeyBytes)
	if privateKeyBlock == nil {
		// Snowflake generally uses base64 encoded keys everywhere not pem encoding, so having compatibility with that as a fallback.
		dbuf := make([]byte, base64.StdEncoding.DecodedLen(len(privateKeyBytes)))
		n, err := base64.StdEncoding.Decode(dbuf, privateKeyBytes)
		if err != nil {
			return nil, errors.New("could not parse private key, key is not in PEM format")
		}
		privateKeyBlock = &pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: dbuf[:n],
		}
		if passphrase != "" {
			privateKeyBlock.Type = "ENCRYPTED PRIVATE KEY"
		}
		privateKeyBytes = pem.EncodeToMemory(privateKeyBlock)
	}

	if privateKeyBlock.Type == "ENCRYPTED PRIVATE KEY" {
		if passphrase == "" {
			return nil, errors.New("private key requires a passphrase, but snowflake_private_key_pass was not supplied")
		}

		// Only keys encrypted with pbes2 http://oid-info.com/get/1.2.840.113549.1.5.13 are supported.
		privateKey, err := pkcs8.ParsePKCS8PrivateKeyRSA(privateKeyBlock.Bytes, []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt encrypted private key (only ciphers aes-128-cbc, aes-128-gcm, aes-192-cbc, aes-192-gcm, aes-256-cbc, aes-256-gcm, and des-ede3-cbc are supported): %s", err)
		}

		return privateKey, nil
	}

	privateKey, err := ssh.ParseRawPrivateKey(privateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("could not parse private key: %s", err)
	}

	rsaPrivateKey, ok := privateKey.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key must be of type RSA but got %T instead", privateKey)
	}
	return rsaPrivateKey, nil
}
