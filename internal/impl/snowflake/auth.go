/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package snowflake

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"

	"github.com/youmark/pkcs8"
	"golang.org/x/crypto/ssh"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func wipeSlice(b []byte) {
	for i := range b {
		b[i] = '~'
	}
}

// getPrivateKeyFromFile reads and parses the private key
// Inspired from https://github.com/chanzuckerberg/terraform-provider-snowflake/blob/c07d5820bea7ac3d8a5037b0486c405fdf58420e/pkg/provider/provider.go#L367
func getPrivateKeyFromFile(f fs.FS, path, passphrase string) (*rsa.PrivateKey, error) {
	privateKeyBytes, err := service.ReadFile(f, path)
	defer wipeSlice(privateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key %s: %s", path, err)
	}
	if len(privateKeyBytes) == 0 {
		return nil, errors.New("private key is empty")
	}
	return getPrivateKey(privateKeyBytes, passphrase)
}

func getPrivateKey(privateKeyBytes []byte, passphrase string) (*rsa.PrivateKey, error) {
	privateKeyBlock, _ := pem.Decode(privateKeyBytes)
	if privateKeyBlock == nil {
		// Snowflake generally uses base64 encoded keys everywhere not pem encoding,
		// so let's be compatible with that as a fallback.
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
			return nil, errors.New("private key requires a passphrase, but private_key_pass was not supplied")
		}

		// Only keys encrypted with pbes2 http://oid-info.com/get/1.2.840.113549.1.5.13 are supported.
		// pbeWithMD5AndDES-CBC http://oid-info.com/get/1.2.840.113549.1.5.3 is not supported.
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
		return nil, fmt.Errorf("private key must be of type RSA but got %T instead: ", privateKey)
	}
	return rsaPrivateKey, nil
}

// calculatePublicKeyFingerprint computes the value of the `RSA_PUBLIC_KEY_FP` for the current user based on the
// configured private key
// Inspired from https://stackoverflow.com/questions/63598044/snowpipe-rest-api-returning-always-invalid-jwt-token
func calculatePublicKeyFingerprint(privateKey *rsa.PrivateKey) (string, error) {
	pubKey := privateKey.Public()
	pubDER, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		return "", fmt.Errorf("failed to marshal public key: %s", err)
	}

	hash := sha256.Sum256(pubDER)
	return "SHA256:" + base64.StdEncoding.EncodeToString(hash[:]), nil
}
