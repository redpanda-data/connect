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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/require"
)

func generatePrivateKey() ([]byte, error) {
	const keySize = 2048
	privateKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, err
	}
	return x509.MarshalPKCS8PrivateKey(privateKey)
}

func generateBase64EncodedKey() ([]byte, error) {
	privDER, err := generatePrivateKey()
	if err != nil {
		return nil, err
	}
	return []byte(base64.StdEncoding.EncodeToString(privDER)), nil
}

func generatePEMEncodedKey() ([]byte, error) {
	privDER, err := generatePrivateKey()
	if err != nil {
		return nil, err
	}
	privBlock := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privDER,
	}
	return pem.EncodeToMemory(privBlock), nil
}

func TestPrivateKeyPemEncoded(t *testing.T) {
	k, err := generatePEMEncodedKey()
	require.NoError(t, err)
	_, err = getPrivateKey(k, "")
	require.NoError(t, err)
}

func TestPrivateKeyBase64Encoded(t *testing.T) {
	k, err := generateBase64EncodedKey()
	require.NoError(t, err)
	_, err = getPrivateKey(k, "")
	require.NoError(t, err)
}
