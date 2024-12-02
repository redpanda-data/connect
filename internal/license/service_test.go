// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package license

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createLicense(t testing.TB, license RedpandaLicense) (pubKey []byte, licenseStr string) {
	t.Helper()

	licenseBytes, err := json.Marshal(license)
	require.NoError(t, err)

	licenseBytesEncoded := base64.StdEncoding.AppendEncode(nil, bytes.TrimSpace(licenseBytes))
	licenseEncodedBytesHash := sha256.Sum256(licenseBytesEncoded)

	privKeyRSA, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pubKeyMarshalled, err := x509.MarshalPKIXPublicKey(&privKeyRSA.PublicKey)
	require.NoError(t, err)

	var pemBuf bytes.Buffer
	require.NoError(t, pem.Encode(&pemBuf, &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubKeyMarshalled,
	}))

	signature, err := rsa.SignPKCS1v15(nil, privKeyRSA, crypto.SHA256, licenseEncodedBytesHash[:])
	require.NoError(t, err)

	return pemBuf.Bytes(), string(licenseBytesEncoded) + "." + base64.StdEncoding.EncodeToString(signature)
}

func TestLicenseEnterpriseValidation(t *testing.T) {
	for _, test := range []struct {
		Name         string
		License      RedpandaLicense
		IsEnterprise bool
	}{
		{
			Name: "expired license",
			License: RedpandaLicense{
				Version:      3,
				Organization: "meow",
				Type:         1,
				Expiry:       time.Now().Add(-time.Hour).Unix(),
			},
			IsEnterprise: false,
		},
		{
			Name: "free trial license",
			License: RedpandaLicense{
				Version:      3,
				Organization: "meow",
				Type:         0,
				Expiry:       time.Now().Add(time.Hour).Unix(),
			},
			IsEnterprise: false,
		},
		{
			Name: "enterprise license",
			License: RedpandaLicense{
				Version:      3,
				Organization: "meow",
				Type:         1,
				Expiry:       time.Now().Add(time.Hour).Unix(),
			},
			IsEnterprise: true,
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			pubKey, license := createLicense(t, test.License)

			res := service.MockResources()
			RegisterService(res, Config{
				License:            license,
				customPublicKeyPem: pubKey,
			})

			loaded, err := LoadFromResources(res)
			require.NoError(t, err)

			assert.Equal(t, test.IsEnterprise, loaded.AllowsEnterpriseFeatures())
		})
	}
}

func TestLicenseEnterpriseDefaultPath(t *testing.T) {
	tmpDir := t.TempDir()
	tmpLicensePath := filepath.Join(tmpDir, "foo.license")

	pubKey, license := createLicense(t, RedpandaLicense{
		Version:      3,
		Organization: "meow",
		Type:         1,
		Expiry:       time.Now().Add(time.Hour).Unix(),
	})

	// No file created

	res := service.MockResources()
	RegisterService(res, Config{
		customPublicKeyPem:           pubKey,
		customDefaultLicenseFilepath: tmpLicensePath,
	})

	loaded, err := LoadFromResources(res)
	require.NoError(t, err)

	assert.False(t, loaded.AllowsEnterpriseFeatures())

	// File created

	require.NoError(t, os.WriteFile(tmpLicensePath, []byte(license), 0o777))

	res = service.MockResources()
	RegisterService(res, Config{
		customPublicKeyPem:           pubKey,
		customDefaultLicenseFilepath: tmpLicensePath,
	})

	loaded, err = LoadFromResources(res)
	require.NoError(t, err)

	assert.True(t, loaded.AllowsEnterpriseFeatures())
}

func TestLicenseEnterpriseCustomPath(t *testing.T) {
	tmpDir := t.TempDir()
	tmpBadLicensePath := filepath.Join(tmpDir, "bad.license")
	tmpGoodLicensePath := filepath.Join(tmpDir, "good.license")

	_, license := createLicense(t, RedpandaLicense{
		Version:      3,
		Organization: "meow",
		Type:         1,
		Expiry:       time.Now().Add(-time.Hour).Unix(),
	})
	require.NoError(t, os.WriteFile(tmpBadLicensePath, []byte(license), 0o777))

	pubKey, license := createLicense(t, RedpandaLicense{
		Version:      3,
		Organization: "meow",
		Type:         1,
		Expiry:       time.Now().Add(time.Hour).Unix(),
	})
	require.NoError(t, os.WriteFile(tmpGoodLicensePath, []byte(license), 0o777))

	res := service.MockResources()
	RegisterService(res, Config{
		LicenseFilepath:              tmpGoodLicensePath,
		customPublicKeyPem:           pubKey,
		customDefaultLicenseFilepath: tmpBadLicensePath,
	})

	loaded, err := LoadFromResources(res)
	require.NoError(t, err)

	assert.True(t, loaded.AllowsEnterpriseFeatures())
}

func TestLicenseEnterpriseExplicit(t *testing.T) {
	tmpDir := t.TempDir()
	tmpBadLicensePath := filepath.Join(tmpDir, "bad.license")
	tmpAlsoBadLicensePath := filepath.Join(tmpDir, "alsobad.license")

	_, license := createLicense(t, RedpandaLicense{
		Version:      3,
		Organization: "meow",
		Type:         1,
		Expiry:       time.Now().Add(-time.Hour).Unix(),
	})
	require.NoError(t, os.WriteFile(tmpBadLicensePath, []byte(license), 0o777))

	_, license = createLicense(t, RedpandaLicense{
		Version:      3,
		Organization: "meow",
		Type:         0,
		Expiry:       time.Now().Add(time.Hour).Unix(),
	})
	require.NoError(t, os.WriteFile(tmpAlsoBadLicensePath, []byte(license), 0o777))

	pubKey, license := createLicense(t, RedpandaLicense{
		Version:      3,
		Organization: "meow",
		Type:         1,
		Expiry:       time.Now().Add(time.Hour).Unix(),
	})

	res := service.MockResources()
	RegisterService(res, Config{
		License:                      license,
		LicenseFilepath:              tmpAlsoBadLicensePath,
		customPublicKeyPem:           pubKey,
		customDefaultLicenseFilepath: tmpBadLicensePath,
	})

	loaded, err := LoadFromResources(res)
	require.NoError(t, err)

	assert.True(t, loaded.AllowsEnterpriseFeatures())
}

func TestLicenseEnterpriseNoLicense(t *testing.T) {
	tmpDir := t.TempDir()
	tmpBadLicensePath := filepath.Join(tmpDir, "bad.license")

	res := service.MockResources()
	RegisterService(res, Config{
		customDefaultLicenseFilepath: tmpBadLicensePath,
	})

	loaded, err := LoadFromResources(res)
	require.NoError(t, err)

	assert.False(t, loaded.AllowsEnterpriseFeatures())
}
