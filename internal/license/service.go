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
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "embed"
)

//go:embed public_key.pem
var licensePublicKeyPem []byte

const defaultLicenseFilepath = "/etc/redpanda/redpanda.license"

var openSourceLicense = RedpandaLicense{
	Type:   -1,
	Expiry: time.Now().Add(time.Hour * 24 * 365 * 10).Unix(),
}

// Service is the license service.
type Service struct {
	logger        *service.Logger
	loadedLicense *atomic.Pointer[RedpandaLicense]
	conf          Config
}

// Config is a struct used to provide configuration to a license service.
type Config struct {
	License         string
	LicenseFilepath string

	// Just for testing
	customPublicKeyPem           []byte
	customDefaultLicenseFilepath string
}

func (c Config) publicKeyPem() []byte {
	if len(c.customPublicKeyPem) > 0 {
		return c.customPublicKeyPem
	}
	return licensePublicKeyPem
}

func (c Config) defaultLicenseFilepath() string {
	if c.customDefaultLicenseFilepath != "" {
		return c.customDefaultLicenseFilepath
	}
	return defaultLicenseFilepath
}

// RegisterService creates a new license service and registers it to the
// provided resources pointer.
func RegisterService(res *service.Resources, conf Config) {
	s := &Service{
		logger:        res.Logger(),
		loadedLicense: &atomic.Pointer[RedpandaLicense]{},
		conf:          conf,
	}

	license, err := s.readAndValidateLicense()
	if err != nil {
		res.Logger().With("error", err).Error("Failed to read Redpanda License")
	}
	s.loadedLicense.Store(&license)

	setSharedService(res, s)
}

// InjectTestService inserts an enterprise license into a resources pointer in
// order to provide testing frameworks a way to test enterprise components.
func InjectTestService(res *service.Resources) {
	s := &Service{
		logger:        res.Logger(),
		loadedLicense: &atomic.Pointer[RedpandaLicense]{},
	}
	s.loadedLicense.Store(&RedpandaLicense{
		Version:      1,
		Organization: "test",
		Type:         1,
		Expiry:       time.Now().Add(time.Hour).Unix(),
	})
	setSharedService(res, s)
}

// InjectCustomLicenseBytes attempts to parse a Redpanda Enterprise license
// from a slice of bytes and, if successful, stores it within the provided
// resources pointer for enterprise components to reference.
func InjectCustomLicenseBytes(res *service.Resources, conf Config, licenseBytes []byte) error {
	s := &Service{
		logger:        res.Logger(),
		loadedLicense: &atomic.Pointer[RedpandaLicense]{},
		conf:          conf,
	}

	license, err := s.validateLicense(licenseBytes)
	if err != nil {
		return fmt.Errorf("failed to validate license: %w", err)
	}

	if err := license.CheckExpiry(); err != nil {
		return err
	}

	s.logger.With(
		"license_org", license.Organization,
		"license_type", typeDisplayName(license.Type),
		"expires_at", time.Unix(license.Expiry, 0).Format(time.RFC3339),
	).Debug("Successfully loaded Redpanda license")

	s.loadedLicense.Store(&license)
	setSharedService(res, s)
	return nil
}

func (s *Service) readAndValidateLicense() (RedpandaLicense, error) {
	licenseBytes, err := s.readLicense()
	if err != nil {
		return openSourceLicense, err
	}

	license := openSourceLicense
	if len(licenseBytes) > 0 {
		if license, err = s.validateLicense(licenseBytes); err != nil {
			return openSourceLicense, fmt.Errorf("failed to validate license: %w", err)
		}
	}

	if err := license.CheckExpiry(); err != nil {
		return openSourceLicense, err
	}

	s.logger.With(
		"license_org", license.Organization,
		"license_type", typeDisplayName(license.Type),
		"expires_at", time.Unix(license.Expiry, 0).Format(time.RFC3339),
	).Debug("Successfully loaded Redpanda license")

	return license, nil
}

func (s *Service) readLicense() (licenseFileContents []byte, err error) {
	// Explicit license takes priority.
	if s.conf.License != "" {
		s.logger.Debug("Loading explicitly defined Redpanda Enterprise license")

		licenseFileContents = []byte(s.conf.License)
		return
	}

	// Followed by explicit license file path.
	if s.conf.LicenseFilepath != "" {
		s.logger.Info("Loading Redpanda Enterprise license from explicit file path")

		licenseFileContents, err = os.ReadFile(s.conf.LicenseFilepath)
		if err != nil {
			return nil, fmt.Errorf("failed to read license file: %w", err)
		}
		return
	}

	// Followed by the default file path.
	if licenseFileContents, err = os.ReadFile(s.conf.defaultLicenseFilepath()); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read default path license file: %w", err)
		}
		return nil, nil
	}

	s.logger.Info("Loaded Redpanda Enterprise license from default file path")
	return
}

func (s *Service) validateLicense(license []byte) (RedpandaLicense, error) {
	publicKeyBytes := s.conf.publicKeyPem()

	// 1. Try to parse embedded public key
	block, _ := pem.Decode(publicKeyBytes)
	publicKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return openSourceLicense, fmt.Errorf("failed to parse public key: %w", err)
	}
	publicKeyRSA, ok := publicKey.(*rsa.PublicKey)
	if !ok {
		return openSourceLicense, errors.New("failed to parse public key, expected dateFormat is not RSA")
	}

	// Trim Whitespace and Linebreaks for input license
	license = bytes.TrimSpace(license)

	// 2. Split license contents by delimiter
	splitParts := bytes.Split(license, []byte("."))
	if len(splitParts) != 2 {
		return openSourceLicense, errors.New("failed to split license contents by delimiter")
	}

	licenseDataEncoded := splitParts[0]
	signatureEncoded := splitParts[1]

	licenseData, err := base64.StdEncoding.DecodeString(string(licenseDataEncoded))
	if err != nil {
		return openSourceLicense, fmt.Errorf("failed to decode license data: %w", err)
	}

	signature, err := base64.StdEncoding.DecodeString(string(signatureEncoded))
	if err != nil {
		return openSourceLicense, fmt.Errorf("failed to decode license signature: %w", err)
	}
	hash := sha256.Sum256(licenseDataEncoded)

	// 3. Verify license contents with static public key
	if err := rsa.VerifyPKCS1v15(publicKeyRSA, crypto.SHA256, hash[:], signature); err != nil {
		return openSourceLicense, fmt.Errorf("failed to verify license signature: %w", err)
	}

	// 4. If license contents seem to be legit, we will continue unpacking the license
	var rpLicense RedpandaLicense
	if err := json.Unmarshal(licenseData, &rpLicense); err != nil {
		return openSourceLicense, fmt.Errorf("failed to unmarshal license data: %w", err)
	}

	return rpLicense, nil
}
