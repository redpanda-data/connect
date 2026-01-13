// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package license

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/common-go/license"
)

const defaultLicenseFilepath = "/etc/redpanda/redpanda.license"

var openSourceLicense license.RedpandaLicense = &license.V1RedpandaLicense{
	Version:  1,
	Type:     license.LicenseTypeOpenSource,
	Expiry:   time.Now().Add(time.Hour * 24 * 365 * 10).Unix(),
	Products: []license.Product{license.ProductConnect},
}

// Service is the license service.
type Service struct {
	logger        *service.Logger
	loadedLicense *atomic.Pointer[license.RedpandaLicense]
	conf          Config

	expiryMetric *service.MetricGauge
	cancel       context.CancelFunc
}

// Config is a struct used to provide configuration to a license service.
type Config struct {
	License                      string
	LicenseFilepath              string
	customDefaultLicenseFilepath string
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
		loadedLicense: &atomic.Pointer[license.RedpandaLicense]{},
		conf:          conf,
	}

	license, err := s.readAndValidateLicense()
	if err != nil {
		res.Logger().With("error", err).Error("Failed to read Redpanda License")
		license = openSourceLicense
	}

	s.setLicense(res, license)
	setSharedService(res, s)
}

// InjectTestService inserts an enterprise license into a resources pointer in
// order to provide testing frameworks a way to test enterprise components.
func InjectTestService(res *service.Resources) {
	s := &Service{
		logger:        res.Logger(),
		loadedLicense: &atomic.Pointer[license.RedpandaLicense]{},
	}

	s.setLicense(res, &license.V1RedpandaLicense{
		Version:      1,
		Organization: "test",
		Type:         license.LicenseTypeEnterprise,
		Expiry:       time.Now().Add(time.Hour).Unix(),
		Products:     []license.Product{license.ProductConnect},
	})
	setSharedService(res, s)
}

// InjectCustomLicenseBytes attempts to parse a Redpanda Enterprise license
// from a slice of bytes and, if successful, stores it within the provided
// resources pointer for enterprise components to reference.
func InjectCustomLicenseBytes(res *service.Resources, conf Config, licenseBytes []byte) error {
	s := &Service{
		logger:        res.Logger(),
		loadedLicense: &atomic.Pointer[license.RedpandaLicense]{},
		conf:          conf,
	}

	l, err := license.ParseLicense(licenseBytes)
	if err != nil {
		return fmt.Errorf("failed to validate license: %w", err)
	}

	expiryTime := l.Expires()
	if time.Now().After(expiryTime) {
		return fmt.Errorf("license expired on %s", expiryTime.Format(time.RFC3339))
	}

	var orgStr, licenseTypeStr string
	switch t := l.(type) {
	case *license.V0RedpandaLicense:
		orgStr = t.Organization
		licenseTypeStr = t.Type.String()
	case *license.V1RedpandaLicense:
		orgStr = t.Organization
		licenseTypeStr = string(t.Type)
	}

	s.logger.With(
		"license_org", orgStr,
		"license_type", licenseTypeStr,
		"expires_at", expiryTime.Format(time.RFC3339),
	).Info("Successfully loaded Redpanda license")

	s.setLicense(res, l)
	setSharedService(res, s)

	return nil
}

func (s *Service) setLicense(res *service.Resources, l license.RedpandaLicense) {
	s.loadedLicense.Store(&l)

	if s.cancel != nil {
		s.cancel()
	}
	if l == nil || !l.AllowsEnterpriseFeatures() {
		return
	}

	if s.expiryMetric == nil {
		s.expiryMetric = res.Metrics().NewGauge("redpanda_cluster_features_enterprise_license_expiry_sec")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go s.updateExpiryMetricLoop(ctx, l)
}

// updateExpiryMetricLoop updates the license expiry metric every hour. The
// metric value is the delta in seconds between now and the expiry time.
func (s *Service) updateExpiryMetricLoop(ctx context.Context, l license.RedpandaLicense) {
	updateMetric := func() {
		expiryTime := l.Expires()
		deltaSeconds := time.Until(expiryTime).Seconds()
		s.expiryMetric.Set(int64(deltaSeconds))
	}
	updateMetric()

	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			updateMetric()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) readAndValidateLicense() (license.RedpandaLicense, error) {
	licenseBytes, err := s.readLicense()
	if err != nil {
		return nil, err
	}

	l := openSourceLicense
	if len(licenseBytes) > 0 {
		if l, err = license.ParseLicense(licenseBytes); err != nil {
			return nil, fmt.Errorf("failed to validate license: %w", err)
		}
	}

	expiryTime := l.Expires()
	if time.Now().After(expiryTime) {
		return nil, fmt.Errorf("license expired on %s", expiryTime.Format(time.RFC3339))
	}

	var orgStr, licenseTypeStr string
	switch t := l.(type) {
	case *license.V0RedpandaLicense:
		orgStr = t.Organization
		licenseTypeStr = t.Type.String()
	case *license.V1RedpandaLicense:
		orgStr = t.Organization
		licenseTypeStr = string(t.Type)
	}

	s.logger.With(
		"license_org", orgStr,
		"license_type", licenseTypeStr,
		"expires_at", expiryTime.Format(time.RFC3339),
	).Info("Successfully loaded Redpanda license")

	return l, nil
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
		s.logger.Debug("Loading Redpanda Enterprise license from explicit file path")

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

	s.logger.Debug("Loaded Redpanda Enterprise license from default file path")
	return
}
