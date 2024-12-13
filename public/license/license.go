// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package license

import (
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

// LocateLicenseOptBuilder represents options specified for a license locator.
type LocateLicenseOptBuilder struct {
	c license.Config
}

// LocateLicenseOptFunc defines an option to pass through the LocateLicense
// function call in order to customize its behavior.
type LocateLicenseOptFunc func(*LocateLicenseOptBuilder)

// LocateLicense attempts to locate a Redpanda Enteprise license from the
// environment and, if successful, enriches the provided resources with
// information of this license that enterprise components may reference.
func LocateLicense(res *service.Resources, opts ...LocateLicenseOptFunc) {
	optBuilder := LocateLicenseOptBuilder{}
	for _, o := range opts {
		o(&optBuilder)
	}
	license.RegisterService(res, optBuilder.c)
}

// StoreCustomLicenseBytes attempts to parse a Redpanda Enterprise license
// from a slice of bytes and, if successful, stores it within the provided
// resources pointer for enterprise components to reference.
func StoreCustomLicenseBytes(res *service.Resources, licenseBytes []byte) error {
	return license.InjectCustomLicenseBytes(res, licenseBytes)
}
