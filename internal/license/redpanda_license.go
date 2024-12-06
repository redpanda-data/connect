// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package license

import (
	"fmt"
	"time"
)

// RedpandaLicense is the payload that will be decoded from a license file.
type RedpandaLicense struct {
	Version      int    `json:"version"`
	Organization string `json:"org"`

	// 0 = FreeTrial; 1 = Enterprise
	Type int `json:"type"`

	// Unix epoch
	Expiry int64 `json:"expiry"`
}

// AllowsEnterpriseFeatures returns true if license type allows enterprise features.
func (r *RedpandaLicense) AllowsEnterpriseFeatures() bool {
	// Right now any enterprise or trial license that was valid when we started
	// is considered valid here.
	return r.Type == 1 || r.Type == 0
}

func typeDisplayName(t int) string {
	switch t {
	case 0:
		return "free trial"
	case 1:
		return "enterprise"
	default:
		return "open source"
	}
}

// CheckExpiry returns nil if the license is still valid (not expired). Otherwise,
// it will return an error that provides context when the license expired.
func (r *RedpandaLicense) CheckExpiry() error {
	expires := time.Unix(r.Expiry, 0)
	if expires.Before(time.Now().UTC()) {
		return fmt.Errorf("license expired on %q", expires.Format(time.RFC3339))
	}
	return nil
}
