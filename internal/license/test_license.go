// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package license

import (
	"time"

	"github.com/redpanda-data/common-go/license"
)

// embeddedTestLicense is compiled into the self-hosted binary and activates
// when no production license is present. It grants enterprise feature access
// under a 1 MB/s throughput cap (see Throttler).
var embeddedTestLicense license.RedpandaLicense = &license.V1RedpandaLicense{
	Version:      1,
	Organization: "embedded-test",
	Type:         license.LicenseTypeEnterprise,
	// Far-future expiry — this license is only a developer convenience, not a
	// security boundary, so we don't impose an artificial time limit.
	Expiry:   time.Date(2126, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	Products: []license.Product{license.ProductConnect},
}
