// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/httpclient"
	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
)

// newSalesforceHTTPClient constructs the shared httpclient used by both the
// processor and output. No RetryConfig is passed, so the retry transport
// operates in adaptive 429-only mode: it retries on 429 (rate limit) and
// network errors only. 401 (expired token) is intentionally not retried here
// — it passes through to salesforcehttp.Client.withAuth(), which refreshes
// the OAuth2 token and retries the request once. The two layers handle
// disjoint status codes.
func newSalesforceHTTPClient(orgURL string, timeout time.Duration, maxRetries int, mgr *service.Resources) (salesforcehttp.HTTPDoer, error) {
	cfg := httpclient.Config{
		BaseURL:                orgURL,
		Timeout:                timeout,
		BackoffMaxRetries:      maxRetries,
		BackoffInitialInterval: 500 * time.Millisecond,
		BackoffMaxInterval:     30 * time.Second,
		Transport:              httpclient.DefaultTransportConfig(),
		MetricPrefix:           "salesforce_http",
	}
	c, err := httpclient.NewClient(cfg, mgr)
	if err != nil {
		return nil, fmt.Errorf("create HTTP client: %w", err)
	}
	return c, nil
}
