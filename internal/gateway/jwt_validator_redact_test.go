// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
package gateway_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/gateway"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestJWTIssuerURLParseErrorRedacted(t *testing.T) {
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL", "http://u:S3cretPass@bad host:1234")
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE", "foo")
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID", "bar")

	mgr := service.MockResources()
	license.InjectTestService(mgr)

	_, err := gateway.NewRPJWTMiddleware(mgr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing gateway JWT issuer URL")
	assert.NotContains(t, err.Error(), "S3cretPass")
}
