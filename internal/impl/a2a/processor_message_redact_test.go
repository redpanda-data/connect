// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package a2a

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/serviceaccount"
)

func TestAgentCardFetchErrorRedacted(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"tok","token_type":"bearer","expires_in":3600}`))
	}))
	t.Cleanup(tokenServer.Close)
	// The service account is process global and can't be reset from here, so
	// it stays set for the rest of this package's tests.
	require.NoError(t, serviceaccount.InitGlobal(t.Context(), tokenServer.URL, "id", "secret", ""))

	mgr := service.MockResources()
	license.InjectTestService(mgr)

	conf, err := processorConfig().ParseYAML(`
agent_card_url: "http://u:S3cretPass@127.0.0.1:1"
`, nil)
	require.NoError(t, err)

	_, err = makeProcessor(conf, mgr)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}
