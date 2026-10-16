// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
package splunk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const testBadURL = "http://u:S3cretPass@bad host:8088/services/collector/event"

func TestOutputRequestErrorRedacted(t *testing.T) {
	pConf, err := outputSpec().ParseYAML(`
url: "`+testBadURL+`"
token: tok
event_host: h
event_source: s
event_sourcetype: st
event_index: i
`, nil)
	require.NoError(t, err)

	o, err := outputFromParsed(pConf, service.MockResources().Logger())
	require.NoError(t, err)

	err = o.WriteBatch(t.Context(), service.MessageBatch{service.NewMessage([]byte(`{"a":1}`))})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}

func TestInputRequestErrorRedacted(t *testing.T) {
	pConf, err := inputSpec().ParseYAML(`
url: "`+testBadURL+`"
user: u
password: p
query: q
`, nil)
	require.NoError(t, err)

	i, err := inputFromParsed(pConf, service.MockResources().Logger())
	require.NoError(t, err)

	err = i.Connect(t.Context())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}
