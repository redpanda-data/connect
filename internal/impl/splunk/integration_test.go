// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package splunk

import (
	"crypto/tls"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

func TestIntegrationSplunk(t *testing.T) {
	integration.CheckSkip(t)

	dummySplunkPassword := "blobfishAreC00l!"
	containerInputPort := "8089/tcp"
	containerOutputPort := "8088/tcp"

	// Splunk uses Ansible internally on startup and can take well over a minute
	// to become ready.
	startupTimeout := 10 * time.Minute
	if deadline, ok := t.Deadline(); ok {
		startupTimeout = time.Until(deadline) - 100*time.Millisecond
	}

	// WithStartupTimeout must be set on the strategy itself: the deadline passed
	// to WithWaitStrategyAndDeadline only bounds the outer wait.ForAll, while the
	// inner HTTP strategy otherwise falls back to its 60s default (which caps the
	// wait via context.WithTimeout and wins over the longer outer deadline). See
	// CON-376.
	ctr, err := testcontainers.Run(t.Context(), "splunk/splunk:9.3.3",
		testcontainers.WithImagePlatform("linux/amd64"),
		testcontainers.WithExposedPorts(containerInputPort, containerOutputPort),
		testcontainers.WithEnv(map[string]string{
			"SPLUNK_START_ARGS": "--accept-license",
			"SPLUNK_PASSWORD":   dummySplunkPassword,
			"SPLUNK_HEC_TOKEN":  dummySplunkPassword,
		}),
		testcontainers.WithWaitStrategyAndDeadline(startupTimeout,
			wait.ForHTTP("/services/collector/health").
				WithPort(containerOutputPort).
				WithStartupTimeout(startupTimeout).
				WithTLS(true, &tls.Config{InsecureSkipVerify: true}). //nolint:gosec
				WithResponseMatcher(func(body io.Reader) bool {
					b, err := io.ReadAll(body)
					if err != nil {
						return false
					}
					return string(b) == `{"text":"HEC is healthy","code":17}`
				}).
				WithPollInterval(2*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	inputPortM, err := ctr.MappedPort(t.Context(), containerInputPort)
	require.NoError(t, err)
	serviceInputPort := inputPortM.Port()

	outputPortM, err := ctr.MappedPort(t.Context(), containerOutputPort)
	require.NoError(t, err)
	serviceOutputPort := outputPortM.Port()

	t.Run("splunk_hec output -> input roundtrip", func(t *testing.T) {
		template := `
output:
  broker:
    pattern: fan_out_sequential
    outputs:
      - splunk_hec:
          url: https://127.0.0.1:$VAR2/services/collector/event
          token: "$VAR3"
          gzip: false
          event_host: "blobhost"
          event_source: "blobsource"
          event_sourcetype: "blobsourcetype"
          event_index: "main"
          tls:
            enabled: true
            skip_cert_verify: true
        processors:
          - mapping: |
              root = {
                "data": content().string(),
                "id": "$ID"
              }
      - drop: {}
        processors:
          - sleep:
              # Need to wait a bit for the Splunk emulator to persist the data... :(
              duration: 5s

input:
  splunk:
    url: https://127.0.0.1:$VAR1/services/search/v2/jobs/export
    user: admin
    password: "$VAR3"
    query: |
      index="main" earliest=-5m@m latest=now id=$ID
    tls:
      enabled: true
      skip_cert_verify: true
  processors:
    - mapping: |
        root = this.result._raw.parse_json().data
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptVarSet("VAR1", serviceInputPort),
			integration.StreamTestOptVarSet("VAR2", serviceOutputPort),
			integration.StreamTestOptVarSet("VAR3", dummySplunkPassword),
			integration.StreamTestOptOnResourcesInit(func(res *service.Resources) error {
				license.InjectTestService(res)
				return nil
			}),
		)
	})
}
