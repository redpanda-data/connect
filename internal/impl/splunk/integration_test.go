// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package splunk

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

func TestIntegrationSplunk(t *testing.T) {
	// integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// A generous amount of time is required for this container to be up and running, since it uses Ansible to deploy
	// all sorts of stuff inside it on startup before finally launching various services...
	pool.MaxWait = 10 * time.Minute
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	dummySplunkPassword := "blobfishAreC00l!"
	containerInputPort := "8089/tcp"
	containerOutputPort := "8088/tcp"
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "splunk/splunk",
		Tag:        "9.1.1", // TODO: Update this after https://github.com/splunk/docker-splunk/issues/668 is fixed
		Env: []string{
			"SPLUNK_START_ARGS=--accept-license",
			"SPLUNK_PASSWORD=" + dummySplunkPassword,
			"SPLUNK_HEC_TOKEN=" + dummySplunkPassword,
		},
		ExposedPorts: []string{
			containerInputPort,
			containerOutputPort,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	serviceInputPort := resource.GetPort(containerInputPort)
	serviceOutputPort := resource.GetPort(containerOutputPort)

	err = pool.Retry(func() error {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig.InsecureSkipVerify = true
		client := http.Client{Transport: tr}
		resp, err := client.Get("https://localhost:" + serviceOutputPort + "//services/collector/health")
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed healthcheck with status: %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if string(body) != `{"text":"HEC is healthy","code":17}` {
			return fmt.Errorf("healthcheck returned invalid response: %s", body)
		}

		return nil
	})
	require.NoError(t, err, "Failed to start Splunk emulator")

	t.Run("splunk_hec output -> input roundtrip", func(t *testing.T) {
		template := `
output:
  broker:
    pattern: fan_out_sequential
    outputs:
      - splunk_hec:
          url: https://localhost:$VAR2/services/collector/event
          token: "$VAR3"
          gzip: false
          event_host: "blobhost"
          event_source: "blobsource"
          event_sourcetype: "blobsourcetype"
          event_index: "main"
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
    url: https://localhost:$VAR1/services/search/v2/jobs/export
    user: admin
    password: "$VAR3"
    query: |
      index="main" earliest=-5m@m latest=now id=$ID
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
		)
	})
}
