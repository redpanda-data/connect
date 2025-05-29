// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package splunk

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	soFieldURL             = "url"
	soFieldToken           = "token"
	soFieldGzip            = "gzip"
	soFieldEventHost       = "event_host"
	soFieldEventSource     = "event_source"
	soFieldEventSourceType = "event_sourcetype"
	soFieldEventIndex      = "event_index"
	soFieldTLS             = "tls"
	soFieldBatching        = "batching"

	// Deprecated fields
	soFieldSkipCertVerify = "skip_cert_verify"
	soFieldBatchCount     = "batching_count"
	soFieldBatchPeriod    = "batching_period"
	soFieldBatchByteSize  = "batching_byte_size"
	soFieldRateLimit      = "rate_limit"
)

//------------------------------------------------------------------------------

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("4.30.0").
		Categories("Services").
		Summary(`Publishes messages to a Splunk HTTP Endpoint Collector (HEC).`).
		Description(service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewStringField(soFieldURL).Description("Full HTTP Endpoint Collector (HEC) URL.").Example("https://foobar.splunkcloud.com/services/collector/event"),
			service.NewStringField(soFieldToken).Description("A bot token used for authentication.").Secret(),
			service.NewBoolField(soFieldGzip).Description("Enable gzip compression").Default(false),
			service.NewStringField(soFieldEventHost).Description("Set the host value to assign to the event data. Overrides existing host field if present.").Optional(),
			service.NewStringField(soFieldEventSource).Description("Set the source value to assign to the event data. Overrides existing source field if present.").Optional(),
			service.NewStringField(soFieldEventSourceType).Description("Set the sourcetype value to assign to the event data. Overrides existing sourcetype field if present.").Optional(),
			service.NewStringField(soFieldEventIndex).Description("Set the index value to assign to the event data. Overrides existing index field if present.").Optional(),
			service.NewTLSToggledField(soFieldTLS),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(soFieldBatching),

			// Old deprecated fields
			service.NewBoolField(soFieldSkipCertVerify).
				Optional().
				Deprecated(),
			service.NewIntField(soFieldBatchCount).
				Optional().
				Deprecated(),
			service.NewStringField(soFieldBatchPeriod).
				Optional().
				Deprecated(),
			service.NewIntField(soFieldBatchByteSize).
				Optional().
				Deprecated(),
			service.NewStringField(soFieldRateLimit).
				Optional().
				Deprecated(),
		)
}

func init() {
	service.MustRegisterBatchOutput("splunk_hec", outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if err = license.CheckRunningEnterprise(mgr); err != nil {
				return
			}

			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(soFieldBatching); err != nil {
				return
			}

			// Check for presence of deprecated fields
			if conf.Contains(soFieldBatchCount) {
				batchPolicy.Count, _ = conf.FieldInt(soFieldBatchCount)
			}
			if conf.Contains(soFieldBatchPeriod) {
				batchPolicy.Period, _ = conf.FieldString(soFieldBatchPeriod)
			}
			if conf.Contains(soFieldBatchByteSize) {
				batchPolicy.ByteSize, _ = conf.FieldInt(soFieldBatchByteSize)
			}

			out, err = outputFromParsed(conf, mgr.Logger())
			return
		})
}

type output struct {
	url                string
	token              string
	useGzipCompression bool
	eventHost          string
	eventSource        string
	eventSourceType    string
	eventIndex         string

	client http.Client
	log    *service.Logger
}

func outputFromParsed(pConf *service.ParsedConfig, log *service.Logger) (o *output, err error) {
	o = &output{
		log: log,
	}

	if o.url, err = pConf.FieldString(soFieldURL); err != nil {
		return
	}

	if o.token, err = pConf.FieldString(soFieldToken); err != nil {
		return
	}

	if o.useGzipCompression, err = pConf.FieldBool(soFieldGzip); err != nil {
		return
	}

	if o.eventHost, err = pConf.FieldString(soFieldEventHost); err != nil {
		return
	}

	if o.eventSource, err = pConf.FieldString(soFieldEventSource); err != nil {
		return
	}

	if o.eventSourceType, err = pConf.FieldString(soFieldEventSourceType); err != nil {
		return
	}

	if o.eventIndex, err = pConf.FieldString(soFieldEventIndex); err != nil {
		return
	}

	var tlsConf *tls.Config
	var tlsEnabled bool
	if tlsConf, tlsEnabled, err = pConf.FieldTLSToggled(soFieldTLS); err != nil {
		return
	}

	o.client = http.Client{}
	if tlsEnabled && tlsConf != nil {
		if c, ok := http.DefaultTransport.(*http.Transport); ok {
			cloned := c.Clone()
			cloned.TLSClientConfig = tlsConf
			o.client.Transport = cloned
		} else {
			o.client.Transport = &http.Transport{
				TLSClientConfig: tlsConf,
			}
		}
	}

	return
}

//------------------------------------------------------------------------------

func (*output) Connect(context.Context) error { return nil }

func (o *output) WriteBatch(ctx context.Context, b service.MessageBatch) (err error) {
	header := make(http.Header)
	header.Set("Content-Type", "application/json")
	header.Set("Authorization", "Splunk "+o.token)

	var payload bytes.Buffer
	var payloadWriter io.Writer = &payload
	var gzipFlusher func() error
	if o.useGzipCompression {
		header.Set("Content-Encoding", "gzip")
		gzipper := gzip.NewWriter(&payload)
		payloadWriter = gzipper
		gzipFlusher = gzipper.Close
	}
	encoder := json.NewEncoder(payloadWriter)

	for _, msg := range b {
		data, err := msg.AsStructuredMut()
		if err != nil {
			rawData, err := msg.AsBytes()
			if err != nil {
				return fmt.Errorf("failed to get message bytes: %s", err)
			}
			data = map[string]any{"event": string(rawData)}
		}

		var dataObj map[string]any
		var ok bool
		if dataObj, ok = data.(map[string]any); !ok {
			dataObj = map[string]any{"event": data}
		} else if _, ok := dataObj["event"]; !ok {
			dataObj = map[string]any{"event": data}
		}

		if o.eventHost != "" {
			dataObj["host"] = o.eventHost
		}
		if o.eventSource != "" {
			dataObj["source"] = o.eventSource
		}
		if o.eventSourceType != "" {
			dataObj["sourcetype"] = o.eventSourceType
		}
		if o.eventIndex != "" {
			dataObj["index"] = o.eventIndex
		}

		err = encoder.Encode(dataObj)
		if err != nil {
			return fmt.Errorf("failed to marshal message to json: %s", err)
		}
	}

	if o.useGzipCompression {
		if err := gzipFlusher(); err != nil {
			return fmt.Errorf("failed to compress messages: %s", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url, &payload)
	if err != nil {
		return fmt.Errorf("failed to construct HTTP request: %s", err)
	}
	req.Header = header
	req.ContentLength = int64(payload.Len())

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute http request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if respData, err := httputil.DumpResponse(resp, true); err != nil {
			return fmt.Errorf("failed to read response: %s", err)
		} else {
			o.log.Debugf("Failed to push data to Splunk with status %d: %s", resp.StatusCode, string(respData))
		}

		return fmt.Errorf("HTTP request returned status: %d", resp.StatusCode)
	}

	return
}

func (*output) Close(context.Context) error { return nil }
