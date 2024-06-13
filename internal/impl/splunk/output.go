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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	soFieldURL             = "url"
	soFieldToken           = "token"
	soFieldGzip            = "gzip"
	soFieldEventHost       = "event_host"
	soFieldEventSource     = "event_source"
	soFieldEventSourceType = "event_sourcetype"
	soFieldEventIndex      = "event_index"
	soFieldSkipCertVerify  = "skip_cert_verify"
	soFieldBatching        = "batching"
)

//------------------------------------------------------------------------------

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("4.29.1").
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
			service.NewBoolField(soFieldSkipCertVerify).Description("Whether to skip server side certificate verification.").Advanced().Default(false),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(soFieldBatching),
		)
}

func init() {
	err := service.RegisterBatchOutput("splunk_hec", outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(soFieldBatching); err != nil {
				return
			}
			out, err = outputFromParsed(conf, mgr.Logger())
			return
		})
	if err != nil {
		panic(err)
	}
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

	var skipCertVerify bool
	if skipCertVerify, err = pConf.FieldBool(soFieldSkipCertVerify); err != nil {
		return
	}

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig.InsecureSkipVerify = skipCertVerify
	o.client = http.Client{Transport: tr}

	return
}

//------------------------------------------------------------------------------

func (o *output) Connect(_ context.Context) error { return nil }

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
		return fmt.Errorf("failed to construct http request: %s", err)
	}
	req.Header = header
	req.ContentLength = int64(payload.Len())

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute http request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if body, err := httputil.DumpResponse(resp, true); err != nil {
			return fmt.Errorf("failed to read response body: %s", err)
		} else {
			o.log.Debugf("Failed to fetch data from Splunk with status %d: %s", resp.StatusCode, string(body))
		}

		return fmt.Errorf("http request returned status: %d", resp.StatusCode)
	}

	return
}

func (o *output) Close(_ context.Context) error { return nil }
