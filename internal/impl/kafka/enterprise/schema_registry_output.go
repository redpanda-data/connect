// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	sroFieldURL     = "url"
	sroFieldSubject = "subject"
	sroFieldTLS     = "tls"
)

//------------------------------------------------------------------------------

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("4.32.2").
		Categories("Integration").
		Summary(`Publishes schemas to SchemaRegistry.`).
		Description(service.OutputPerformanceDocs(true, false)).
		Fields(
			schemaRegistryOutputConfigFields()...,
		).Example("Write schemas", "Write schemas to a Schema Registry instance and log errors for schemas which already exist.", `
output:
  fallback:
    - schema_registry:
        url: http://localhost:8082
        subject: ${! @schema_registry_subject }
    - switch:
        cases:
          - check: '@fallback_error == "request returned status: 422"'
            output:
              drop: {}
              processors:
                - log:
                    message: |
                      Subject '${! @schema_registry_subject }' version ${! @schema_registry_version } already has schema: ${! content() }
          - output:
              reject: ${! @fallback_error }
`)
}

func schemaRegistryOutputConfigFields() []*service.ConfigField {
	return append([]*service.ConfigField{
		service.NewStringField(sroFieldURL).Description("The base URL of the schema registry service."),
		service.NewInterpolatedStringField(sroFieldSubject).Description("Subject."),
		service.NewStringField(sroFieldURL).Description("The base URL of the schema registry service."),
		service.NewTLSToggledField(sroFieldTLS),
		service.NewOutputMaxInFlightField(),
	},
		service.NewHTTPRequestAuthSignerFields()...,
	)
}

func init() {
	err := service.RegisterOutput("schema_registry", outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}

			out, err = outputFromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type schemaRegistryOutput struct {
	url           *url.URL
	subject       *service.InterpolatedString
	requestSigner func(fs.FS, *http.Request) error

	client    http.Client
	connected atomic.Bool
	mgr       *service.Resources
}

func outputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (o *schemaRegistryOutput, err error) {
	o = &schemaRegistryOutput{
		mgr: mgr,
	}

	var u string
	if u, err = pConf.FieldString(sriFieldURL); err != nil {
		return
	}
	if o.url, err = url.Parse(u); err != nil {
		return nil, fmt.Errorf("failed to parse URL: %s", err)
	}

	if o.subject, err = pConf.FieldInterpolatedString(sroFieldSubject); err != nil {
		return
	}

	if o.requestSigner, err = pConf.HTTPRequestAuthSignerFromParsed(); err != nil {
		return nil, err
	}

	var tlsConf *tls.Config
	var tlsEnabled bool
	if tlsConf, tlsEnabled, err = pConf.FieldTLSToggled(sroFieldTLS); err != nil {
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

func (o *schemaRegistryOutput) Connect(ctx context.Context) error {
	if o.connected.Load() {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.url.JoinPath("mode").String(), nil)
	if err != nil {
		return fmt.Errorf("failed to construct mode HTTP request: %s", err)
	}

	if err = o.requestSigner(o.mgr.FS(), req); err != nil {
		return fmt.Errorf("failed to sign request: %s", err)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute mode HTTP request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received invalid status: %d", resp.StatusCode)
	}

	var payload struct {
		Mode string
	}
	if data, err := io.ReadAll(resp.Body); err != nil {
		return fmt.Errorf("failed to read mode HTTP request body: %s", err)
	} else {
		if err := json.Unmarshal(data, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal response: %s", err)
		}
	}

	if payload.Mode != "READWRITE" && payload.Mode != "IMPORT" {
		return fmt.Errorf("schema registry instance mode must be set to READWRITE or IMPORT instead of %q", payload.Mode)
	}

	o.connected.Store(true)

	return nil
}

func (o *schemaRegistryOutput) Write(ctx context.Context, m *service.Message) error {
	if !o.connected.Load() {
		return service.ErrNotConnected
	}

	var subject string
	var err error
	if subject, err = o.subject.TryString(m); err != nil {
		return fmt.Errorf("failed subject interpolation: %w", err)
	}

	var b []byte
	if b, err = m.AsBytes(); err != nil {
		return fmt.Errorf("failed to extract message bytes: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url.JoinPath("subjects", subject, "versions").String(), bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("failed to construct request: %s", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	if err = o.requestSigner(o.mgr.FS(), req); err != nil {
		return fmt.Errorf("failed to sign request: %s", err)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if respData, err := httputil.DumpResponse(resp, true); err != nil {
			return fmt.Errorf("failed to read response: %s", err)
		} else {
			o.mgr.Logger().Debugf("Failed to push data to SchemaRegistry with status %d: %s", resp.StatusCode, string(respData))
		}

		return fmt.Errorf("request returned status: %d", resp.StatusCode)
	}

	return nil
}

func (o *schemaRegistryOutput) Close(_ context.Context) error {
	o.connected.Store(false)

	return nil
}
