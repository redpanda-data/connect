// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
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
	subject *service.InterpolatedString

	client    *sr.Client
	connected atomic.Bool
	mgr       *service.Resources
}

func outputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (o *schemaRegistryOutput, err error) {
	o = &schemaRegistryOutput{
		mgr: mgr,
	}

	var srURLStr string
	if srURLStr, err = pConf.FieldString(sriFieldURL); err != nil {
		return
	}
	var srURL *url.URL
	if srURL, err = url.Parse(srURLStr); err != nil {
		return nil, fmt.Errorf("failed to parse URL: %s", err)
	}

	if o.subject, err = pConf.FieldInterpolatedString(sroFieldSubject); err != nil {
		return
	}

	var reqSigner func(f fs.FS, req *http.Request) error
	if reqSigner, err = pConf.HTTPRequestAuthSignerFromParsed(); err != nil {
		return nil, err
	}

	var tlsConf *tls.Config
	var tlsEnabled bool
	if tlsConf, tlsEnabled, err = pConf.FieldTLSToggled(sroFieldTLS); err != nil {
		return
	}

	if !tlsEnabled {
		tlsConf = nil
	}
	if o.client, err = sr.NewClient(srURL.String(), reqSigner, tlsConf, mgr); err != nil {
		return nil, fmt.Errorf("failed to create Schema Registry client: %s", err)
	}

	return
}

//------------------------------------------------------------------------------

func (o *schemaRegistryOutput) Connect(ctx context.Context) error {
	if o.connected.Load() {
		return nil
	}

	mode, err := o.client.GetMode(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch mode: %s", err)
	}

	if mode != "READWRITE" && mode != "IMPORT" {
		return fmt.Errorf("schema registry instance mode must be set to READWRITE or IMPORT instead of %q", mode)
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

	var payload []byte
	if payload, err = m.AsBytes(); err != nil {
		return fmt.Errorf("failed to extract message bytes: %w", err)
	}

	err = o.client.CreateSchema(ctx, subject, payload)
	if err != nil {
		return fmt.Errorf("failed to create schema for subject %q: %s", subject, err)
	}

	return nil
}

func (o *schemaRegistryOutput) Close(_ context.Context) error {
	o.connected.Store(false)

	return nil
}
