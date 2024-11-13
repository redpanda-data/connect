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
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"

	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

const (
	sroFieldURL                  = "url"
	sroFieldSubject              = "subject"
	sroFieldBackfillDependencies = "backfill_dependencies"
	sroFieldInputResource        = "input_resource"
	sroFieldTLS                  = "tls"

	sroResourceDefaultLabel = "schema_registry_output"
)

//------------------------------------------------------------------------------

func schemaRegistryOutputSpec() *service.ConfigSpec {
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
		service.NewBoolField(sroFieldBackfillDependencies).Description("Backfill schema references and previous versions.").Default(true).Advanced(),
		service.NewStringField(sroFieldInputResource).
			Description("The label of the schema_registry input from which to read source schemas.").
			Default(sriResourceDefaultLabel).
			Advanced(),
		service.NewTLSToggledField(sroFieldTLS),
		service.NewOutputMaxInFlightField(),
	},
		service.NewHTTPRequestAuthSignerFields()...,
	)
}

func init() {
	err := service.RegisterOutput("schema_registry", schemaRegistryOutputSpec(),
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
	subject              *service.InterpolatedString
	backfillDependencies bool
	inputResource        srResourceKey

	client      *sr.Client
	inputClient *sr.Client
	connected   atomic.Bool
	mgr         *service.Resources
	// Stores <SchemaID, SchemaVersionID, Subject> as key and destination SchemaID as value.
	schemaLineageCache sync.Map
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

	if o.backfillDependencies, err = pConf.FieldBool(sroFieldBackfillDependencies); err != nil {
		return
	}

	if o.backfillDependencies {
		var res string
		if res, err = pConf.FieldString(sroFieldInputResource); err != nil {
			return nil, err
		}
		o.inputResource = srResourceKey(res)
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

	if label := mgr.Label(); label != "" {
		mgr.SetGeneric(srResourceKey(mgr.Label()), o)
	} else {
		mgr.SetGeneric(srResourceKey(sroResourceDefaultLabel), o)
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

	if o.backfillDependencies {
		if res, ok := o.mgr.GetGeneric(o.inputResource); ok {
			o.inputClient = res.(*schemaRegistryInput).client
		} else {
			return fmt.Errorf("input resource %q not found", o.inputResource)
		}
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
		return fmt.Errorf("failed subject interpolation: %s", err)
	}

	var payload []byte
	if payload, err = m.AsBytes(); err != nil {
		return fmt.Errorf("failed to extract message bytes: %s", err)
	}

	var sd franz_sr.SubjectSchema
	if err := json.Unmarshal(payload, &sd); err != nil {
		return fmt.Errorf("failed to unmarshal schema details: %s", err)
	}
	// Populate the subject from the metadata.
	sd.Subject = subject

	destinationID, err := o.getOrCreateSchemaID(ctx, sd)
	if err != nil {
		return err
	}

	o.mgr.Logger().Debugf("Schema for subject %q created with ID %d", subject, destinationID)

	return nil
}

func (o *schemaRegistryOutput) Close(_ context.Context) error {
	o.connected.Store(false)

	return nil
}

//------------------------------------------------------------------------------

// GetDestinationSchemaID attempts to fetch the schema ID for the provided source schema ID and subject. It will first
// migrate it to the destination Schema Registry if it doesn't exist there yet.
func (o *schemaRegistryOutput) GetDestinationSchemaID(ctx context.Context, id int, subject string) (int, error) {
	schema, err := o.inputClient.GetSchemaByIDAndSubject(ctx, id, subject, false)
	if err != nil {
		return -1, fmt.Errorf("failed to get schema for ID %d and subject %q: %s", id, subject, err)
	}

	latestVersion, err := o.inputClient.GetLatestSchemaVersionForSchemaIDAndSubject(ctx, id, subject)
	if err != nil {
		return -1, fmt.Errorf("failed to get schema for ID %d and subject %q: %s", id, subject, err)
	}

	return o.getOrCreateSchemaID(
		ctx,
		franz_sr.SubjectSchema{
			Subject: subject,
			Version: latestVersion,
			ID:      id,
			Schema:  schema,
		},
	)
}

// schemaLineageCacheKey is used as a lightweight key for the schema ID map cache so we don't store the full schemas in
// memory.
type schemaLineageCacheKey struct {
	id        int
	versionID int
	subject   string
}

// getOrCreateSchemaID attempts to fetch the schema ID for the provided schema subject and payload from the cache or the
// configured Schema Registry output if present. Otherwise, it creates it, caches it and returns the generated ID.
func (o *schemaRegistryOutput) getOrCreateSchemaID(ctx context.Context, ss franz_sr.SubjectSchema) (int, error) {
	key := schemaLineageCacheKey{
		id:        ss.ID,
		versionID: ss.Version,
		subject:   ss.Subject,
	}
	if destinationID, ok := o.schemaLineageCache.Load(key); ok {
		return destinationID.(int), nil
	}

	if o.backfillDependencies {
		if err := o.createSchemaDeps(ctx, ss, true); err != nil {
			return 0, fmt.Errorf("failed to backfill dependencies for schema with subject %q and version %d: %s", ss.Subject, ss.Version, err)
		}
	}

	return o.createSchema(ctx, key, ss)
}

// createSchemaDeps creates and caches all the dependencies of the current schema (both references and previous versions).
func (o *schemaRegistryOutput) createSchemaDeps(ctx context.Context, ss franz_sr.SubjectSchema, backfillPrevVersions bool) error {
	key := schemaLineageCacheKey{
		id:        ss.ID,
		versionID: ss.Version,
		subject:   ss.Subject,
	}
	if _, ok := o.schemaLineageCache.Load(key); ok {
		return nil
	}

	// Backfill references recursively.
	for _, ref := range ss.References {
		schema, err := o.inputClient.GetSchemaBySubjectAndVersion(ctx, ref.Subject, &ref.Version, false)
		if err != nil {
			return fmt.Errorf("failed to get schema for subject %q with version %d: %s", ref.Subject, ref.Version, err)
		}

		if err := o.createSchemaDeps(ctx, schema, true); err != nil {
			return fmt.Errorf("failed to create schema dependencies: %s", err)
		}
	}

	// Backfill previous schema versions in ascending order.
	if ss.Version > 1 && backfillPrevVersions {
		versions, err := o.inputClient.GetVersionsForSubject(ctx, ss.Subject, false)
		if err != nil {
			return fmt.Errorf("failed to get schema versions for subject %q: %s", ss.Subject, err)
		}

		slices.Sort(versions)
		for _, version := range versions {
			schema, err := o.inputClient.GetSchemaBySubjectAndVersion(ctx, ss.Subject, &version, false)
			if err != nil {
				return fmt.Errorf("failed to get schema for subject %q with version %d: %s", ss.Subject, version, err)
			}

			if err := o.createSchemaDeps(ctx, schema, false); err != nil {
				return fmt.Errorf("failed to create schema dependencies: %s", err)
			}
		}
	}

	if _, err := o.createSchema(ctx, key, ss); err != nil {
		return fmt.Errorf("failed to create schema: %s", err)
	}

	return nil
}

// createSchema creates and caches the provided schema.
func (o *schemaRegistryOutput) createSchema(ctx context.Context, key schemaLineageCacheKey, ss franz_sr.SubjectSchema) (int, error) {
	if destinationID, ok := o.schemaLineageCache.Load(key); ok {
		return destinationID.(int), nil
	}

	// TODO: Use `CreateSchemaWithID()` when `translate_ids: false` after https://github.com/twmb/franz-go/pull/849
	// is merged.

	// This should return the destination ID without an error if the schema already exists.
	destinationID, err := o.client.CreateSchema(ctx, ss.Subject, ss.Schema)
	if err != nil {
		return -1, fmt.Errorf("failed to create schema for subject %q and version %d: %s", ss.Subject, ss.Version, err)
	}

	// Cache the schema along with the destination ID.
	o.schemaLineageCache.Store(key, destinationID)

	return destinationID, nil
}
