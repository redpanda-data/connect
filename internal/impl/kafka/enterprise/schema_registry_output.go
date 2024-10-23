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

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

const (
	sroFieldURL                  = "url"
	sroFieldSubject              = "subject"
	sroFieldBackfillDependencies = "backfill_dependencies"
	sroFieldTranslateIDs         = "translate_ids"
	sroFieldInputResource        = "input_resource"
	sroFieldTLS                  = "tls"

	sroResourceDefaultLabel = "schema_registry_output"
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
		service.NewBoolField(sroFieldBackfillDependencies).Description("Backfill schema references and previous versions.").Default(true).Advanced(),
		service.NewBoolField(sroFieldTranslateIDs).Description("Translate schema IDs.").Default(true).Advanced(),
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
	subject              *service.InterpolatedString
	backfillDependencies bool
	translateIDs         bool
	inputResource        string

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

	if o.translateIDs, err = pConf.FieldBool(sroFieldTranslateIDs); err != nil {
		return
	}

	if o.backfillDependencies || o.translateIDs {
		if o.inputResource, err = pConf.FieldString(sroFieldInputResource); err != nil {
			return nil, err
		}
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
		mgr.SetGeneric(mgr.Label(), o)
	} else {
		mgr.SetGeneric(sroResourceDefaultLabel, o)
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

	if o.backfillDependencies || o.translateIDs {
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

	var sd schemaDetails
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
	schema, err := o.inputClient.GetSchemaByIDAndSubject(ctx, id, subject)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema for ID %d and subject %q: %s", id, subject, err)
	}

	latestVersion, err := o.inputClient.GetLatestSchemaVersionForSchemaIDAndSubject(ctx, id, subject)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema for ID %d and subject %q: %s", id, subject, err)
	}

	schema.ID = id
	return o.getOrCreateSchemaID(
		ctx,
		schemaDetails{
			SchemaInfo: schema,
			Subject:    subject,
			Version:    latestVersion,
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
func (o *schemaRegistryOutput) getOrCreateSchemaID(ctx context.Context, sd schemaDetails) (int, error) {
	key := schemaLineageCacheKey{
		id:        sd.ID,
		versionID: sd.Version,
		subject:   sd.Subject,
	}
	if destinationID, ok := o.schemaLineageCache.Load(key); ok {
		return destinationID.(int), nil
	}

	if o.backfillDependencies {
		if err := o.createSchemaDeps(ctx, sd, true); err != nil {
			return 0, fmt.Errorf("failed to backfill dependencies for schema with subject %q and version %d: %s", sd.Subject, sd.Version, err)
		}
	}

	return o.createSchema(ctx, key, sd)
}

// createSchemaDeps creates and caches all the dependencies of the current schema (both references and previous versions).
func (o *schemaRegistryOutput) createSchemaDeps(ctx context.Context, sd schemaDetails, backfillPrevVersions bool) error {
	key := schemaLineageCacheKey{
		id:        sd.ID,
		versionID: sd.Version,
		subject:   sd.Subject,
	}
	if _, ok := o.schemaLineageCache.Load(key); ok {
		return nil
	}

	// Backfill references recursively.
	for _, ref := range sd.References {
		schema, err := o.inputClient.GetSchemaBySubjectAndVersion(ctx, ref.Subject, &ref.Version)
		if err != nil {
			return fmt.Errorf("failed to get schema for subject %q with version %d: %s", ref.Subject, ref.Version, err)
		}

		if err := o.createSchemaDeps(ctx, schemaDetails{SchemaInfo: schema, Subject: ref.Subject, Version: ref.Version}, true); err != nil {
			return fmt.Errorf("failed to create schema dependencies: %s", err)
		}
	}

	// Backfill previous schema versions in ascending order.
	if sd.Version > 1 && backfillPrevVersions {
		versions, err := o.inputClient.GetVersionsForSubject(ctx, sd.Subject)
		if err != nil {
			return fmt.Errorf("failed to get schema versions for subject %q: %s", sd.Subject, err)
		}

		slices.Sort(versions)
		for _, version := range versions {
			schema, err := o.inputClient.GetSchemaBySubjectAndVersion(ctx, sd.Subject, &version)
			if err != nil {
				return fmt.Errorf("failed to get schema for subject %q with version %d: %s", sd.Subject, version, err)
			}

			if err := o.createSchemaDeps(ctx, schemaDetails{SchemaInfo: schema, Subject: sd.Subject, Version: version}, false); err != nil {
				return fmt.Errorf("failed to create schema dependencies: %s", err)
			}
		}
	}

	if _, err := o.createSchema(ctx, key, sd); err != nil {
		return fmt.Errorf("failed to create schema: %s", err)
	}

	return nil
}

// createSchema creates and caches the provided schema.
func (o *schemaRegistryOutput) createSchema(ctx context.Context, key schemaLineageCacheKey, sd schemaDetails) (int, error) {
	if destinationID, ok := o.schemaLineageCache.Load(key); ok {
		return destinationID.(int), nil
	}

	if o.translateIDs {
		// Remove the ID from the schema to ensure that a new ID is allocated for it.
		sd.ID = 0

		// TODO: Should we also remove the version ID? I guess it's unlikely the new schemas we're trying to push are
		// compatible with any existing ones anyway, so it's probably better to just emit an error if the version ID
		// exists already.
	}

	payload, err := json.Marshal(sd)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal schema with subject %q and version %d: %s", sd.Subject, sd.Version, err)
	}

	// This should return the destination ID without an error if the schema already exists.
	destinationID, err := o.client.CreateSchema(ctx, sd.Subject, payload)
	if err != nil {
		return 0, fmt.Errorf("failed to create schema for subject %q and version %d: %s", sd.Subject, sd.Version, err)
	}

	// Cache the schema along with the destination ID.
	o.schemaLineageCache.Store(key, destinationID)

	return destinationID, nil
}
