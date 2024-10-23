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
	"regexp"
	"sort"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

const (
	sriFieldURL            = "url"
	sriFieldIncludeDeleted = "include_deleted"
	sriFieldFetchInOrder   = "fetch_in_order"
	sriFieldSubjectFilter  = "subject_filter"
	sriFieldTLS            = "tls"

	sriResourceDefaultLabel = "schema_registry_input"
)

//------------------------------------------------------------------------------

func schemaRegistryInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("4.32.2").
		Categories("Integration").
		Summary(`Reads schemas from SchemaRegistry.`).
		Description(`
== Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- schema_registry_subject
- schema_registry_version
`+"```"+`

You can access these metadata fields using
xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].

`).
		Fields(
			schemaRegistryInputConfigFields()...,
		).Example("Read schemas", "Read all schemas (including deleted) from a Schema Registry instance which are associated with subjects matching the `^foo.*` filter.", `
input:
  schema_registry:
    url: http://localhost:8081
    include_deleted: true
    subject_filter: ^foo.*
`)
}

func schemaRegistryInputConfigFields() []*service.ConfigField {
	return append([]*service.ConfigField{
		service.NewStringField(sriFieldURL).Description("The base URL of the schema registry service."),
		service.NewBoolField(sriFieldIncludeDeleted).Description("Include deleted entities.").Default(false).Advanced(),
		service.NewStringField(sriFieldSubjectFilter).Description("Include only subjects which match the regular expression filter. All subjects are selected when not set.").Default("").Advanced(),
		service.NewBoolField(sriFieldFetchInOrder).Description("Fetch all schemas on connect and sort them by ID. Should be set to `true` when schema references are used.").Default(true).Advanced().Version("4.37.0"),
		service.NewTLSToggledField(sriFieldTLS),
		service.NewAutoRetryNacksToggleField(),
	},
		service.NewHTTPRequestAuthSignerFields()...,
	)
}

func init() {
	err := service.RegisterInput("schema_registry", schemaRegistryInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := inputFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
		})
	if err != nil {
		panic(err)
	}
}

type schemaRegistryInput struct {
	subjectFilter *regexp.Regexp
	fetchInOrder  bool

	client    *sr.Client
	connMut   sync.Mutex
	connected bool
	subjects  []string
	subject   string
	versions  []int
	schemas   []schemaDetails
	mgr       *service.Resources
}

func inputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (i *schemaRegistryInput, err error) {
	i = &schemaRegistryInput{
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

	var includeDeleted bool
	if includeDeleted, err = pConf.FieldBool(sriFieldIncludeDeleted); err != nil {
		return
	}
	if includeDeleted {
		q := srURL.Query()
		q.Add("deleted", "true")
		srURL.RawQuery = q.Encode()
	}

	if i.fetchInOrder, err = pConf.FieldBool(sriFieldFetchInOrder); err != nil {
		return
	}

	var filter string
	if filter, err = pConf.FieldString(sriFieldSubjectFilter); err != nil {
		return
	}
	if i.subjectFilter, err = regexp.Compile(filter); err != nil {
		return nil, fmt.Errorf("failed to compile subject filter %q: %s", filter, err)
	}

	var reqSigner func(f fs.FS, req *http.Request) error
	if reqSigner, err = pConf.HTTPRequestAuthSignerFromParsed(); err != nil {
		return nil, err
	}

	var tlsConf *tls.Config
	var tlsEnabled bool
	if tlsConf, tlsEnabled, err = pConf.FieldTLSToggled(sriFieldTLS); err != nil {
		return
	}

	if !tlsEnabled {
		tlsConf = nil
	}
	if i.client, err = sr.NewClient(srURL.String(), reqSigner, tlsConf, mgr); err != nil {
		return nil, fmt.Errorf("failed to create Schema Registry client: %s", err)
	}

	if label := mgr.Label(); label != "" {
		mgr.SetGeneric(mgr.Label(), i)
	} else {
		mgr.SetGeneric(sriResourceDefaultLabel, i)
	}

	return
}

//------------------------------------------------------------------------------

func (i *schemaRegistryInput) Connect(ctx context.Context) error {
	i.connMut.Lock()
	defer i.connMut.Unlock()

	subjects, err := i.client.GetSubjects(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch subjects: %s", err)
	}

	i.subjects = make([]string, 0, len(subjects))
	for _, s := range subjects {
		if i.subjectFilter.MatchString(s) {
			i.subjects = append(i.subjects, s)
		}
	}

	if i.fetchInOrder {
		schemas := map[int][]schemaDetails{}
		for _, subject := range i.subjects {
			var versions []int
			if versions, err = i.client.GetVersionsForSubject(ctx, subject); err != nil {
				return fmt.Errorf("failed to fetch versions for subject %q: %s", subject, err)
			}
			if len(versions) == 0 {
				i.mgr.Logger().Infof("Subject %q does not contain any versions", subject)
				continue
			}

			for _, version := range versions {
				var schema sr.SchemaInfo
				if schema, err = i.client.GetSchemaBySubjectAndVersion(ctx, subject, &version); err != nil {
					return fmt.Errorf("failed to fetch schema version %d for subject %q: %s", version, subject, err)
				}

				si := schemaDetails{
					SchemaInfo: schema,
					Subject:    subject,
					Version:    version,
				}

				schemas[schema.ID] = append(schemas[schema.ID], si)
			}
		}

		// Sort schemas by ID to ensure that schemas with references are sent in the correct order.
		schemaIDs := make([]int, 0, len(schemas))
		for id := range schemas {
			schemaIDs = append(schemaIDs, id)
		}
		sort.Ints(schemaIDs)

		i.schemas = make([]schemaDetails, 0, len(schemas))
		for _, id := range schemaIDs {
			i.schemas = append(i.schemas, schemas[id]...)
		}
	}

	i.connected = true

	return nil
}

func (i *schemaRegistryInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	i.connMut.Lock()
	defer i.connMut.Unlock()
	if !i.connected {
		return nil, nil, service.ErrNotConnected
	}

	var si schemaDetails
	if !i.fetchInOrder {
		for {
			if len(i.subjects) == 0 && len(i.versions) == 0 {
				return nil, nil, service.ErrEndOfInput
			}

			if len(i.versions) != 0 {
				break
			}

			i.subject = i.subjects[0]

			var err error
			if i.versions, err = i.client.GetVersionsForSubject(ctx, i.subject); err != nil {
				return nil, nil, fmt.Errorf("failed to fetch versions for subject %q: %s", i.subject, err)
			}

			i.subjects = i.subjects[1:]

			if len(i.versions) == 0 {
				i.mgr.Logger().Infof("Subject %q does not contain any versions", i.subject)
				continue
			}

			break
		}

		version := i.versions[0]
		defer func() {
			i.versions = i.versions[1:]
		}()

		var schema sr.SchemaInfo
		var err error
		if schema, err = i.client.GetSchemaBySubjectAndVersion(ctx, i.subject, &version); err != nil {
			return nil, nil, fmt.Errorf("failed to fetch schema version %d for subject %q: %s", version, i.subject, err)
		}

		si.SchemaInfo = schema
		si.Subject = i.subject
		si.Version = version
	} else {
		if len(i.schemas) == 0 {
			return nil, nil, service.ErrEndOfInput
		}

		si = i.schemas[0]
		defer func() {
			i.schemas = i.schemas[1:]
		}()
	}

	schema, err := json.Marshal(si)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal schema to json for subject %q version %d: %s", i.subject, si.Version, err)
	}

	msg := service.NewMessage(schema)

	msg.MetaSetMut("schema_registry_subject", si.Subject)
	msg.MetaSetMut("schema_registry_version", si.Version)

	return msg, func(ctx context.Context, err error) error {
		// Nacks are handled by AutoRetryNacks because we don't have an explicit
		// ack mechanism right now.
		return nil
	}, nil
}

func (i *schemaRegistryInput) Close(ctx context.Context) error {
	i.connMut.Lock()
	defer i.connMut.Unlock()

	i.connected = false

	return nil
}
