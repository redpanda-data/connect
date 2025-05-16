// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

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

	franz_sr "github.com/twmb/franz-go/pkg/sr"

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
	service.MustRegisterInput("schema_registry", schemaRegistryInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := inputFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
		})

}

type schemaRegistryInput struct {
	subjectFilter  *regexp.Regexp
	fetchInOrder   bool
	includeDeleted bool

	client    *sr.Client
	connMut   sync.Mutex
	connected bool
	subjects  []string
	subject   string
	versions  []int
	schemas   []franz_sr.SubjectSchema
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

	if i.includeDeleted, err = pConf.FieldBool(sriFieldIncludeDeleted); err != nil {
		return
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
		mgr.SetGeneric(srResourceKey(mgr.Label()), i)
	} else {
		mgr.SetGeneric(srResourceKey(sriResourceDefaultLabel), i)
	}

	return
}

//------------------------------------------------------------------------------

func (i *schemaRegistryInput) Connect(ctx context.Context) error {
	i.connMut.Lock()
	defer i.connMut.Unlock()

	subjects, err := i.client.GetSubjects(ctx, i.includeDeleted)
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
		schemas := map[int][]franz_sr.SubjectSchema{}
		for _, subject := range i.subjects {
			var versions []int
			if versions, err = i.client.GetVersionsForSubject(ctx, subject, i.includeDeleted); err != nil {
				return fmt.Errorf("failed to fetch versions for subject %q: %s", subject, err)
			}
			if len(versions) == 0 {
				i.mgr.Logger().Infof("Subject %q does not contain any versions", subject)
				continue
			}

			for _, version := range versions {
				var schema franz_sr.SubjectSchema
				if schema, err = i.client.GetSchemaBySubjectAndVersion(ctx, subject, &version, i.includeDeleted); err != nil {
					return fmt.Errorf("failed to fetch schema version %d for subject %q: %s", version, subject, err)
				}

				schemas[schema.ID] = append(schemas[schema.ID], schema)
			}
		}

		// Sort schemas by ID to ensure that schemas with references are sent in the correct order.
		schemaIDs := make([]int, 0, len(schemas))
		for id := range schemas {
			schemaIDs = append(schemaIDs, id)
		}
		sort.Ints(schemaIDs)

		i.schemas = make([]franz_sr.SubjectSchema, 0, len(schemas))
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

	var si franz_sr.SubjectSchema
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
			if i.versions, err = i.client.GetVersionsForSubject(ctx, i.subject, i.includeDeleted); err != nil {
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

		var err error
		if si, err = i.client.GetSchemaBySubjectAndVersion(ctx, i.subject, &version, i.includeDeleted); err != nil {
			return nil, nil, fmt.Errorf("failed to fetch schema version %d for subject %q: %s", version, i.subject, err)
		}
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
