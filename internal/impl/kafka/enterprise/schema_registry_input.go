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
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	sriFieldURL            = "url"
	sriFieldIncludeDeleted = "include_deleted"
	sriFieldSubjectFilter  = "subject_filter"
	sriFieldTLS            = "tls"
)

//------------------------------------------------------------------------------

func inputSpec() *service.ConfigSpec {
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
			service.NewStringField(sriFieldURL).Description("The base URL of the schema registry service."),
			service.NewBoolField(sriFieldIncludeDeleted).Description("Include deleted entities.").Default(false).Advanced(),
			service.NewStringField(sriFieldSubjectFilter).Description("Include only subjects which match the regular expression filter. All subjects are selected when not set.").Default("").Advanced(),
			service.NewTLSToggledField(sriFieldTLS),
			service.NewAutoRetryNacksToggleField(),
		).Example("Read schemas", "Read all schemas (including deleted) from a Schema Registry instance which are associated with subjects matching the `^foo.*` filter.", `
input:
  schema_registry:
    url: http://localhost:8081
    include_deleted: true
    subject_filter: ^foo.*
`)
}

func init() {
	err := service.RegisterInput("schema_registry", inputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := inputFromParsed(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
		})
	if err != nil {
		panic(err)
	}
}

type input struct {
	url           *url.URL
	subjectFilter *regexp.Regexp

	client    http.Client
	connMut   sync.Mutex
	connected bool
	subjects  []string
	subject   string
	versions  []int
	log       *service.Logger
}

func inputFromParsed(pConf *service.ParsedConfig, log *service.Logger) (i *input, err error) {
	i = &input{
		log: log,
	}

	var u string
	if u, err = pConf.FieldString(sriFieldURL); err != nil {
		return
	}
	if i.url, err = url.Parse(u); err != nil {
		return nil, fmt.Errorf("failed to parse URL: %s", err)
	}

	var includeDeleted bool
	if includeDeleted, err = pConf.FieldBool(sriFieldIncludeDeleted); err != nil {
		return
	}
	if includeDeleted {
		q := i.url.Query()
		q.Add("deleted", "true")
		i.url.RawQuery = q.Encode()
	}

	var filter string
	if filter, err = pConf.FieldString(sriFieldSubjectFilter); err != nil {
		return
	}
	if i.subjectFilter, err = regexp.Compile(filter); err != nil {
		return nil, fmt.Errorf("failed to compile subject filter %q: %s", filter, err)
	}

	var tlsConf *tls.Config
	var tlsEnabled bool
	if tlsConf, tlsEnabled, err = pConf.FieldTLSToggled(sriFieldTLS); err != nil {
		return
	}

	i.client = http.Client{}
	if tlsEnabled && tlsConf != nil {
		if c, ok := http.DefaultTransport.(*http.Transport); ok {
			cloned := c.Clone()
			cloned.TLSClientConfig = tlsConf
			i.client.Transport = cloned
		} else {
			i.client.Transport = &http.Transport{
				TLSClientConfig: tlsConf,
			}
		}
	}

	return
}

//------------------------------------------------------------------------------

func (i *input) Connect(ctx context.Context) error {
	i.connMut.Lock()
	defer i.connMut.Unlock()

	data, err := doSchemaRegistryRequest(ctx, i.client, i.url.JoinPath("subjects").String())
	if err != nil {
		return fmt.Errorf("failed to fetch subjects: %s", err)
	}

	var subjects []string
	if err := json.Unmarshal(data, &subjects); err != nil {
		return fmt.Errorf("failed to unmarshal HTTP response: %s", err)
	}

	i.subjects = make([]string, 0, len(subjects))
	for _, s := range subjects {
		if i.subjectFilter.MatchString(s) {
			i.subjects = append(i.subjects, s)
		}
	}

	i.connected = true

	return nil
}

func (i *input) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	i.connMut.Lock()
	defer i.connMut.Unlock()
	if !i.connected {
		return nil, nil, service.ErrNotConnected
	}

	for {
		if len(i.subjects) == 0 && len(i.versions) == 0 {
			return nil, nil, service.ErrEndOfInput
		}

		if len(i.versions) != 0 {
			break
		}

		i.subject = i.subjects[0]

		data, err := doSchemaRegistryRequest(ctx, i.client, i.url.JoinPath("subjects", i.subject, "versions").String())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch versions for subject %q: %s", i.subject, err)
		}

		if err := json.Unmarshal(data, &i.versions); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal HTTP response: %s", err)
		}

		i.subjects = i.subjects[1:]

		if len(i.versions) == 0 {
			i.log.Infof("Subject %q does not contain any versions", i.subject)
			continue
		}

		break
	}

	version := i.versions[0]
	defer func() {
		i.versions = i.versions[1:]
	}()

	data, err := doSchemaRegistryRequest(ctx, i.client, i.url.JoinPath("subjects", i.subject, "versions", strconv.Itoa(version)).String())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch version %d for subject %q: %s", version, i.subject, err)
	}

	var structured map[string]any
	if err := json.Unmarshal(data, &structured); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal response body: %s", err)
	}

	// Remove the subject key since the `/subjects/<subject>/versions` endpoint doesn't allow it as part of the
	// payload and we pass it along as metadata.
	delete(structured, "subject")

	msg := service.NewMessage(nil)
	msg.SetStructured(structured)

	msg.MetaSetMut("schema_registry_subject", i.subject)
	msg.MetaSetMut("schema_registry_version", version)

	return msg, func(ctx context.Context, err error) error {
		// Nacks are handled by AutoRetryNacks because we don't have an explicit
		// ack mechanism right now.
		return nil
	}, nil
}

func doSchemaRegistryRequest(ctx context.Context, client http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to construct request: %s", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request returned status: %d", resp.StatusCode)
	}

	var body []byte
	if body, err = io.ReadAll(resp.Body); err != nil {
		return nil, fmt.Errorf("failed to read response body: %s", err)
	}

	return body, nil
}

func (i *input) Close(ctx context.Context) error {
	i.connMut.Lock()
	defer i.connMut.Unlock()

	i.connected = false

	return nil
}
