// Copyright 2026 Redpanda Data, Inc.
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

package schemaregistry

import (
	"fmt"
	"net/http"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldURL     = "url"
	fieldTimeout = "timeout"
	fieldTLS     = "tls"
)

// ConfigFields returns the standard Schema Registry configuration fields.
// These fields can be embedded in any component that needs Schema Registry integration.
func ConfigFields() []*service.ConfigField {
	fields := []*service.ConfigField{
		service.NewStringField(fieldURL).
			Description("Schema Registry URL for schema operations.").
			Example("http://localhost:8081"),
		service.NewDurationField(fieldTimeout).
			Description("HTTP client timeout for Schema Registry requests.").
			Default("5s").
			Advanced(),
		service.NewTLSToggledField(fieldTLS),
	}
	fields = append(fields, service.NewHTTPRequestAuthSignerFields()...)
	return fields
}

// ClientFromParsed creates a franz-go Schema Registry client from a parsed
// config. Returns nil if the Schema Registry configuration is not present in
// the config.
func ClientFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*sr.Client, error) {
	srURL, err := pConf.FieldURL(fieldURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}

	timeout, err := pConf.FieldDuration(fieldTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timeout: %w", err)
	}

	reqSigner, err := pConf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, fmt.Errorf("failed to parse auth: %w", err)
	}

	tlsConf, tlsEnabled, err := pConf.FieldTLSToggled(fieldTLS)
	if err != nil {
		return nil, fmt.Errorf("failed to parse tls: %w", err)
	}
	if !tlsEnabled {
		tlsConf = nil
	}

	opts := []sr.ClientOpt{
		sr.HTTPClient(&http.Client{Timeout: timeout}),
		sr.UserAgent("redpanda-connect"),
		sr.URLs(srURL.String()),
	}

	if tlsConf != nil {
		opts = append(opts, sr.DialTLSConfig(tlsConf))
	}
	if reqSigner != nil {
		opts = append(opts, sr.PreReq(func(req *http.Request) error { return reqSigner(mgr.FS(), req) }))
	}

	client, err := sr.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Schema Registry client: %w", err)
	}

	return client, nil
}

// ClientFromParsedOptional creates a Schema Registry client from a parsed
// config, returning nil if the specified field name is not present in the
// config. This is useful when Schema Registry is an optional feature.
func ClientFromParsedOptional(pConf *service.ParsedConfig, fieldName string, mgr *service.Resources) (*sr.Client, error) {
	if !pConf.Contains(fieldName) {
		return nil, nil // SR not configured
	}

	srConf := pConf.Namespace(fieldName)
	return ClientFromParsed(srConf, mgr)
}
