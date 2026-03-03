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
	"context"
	"fmt"
	"net/http"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/oauth2"
	"github.com/redpanda-data/connect/v4/internal/serviceaccount"
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
	fields = append(fields, oauth2.FieldSpec())
	fields = append(fields, service.NewHTTPRequestAuthSignerFields()...)
	return fields
}

// ClientFromParsed creates a franz-go Schema Registry client from a parsed
// config. The returned cancel function must be called when the client is no
// longer needed to clean up OAuth2 resources.
func ClientFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*sr.Client, context.CancelFunc, error) {
	srURL, err := pConf.FieldURL(fieldURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing url: %w", err)
	}

	timeout, err := pConf.FieldDuration(fieldTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing timeout: %w", err)
	}

	reqSigner, err := pConf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, nil, fmt.Errorf("parsing auth: %w", err)
	}

	tlsConf, tlsEnabled, err := pConf.FieldTLSToggled(fieldTLS)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing tls: %w", err)
	}
	if !tlsEnabled {
		tlsConf = nil
	}

	opts := []sr.ClientOpt{
		sr.UserAgent("redpanda-connect"),
		sr.URLs(srURL.String()),
	}

	var oa2Conf oauth2.Config
	if pConf.Contains("oauth2") {
		if oa2Conf, err = oauth2.ParseConfig(pConf.Namespace("oauth2")); err != nil {
			return nil, nil, fmt.Errorf("parsing oauth2: %w", err)
		}
	}

	// OAuth2 provides its own HTTP client with token auth. If no explicit
	// OAuth2 is configured, fall back to the global service account when
	// running in Redpanda Cloud. Otherwise use a plain HTTP client.
	var (
		httpClient = &http.Client{Timeout: timeout}
		cancel     context.CancelFunc
	)
	if oa2Conf.Enabled {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())

		c, err := oa2Conf.HTTPClient(ctx, httpClient)
		if err != nil {
			cancel()
			return nil, nil, fmt.Errorf("creating oauth2 http client: %w", err)
		}
		httpClient = c
	} else if reqSigner == nil {
		if c, err := serviceaccount.GetHTTPClient(); err == nil {
			mgr.Logger().Info("Using Redpanda Cloud service account for Schema Registry authentication")
			httpClient = c
		}
	}
	opts = append(opts, sr.HTTPClient(httpClient))

	if tlsConf != nil {
		opts = append(opts, sr.DialTLSConfig(tlsConf))
	}
	if reqSigner != nil {
		opts = append(opts, sr.PreReq(func(req *http.Request) error { return reqSigner(mgr.FS(), req) }))
	}

	client, err := sr.NewClient(opts...)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, nil, fmt.Errorf("creating Schema Registry client: %w", err)
	}

	return client, cancel, nil
}

// ClientFromParsedOptional creates a Schema Registry client from a parsed
// config, returning nil if the specified field name is not present in the
// config. This is useful when Schema Registry is an optional feature. The
// returned cancel function must be called when the client is no longer needed
// to clean up OAuth2 resources.
func ClientFromParsedOptional(pConf *service.ParsedConfig, fieldName string, mgr *service.Resources) (*sr.Client, context.CancelFunc, error) {
	if !pConf.Contains(fieldName) {
		return nil, nil, nil // SR not configured
	}

	srConf := pConf.Namespace(fieldName)
	return ClientFromParsed(srConf, mgr)
}
