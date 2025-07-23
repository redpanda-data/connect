// Copyright 2025 Redpanda Data, Inc.
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

package migrator

import (
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

const (
	srObjectField = "schema_registry"
	srFieldURL    = "url"
	srFieldTLS    = "tls"
)

func schemaRegistryField() *service.ConfigField {
	fields := append(
		[]*service.ConfigField{
			service.NewStringField(srFieldURL).Description("The base URL of the schema registry service."),
			service.NewTLSToggledField(srFieldTLS),
		},
		service.NewHTTPRequestAuthSignerFields()...)

	return service.NewObjectField(srObjectField, fields...)
}

func schemaRegistryClientFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*sr.Client, error) {
	if !pConf.Contains("schema_registry") {
		return nil, nil
	}

	srURL, err := pConf.FieldURL(srObjectField, srFieldURL)
	if err != nil {
		return nil, err
	}

	// TODO(mmt): this does not work with objects
	reqSigner, err := pConf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := pConf.FieldTLSToggled(srFieldTLS)
	if err != nil {
		return nil, err
	}
	if !tlsEnabled {
		tlsConf = nil
	}

	client, err := sr.NewClient(srURL.String(), reqSigner, tlsConf, mgr)
	if err != nil {
		return nil, err
	}

	return client, nil
}
