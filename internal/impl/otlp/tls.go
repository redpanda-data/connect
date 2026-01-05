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

package otlp

import (
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	tlsFieldEnabled  = "enabled"
	tlsFieldCertFile = "cert_file"
	tlsFieldKeyFile  = "key_file"
)

func tlsFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewBoolField(tlsFieldEnabled).
			Description("Enable TLS connections.").
			Default(false),
		service.NewStringField(tlsFieldCertFile).
			Description("Path to the TLS certificate file.").
			Default(""),
		service.NewStringField(tlsFieldKeyFile).
			Description("Path to the TLS key file.").
			Default(""),
	}
}

type tlsConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
}

func parseTLSConfig(pConf *service.ParsedConfig) (tlsConf tlsConfig, err error) {
	if tlsConf.Enabled, err = pConf.FieldBool(tlsFieldEnabled); err != nil {
		return
	}
	if tlsConf.CertFile, err = pConf.FieldString(tlsFieldCertFile); err != nil {
		return
	}
	if tlsConf.KeyFile, err = pConf.FieldString(tlsFieldKeyFile); err != nil {
		return
	}
	if tlsConf.Enabled && (tlsConf.CertFile == "" || tlsConf.KeyFile == "") {
		err = errors.New("both cert_file and key_file must be provided when TLS is enabled")
		return
	}

	return tlsConf, nil
}
