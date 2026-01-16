// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlp

import (
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	tlsFieldEnabled        = "enabled"
	tlsFieldSkipCertVerify = "skip_cert_verify"
	tlsFieldCertFile       = "cert_file"
	tlsFieldKeyFile        = "key_file"
)

// tlsClientConfigFields returns TLS configuration fields for client connections (outputs).
func tlsClientConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewBoolField(tlsFieldEnabled).
			Description("Enable TLS connections.").
			Default(false),
		service.NewBoolField(tlsFieldSkipCertVerify).
			Description("Skip certificate verification (insecure).").
			Default(false),
		service.NewStringField(tlsFieldCertFile).
			Description("Path to the TLS certificate file for client authentication.").
			Default(""),
		service.NewStringField(tlsFieldKeyFile).
			Description("Path to the TLS key file for client authentication.").
			Default(""),
	}
}

// tlsServerConfigFields returns TLS configuration fields for server connections (inputs).
func tlsServerConfigFields() []*service.ConfigField {
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

type tlsClientConfig struct {
	Enabled        bool
	SkipCertVerify bool
	CertFile       string
	KeyFile        string
}

type tlsServerConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
}

func parseTLSClientConfig(pConf *service.ParsedConfig) (tlsConf tlsClientConfig, err error) {
	if tlsConf.Enabled, err = pConf.FieldBool(tlsFieldEnabled); err != nil {
		return
	}
	if tlsConf.SkipCertVerify, err = pConf.FieldBool(tlsFieldSkipCertVerify); err != nil {
		return
	}
	if tlsConf.CertFile, err = pConf.FieldString(tlsFieldCertFile); err != nil {
		return
	}
	if tlsConf.KeyFile, err = pConf.FieldString(tlsFieldKeyFile); err != nil {
		return
	}
	if tlsConf.Enabled && !tlsConf.SkipCertVerify && (tlsConf.CertFile == "" || tlsConf.KeyFile == "") {
		err = errors.New("both cert_file and key_file must be provided when TLS is enabled and skip_cert_verify is false")
		return
	}

	return tlsConf, nil
}

func parseTLSServerConfig(pConf *service.ParsedConfig) (tlsConf tlsServerConfig, err error) {
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
