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

package telemetry

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"time"

	"github.com/go-jose/go-jose/v4"
	josejwt "github.com/go-jose/go-jose/v4/jwt"
	"github.com/go-resty/resty/v2"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "embed"
)

// This embed captures our private JWT authentication key. Changes to this file
// will not be indexed by git as we have run:
//
// `git update-index --skip-worktree key.pem`
//
//go:embed key.pem
var privateKey string

var (
	// ExportHost customises the host to deliver telemetry exports to.
	ExportHost string

	// ExportDelay customises the time period a Connect instance must be running
	// before we begin exporting telemetry data.
	ExportDelay string

	// ExportPeriod customises the period with which telemetry data is exported
	// after the ExportDelay.
	ExportPeriod string
)

const (
	defaultExportHost   = "https://m.rp.vectorized.io"
	defaultExportDelay  = time.Minute * 5
	defaultExportPeriod = time.Hour * 24
)

// ParseRSAPrivateKeyFromPEM parses a PEM encoded PKCS1 or PKCS8 private key
func ParseRSAPrivateKeyFromPEM(key []byte) (*rsa.PrivateKey, error) {
	var err error

	// Parse PEM block
	var block *pem.Block
	if block, _ = pem.Decode(key); block == nil {
		return nil, errors.New("cert must be pem encoded")
	}

	var parsedKey any
	if parsedKey, err = x509.ParsePKCS1PrivateKey(block.Bytes); err != nil {
		if parsedKey, err = x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
			return nil, err
		}
	}

	var pkey *rsa.PrivateKey
	var ok bool
	if pkey, ok = parsedKey.(*rsa.PrivateKey); !ok {
		return nil, errors.New("not a RSA private key")
	}

	return pkey, nil
}

// ActivateExporter runs the telemetry exporter asynchronously, provided all
// conditions for telemetry are satisfied.
func ActivateExporter(identifier, version string, logger *service.Logger, schema *service.ConfigSchema, conf *service.ParsedConfig) {
	// If TLS information isn't present in the build then we do not send
	// telemetry data.
	if privateKey == "" {
		return
	}

	// Parse private key for signing the JWT payload before sending it to our telemetry endpoint.
	rsaPrivateKey, err := ParseRSAPrivateKeyFromPEM([]byte(privateKey))
	if err != nil {
		logger.With("error", err).Debug("Failed to parse private key")
		return
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: rsaPrivateKey},
		(&jose.SignerOptions{}).WithHeader("key_generation", 1))
	if err != nil {
		logger.With("error", err).Debug("Failed to create JWT signer")
		return
	}

	// Parse export delay and periods.
	exportDelay, exportPeriod := defaultExportDelay, defaultExportPeriod
	if ExportDelay != "" {
		if exportDelay, err = time.ParseDuration(ExportDelay); err != nil {
			logger.With("error", err).Debug("Failed to parse export delay")
			return
		}
	}
	if ExportPeriod != "" {
		if exportPeriod, err = time.ParseDuration(ExportPeriod); err != nil {
			logger.With("error", err).Debug("Failed to parse export period")
			return
		}
	}

	exportHost := defaultExportHost
	if ExportHost != "" {
		exportHost = ExportHost
	}

	tExporter := &telemetryExporter{
		logger: logger,
		Resty: resty.New().
			SetHeader("User-Agent", "RedpandaConnect/"+version).
			SetHeader("Accept-Encoding", "gzip").
			SetHeader("Content-Type", "text/plain").
			SetHeader("Accept", "application/json").
			SetBaseURL(exportHost).
			SetTimeout(10 * time.Second).
			SetLogger(&logWrapper{l: logger}).
			SetRetryCount(3),
		JWTBuilder: josejwt.Signed(signer),
	}

	payload, err := extractPayload(identifier, logger, schema, conf)
	if err != nil {
		logger.With("error", err).Debug("Failed to create telemetry payload")
		return
	}

	go exporterLoop(payload, exportDelay, exportPeriod, tExporter)
}

type telemetryExporter struct {
	logger *service.Logger

	Resty      *resty.Client
	JWTBuilder josejwt.Builder
}

// Send telemetry payload to a hardcoded HTTP endpoint.
func (t *telemetryExporter) export(p *payload) {
	tokenStr, err := t.JWTBuilder.Claims(p).Serialize()
	if err != nil {
		t.logger.With("error", err).Debug("Failed to get token string")
		return
	}

	response, err := t.Resty.NewRequest().
		SetBody(tokenStr).
		Post("/connect/telemetry")
	if err != nil {
		t.logger.With("error", err).Debug("Failed to send request")
		return
	}
	if response.IsError() {
		t.logger.With("status_code", response.StatusCode()).Debug("Failed to send request")
	}
}
