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
	"context"
	"time"

	commontelemetry "github.com/redpanda-data/common-go/telemetry"

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

// ActivateExporter runs the telemetry exporter asynchronously, provided all
// conditions for telemetry are satisfied.
func ActivateExporter(identifier, version, deploymentType, tenantID string, logger *service.Logger, schema *service.ConfigSchema, conf *service.ParsedConfig) {
	// If TLS information isn't present in the build then we do not send
	// telemetry data.
	if privateKey == "" {
		return
	}

	// Parse export delay and periods.
	var err error
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

	p, err := extractPayload(identifier, deploymentType, tenantID, logger, schema, conf)
	if err != nil {
		logger.With("error", err).Debug("Failed to create telemetry payload")
		return
	}

	client, err := commontelemetry.New(commontelemetry.Config{
		Endpoint:      exportHost,
		Path:          "/connect/telemetry",
		UserAgent:     "RedpandaConnect/" + version,
		SigningKeyPEM: []byte(privateKey),
		JWTHeaders:    map[string]any{"key_generation": 1},
	})
	if err != nil {
		logger.With("error", err).Debug("failed to build telemetry client")
		return
	}

	started := time.Now()
	reporter := &commontelemetry.Reporter{
		Client: client,
		Delay:  exportDelay,
		Period: exportPeriod,
		Logger: benthosLogger{l: logger},
		Collector: func(_ context.Context) (any, error) {
			p.Uptime = int64(time.Since(started) / time.Second)
			return p, nil
		},
	}

	go func() { _ = reporter.Run(context.Background()) }()
}
