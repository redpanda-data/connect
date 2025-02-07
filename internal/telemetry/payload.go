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
	"fmt"
	"runtime"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Information gathered from each component present in the running config.
type componentInfo struct {
	// The type (input, output, etc) of the plugin.
	Type string `json:"type"`

	// The name (aws_s3, generate, etc) of the plugin.
	Name string `json:"name"`
}

// Information gathered about the host that we're running on
type hostInfo struct {
	// Number of logical CPUs usable
	NumCPU int `json:"numCpu"`

	// Limit of concurrent goroutines by the scheduler
	GoMaxProcs int `json:"goMaxProcs"`

	// Architecture we're running on
	GoArch string `json:"goArch"`

	// OS we're running on
	GoOS string `json:"goOS"`
}

// Contains all of the information which is delivered during a telemetry
// export, serialisable in JSON format.
type payload struct {
	// A unique identifier for the Redpanda Connect instance.
	ID string `json:"id"`

	// Uptime of the Redpanda Connect instance.
	Uptime int64 `json:"uptime"`

	// A slice representing each component within a config.
	Components []componentInfo `json:"components"`

	// Information about the host and process
	HostInfo hostInfo `json:"hostInfo"`
}

// All information sent during a telemetry export is extracted within this
// function and stored within the payload.
func extractPayload(identifier string, logger *service.Logger, schema *service.ConfigSchema, conf *service.ParsedConfig) (*payload, error) {
	p := payload{ID: identifier, Uptime: 0, HostInfo: hostInfo{
		NumCPU:     runtime.NumCPU(),
		GoMaxProcs: runtime.GOMAXPROCS(0), // using 0 means to just read the value
		GoOS:       runtime.GOOS,
		GoArch:     runtime.GOARCH,
	}}

	rootValue, err := conf.FieldAny()
	if err != nil {
		return nil, fmt.Errorf("failed to obtain root of config: %w", err)
	}

	if err := schema.NewStreamConfigWalker().WalkComponentsAny(rootValue, func(w *service.WalkedComponent) error {
		p.Components = append(p.Components, componentInfo{
			Type: w.ComponentType,
			Name: w.Name,
		})
		return nil
	}); err != nil {
		logger.With("error", err).Debug("Failed to walk config")
	}

	return &p, nil
}

// This function runs asynchronously and is solely where telemetry data is
// exported.
func exporterLoop(p *payload, exportDelay, exportPeriod time.Duration, exporter *telemetryExporter) {
	started := time.Now()

	// First, wait until after the export delay has passed.
	time.Sleep(exportDelay)

	for {
		p.Uptime = int64(time.Since(started) / time.Second)
		exporter.export(p)

		// Now wait for the next export.
		time.Sleep(exportPeriod)
	}
}
