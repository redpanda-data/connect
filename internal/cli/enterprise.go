// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/rs/xid"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
	"github.com/redpanda-data/connect/v4/internal/telemetry"
)

const connectorListPath = "/etc/redpanda/connector_list.yaml"

// InitEnterpriseCLI kicks off the benthos cli with a suite of options that adds
// all of the enterprise functionality of Redpanda Connect. This has been
// abstracted into a separate package so that multiple distributions (classic
// versus cloud) can reference the same code.
func InitEnterpriseCLI(binaryName, version, dateBuilt string, schema *service.ConfigSchema, opts ...service.CLIOptFunc) {
	instanceID := xid.New().String()

	rpLogger := enterprise.NewTopicLogger(instanceID)
	var fbLogger *service.Logger

	cListApplied, err := ApplyConnectorsList(connectorListPath, schema)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	opts = append(opts,
		service.CLIOptSetVersion(version, dateBuilt),
		service.CLIOptSetBinaryName(binaryName),
		service.CLIOptSetProductName("Redpanda Connect"),
		service.CLIOptSetDefaultConfigPaths(
			"redpanda-connect.yaml",
			"/redpanda-connect.yaml",
			"/etc/redpanda-connect/config.yaml",
			"/etc/redpanda-connect.yaml",

			"connect.yaml",
			"/connect.yaml",
			"/etc/connect/config.yaml",
			"/etc/connect.yaml",

			// Keep these for now, for backwards compatibility
			"/benthos.yaml",
			"/etc/benthos/config.yaml",
			"/etc/benthos.yaml",
		),
		service.CLIOptSetDocumentationURL("https://docs.redpanda.com/redpanda-connect"),
		service.CLIOptSetMainSchemaFrom(func() *service.ConfigSchema {
			return schema
		}),
		service.CLIOptSetEnvironment(schema.Environment()),
		service.CLIOptOnLoggerInit(func(l *service.Logger) {
			fbLogger = l
			if cListApplied {
				fbLogger.Infof("Successfully applied connectors allow/deny list from '%v'", connectorListPath)
			}
			rpLogger.SetFallbackLogger(l)
		}),
		service.CLIOptAddTeeLogger(slog.New(rpLogger)),
		service.CLIOptOnConfigParse(func(pConf *service.ParsedConfig) error {
			// Kick off telemetry exporter.
			telemetry.ActivateExporter(instanceID, version, fbLogger, schema, pConf)

			return rpLogger.InitOutputFromParsed(pConf.Namespace("redpanda"))
		}),
		service.CLIOptOnStreamStart(func(s *service.RunningStreamSummary) error {
			rpLogger.SetStreamSummary(s)
			return nil
		}),
	)

	exitCode, err := service.RunCLIToCode(context.Background(), opts...)
	if err != nil {
		if fbLogger != nil {
			fbLogger.Error(err.Error())
		} else {
			fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	rpLogger.TriggerEventStopped(err)

	_ = rpLogger.Close(context.Background())
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}
