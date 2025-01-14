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
	"slices"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/rs/xid"
	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/secrets"
	"github.com/redpanda-data/connect/v4/internal/telemetry"
)

const connectorListPath = "/etc/redpanda/connector_list.yaml"

// InitEnterpriseCLI kicks off the benthos cli with a suite of options that adds
// all of the enterprise functionality of Redpanda Connect. This has been
// abstracted into a separate package so that multiple distributions (classic
// versus cloud) can reference the same code.
func InitEnterpriseCLI(binaryName, version, dateBuilt string, schema *service.ConfigSchema, opts ...service.CLIOptFunc) {
	instanceID := xid.New().String()

	rpMgr := enterprise.NewGlobalRedpandaManager(instanceID)

	rpLogger := rpMgr.SlogHandler()
	var fbLogger *service.Logger

	cListApplied, err := ApplyConnectorsList(connectorListPath, schema)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	secretLookupFn := func(ctx context.Context, key string) (string, bool) {
		return "", false
	}

	var disableTelemetry bool
	licenseConfig := license.Config{
		License:         os.Getenv("REDPANDA_LICENSE"),
		LicenseFilepath: os.Getenv("REDPANDA_LICENSE_FILEPATH"),
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
			rpMgr.SetFallbackLogger(l)
		}),
		service.CLIOptAddTeeLogger(slog.New(rpLogger)),
		service.CLIOptOnConfigParse(func(pConf *service.ParsedConfig) error {
			// Kick off license service, it's important we do this before telemetry.
			license.RegisterService(pConf.Resources(), licenseConfig)

			// Kick off telemetry exporter.
			if !disableTelemetry {
				telemetry.ActivateExporter(instanceID, version, fbLogger, schema, pConf)
			}
			return rpMgr.InitFromParsedConfig(pConf.Namespace("redpanda"))
		}),
		service.CLIOptOnStreamStart(func(s *service.RunningStreamSummary) error {
			rpMgr.SetStreamSummary(s)
			return nil
		}),

		// Secrets management and other custom CLI flags
		service.CLIOptCustomRunFlags(
			slices.Concat(
				// Secrets management flags
				[]cli.Flag{
					&cli.StringSliceFlag{
						Name:  "secrets",
						Usage: "Attempt to load secrets from a provided URN. If more than one entry is specified they will be attempted in order until a value is found. Environment variable lookups are specified with the URN `env:`, which by default is the only entry. In order to disable all secret lookups specify a single entry of `none:`.",
						Value: cli.NewStringSlice("env:"),
					},
					&cli.BoolFlag{
						Name:  "disable-telemetry",
						Usage: "Disable anonymous telemetry from being emitted by this Connect instance.",
					},
					&cli.StringFlag{
						Name:  "redpanda-license",
						Usage: "Provide an explicit Redpanda License, which enables enterprise functionality. By default licenses found at the path `/etc/redpanda/redpanda.license` are applied.",
					},
				},

				// Hidden redpanda flags
				redpandaFlags(),
			),

			func(c *cli.Context) error {
				disableTelemetry = c.Bool("disable-telemetry")
				license := c.String("redpanda-license")
				if license != "" {
					licenseConfig.License = license
				}

				if secretsURNs := c.StringSlice("secrets"); len(secretsURNs) > 0 {
					var err error
					if secretLookupFn, err = secrets.ParseLookupURNs(c.Context, slog.New(rpLogger), secretsURNs...); err != nil {
						return err
					}
				}

				// Hidden redpanda flags
				pipelineID, logsTopic, statusTopic, connDetails, err := parseRedpandaFlags(c)
				if err != nil {
					return err
				}

				if pipelineID != "" && connDetails != nil {
					if err = rpMgr.InitWithCustomDetails(pipelineID, logsTopic, statusTopic, connDetails); err != nil {
						return err
					}
				}
				return nil
			}),
		service.CLIOptSetEnvVarLookup(func(ctx context.Context, key string) (string, bool) {
			return secretLookupFn(ctx, key)
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
	rpMgr.TriggerEventStopped(err)

	_ = rpMgr.Close(context.Background())
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}
