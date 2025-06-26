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

	"github.com/rs/xid"
	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/rpcplugin"
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

	secretLookupFn := func(context.Context, string) (string, bool) {
		return "", false
	}

	var (
		licenseConfig     = defaultLicenseConfig()
		chrootPath        string
		chrootPassthrough []string
		disableTelemetry  bool
	)

	flags := []cli.Flag{
		secretsFlag,
		licenseFlag,
	}
	if shouldAddChrootFlag() {
		flags = append(flags, chrootFlag, chrootPassthroughFlag)
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
			// Kick off license service, it's important we do this before chroot and telemetry
			license.RegisterService(pConf.Resources(), licenseConfig)

			// Chroot if needed
			if chrootPath != "" {
				fbLogger.Infof("Chrooting to '%v'", chrootPath)
				if err := chroot(chrootPath, chrootPassthrough); err != nil {
					return fmt.Errorf("chroot: %w", err)
				}
			}

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
				flags,
				[]cli.Flag{
					&cli.BoolFlag{
						Name:  "disable-telemetry",
						Usage: "Disable anonymous telemetry from being emitted by this Connect instance.",
					},
					&cli.StringSliceFlag{
						Name:  "rpc-plugins",
						Usage: "Plugins to load over the RPC interface. This flag should point to manifest files containing the plugin definitions. Globs are also supported.",
					},
				},
				redpandaFlags(),
			),

			func(c *cli.Context) error {
				applyLicenseFlag(c, &licenseConfig)
				chrootPath = c.String("chroot")
				chrootPassthrough = c.StringSlice("chroot-passthrough")
				disableTelemetry = c.Bool("disable-telemetry")

				if secretLookupFn, err = parseSecretsFlag(slog.New(rpLogger), c); err != nil {
					return err
				}

				rpcPlugins := c.StringSlice("rpc-plugins")
				err := rpcplugin.DiscoverAndRegisterPlugins(service.OSFS(), schema.Environment(), rpcPlugins)
				if err != nil {
					return err
				}

				// Hidden redpanda flags
				pipelineID, logsTopic, statusTopic, connDetails, err := parseRedpandaFlags(c)
				if err != nil {
					return err
				}

				// We need a fallback logger since the normal run cli isnt executed
				rpMgr.SetFallbackLogger(service.NewLoggerFromSlog(slog.Default()))

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

		// Custom subcommands
		service.CLIOptAddCommand(agentCli(rpMgr)),
		service.CLIOptAddCommand(mcpServerCli(rpMgr)),
		service.CLIOptAddCommand(pluginInit()),
	)

	exitCode, err := service.RunCLIToCode(context.Background(), opts...)
	if err != nil {
		slog.New(rpMgr.SlogHandler()).With("status", exitCode, "error", err).Error("Pipeline exited with non-zero status")
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
