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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/secrets"
)

const (
	rfPipelineID        = "x-redpanda-pipeline-id"
	rfLogsTopic         = "x-redpanda-logs-topic"
	rfStatusTopic       = "x-redpanda-status-topic"
	rfBrokers           = "x-redpanda-brokers"
	rfTLSEnabled        = "x-redpanda-tls-enabled"
	rfTLSSkipCertVerify = "x-redpanda-tls-skip-verify"
	rfTLSRootCasFile    = "x-redpanda-root-cas-file"
	rfSASLMechanism     = "x-redpanda-sasl-mechanism"
	rfSASLUsername      = "x-redpanda-sasl-username"
	rfSASLPassword      = "x-redpanda-sasl-password"
)

var secretsFlag = &cli.StringSliceFlag{
	Name:  "secrets",
	Usage: "Attempt to load secrets from a provided URN. If more than one entry is specified they will be attempted in order until a value is found. Environment variable lookups are specified with the URN `env:`, which by default is the only entry. In order to disable all secret lookups specify a single entry of `none:`.",
	Value: cli.NewStringSlice("env:"),
}

var licenseFlag = &cli.StringFlag{
	Name:  "redpanda-license",
	Usage: "Provide an explicit Redpanda License, which enables enterprise functionality. By default licenses found at the path `/etc/redpanda/redpanda.license` are applied.",
}

func parseLicenseFlag(c *cli.Context) string {
	return c.String("redpanda-license")
}

func parseSecretsFlag(logger *slog.Logger, c *cli.Context) (func(context.Context, string) (string, bool), error) {
	if secretsURNs := c.StringSlice("secrets"); len(secretsURNs) > 0 {
		return secrets.ParseLookupURNs(c.Context, logger, secretsURNs...)
	}
	return func(ctx context.Context, key string) (string, bool) {
		return "", false
	}, nil
}

func redpandaFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:   rfPipelineID,
			Hidden: true,
			Value:  "",
		},
		&cli.StringFlag{
			Name:   rfLogsTopic,
			Hidden: true,
			Value:  "",
		},
		&cli.StringFlag{
			Name:   rfStatusTopic,
			Hidden: true,
			Value:  "",
		},
		&cli.StringSliceFlag{
			Name:   rfBrokers,
			Hidden: true,
			Value:  cli.NewStringSlice(),
		},
		&cli.BoolFlag{
			Name:   rfTLSEnabled,
			Hidden: true,
			Value:  false,
		},
		&cli.BoolFlag{
			Name:   rfTLSSkipCertVerify,
			Hidden: true,
			Value:  false,
		},
		&cli.StringFlag{
			Name:   rfTLSRootCasFile,
			Hidden: true,
			Value:  "",
		},
		&cli.StringFlag{
			Name:   rfSASLMechanism,
			Hidden: true,
			Value:  "",
		},
		&cli.StringFlag{
			Name:   rfSASLUsername,
			Hidden: true,
			Value:  "",
		},
		&cli.StringFlag{
			Name:   rfSASLPassword,
			Hidden: true,
			Value:  "",
		},
	}
}

func parseRedpandaFlags(c *cli.Context) (pipelineID, logsTopic, statusTopic string, connDetails *kafka.FranzConnectionDetails, err error) {
	pipelineID = c.String(rfPipelineID)
	logsTopic = c.String(rfLogsTopic)
	statusTopic = c.String(rfStatusTopic)

	connDetails, err = rpConnDetails(
		c.StringSlice(rfBrokers),
		c.Bool(rfTLSEnabled),
		c.String(rfTLSRootCasFile),
		c.Bool(rfTLSSkipCertVerify),
		c.String(rfSASLMechanism),
		c.String(rfSASLUsername),
		c.String(rfSASLPassword),
	)
	return
}

func rpConnDetails(
	brokers []string,
	tlsEnabled bool,
	rootCasFile string,
	tlsSkipVerify bool,
	saslMech, saslUser, saslPass string,
) (connDetails *kafka.FranzConnectionDetails, err error) {
	var pConf *service.ParsedConfig
	if pConf, err = service.NewConfigSpec().Fields(kafka.FranzConnectionFields()...).ParseYAML(`
seed_brokers: [ ]
client_id: rpcn
`, nil); err != nil {
		return
	}

	if connDetails, err = kafka.FranzConnectionDetailsFromConfig(pConf, nil); err != nil {
		return
	}

	connDetails.SeedBrokers = brokers

	if connDetails.TLSEnabled = tlsEnabled; connDetails.TLSEnabled {
		connDetails.TLSConf = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if rootCasFile != "" {
			var caCert []byte
			if caCert, err = os.ReadFile(rootCasFile); err != nil {
				return
			}

			connDetails.TLSConf.RootCAs = x509.NewCertPool()
			connDetails.TLSConf.RootCAs.AppendCertsFromPEM(caCert)
		}

		connDetails.TLSConf.InsecureSkipVerify = tlsSkipVerify
	}

	if saslMech != "" {
		switch strings.ToLower(saslMech) {
		case "scram-sha-256":
			connDetails.SASL = append(connDetails.SASL, scram.Sha256(func(c context.Context) (scram.Auth, error) {
				return scram.Auth{
					User: saslUser,
					Pass: saslPass,
				}, nil
			}))
		case "scram-sha-512":
			connDetails.SASL = append(connDetails.SASL, scram.Sha512(func(c context.Context) (scram.Auth, error) {
				return scram.Auth{
					User: saslUser,
					Pass: saslPass,
				}, nil
			}))
		default:
			err = fmt.Errorf("unsupported sasl mechanism: %v", saslMech)
			return
		}
	}

	return
}
