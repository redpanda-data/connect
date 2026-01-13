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
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"

	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/securetls"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/secrets"
	"github.com/redpanda-data/connect/v4/internal/serviceaccount"
)

const (
	rfPipelineID             = "x-redpanda-pipeline-id"
	rfLogsTopic              = "x-redpanda-logs-topic"
	rfStatusTopic            = "x-redpanda-status-topic"
	rfBrokers                = "x-redpanda-brokers"
	rfTLSEnabled             = "x-redpanda-tls-enabled"
	rfTLSSkipCertVerify      = "x-redpanda-tls-skip-verify"
	rfTLSRootCasFile         = "x-redpanda-root-cas-file"
	rfSASLMechanism          = "x-redpanda-sasl-mechanism"
	rfSASLUsername           = "x-redpanda-sasl-username"
	rfSASLPassword           = "x-redpanda-sasl-password"
	rfCloudTokenURL          = "x-redpanda-cloud-service-account-token-url"
	rfCloudClientID          = "x-redpanda-cloud-service-account-client-id"
	rfCloudClientSecret      = "x-redpanda-cloud-service-account-client-secret"
	rfCloudAudience          = "x-redpanda-cloud-service-account-audience"
	rfCloudAuthzResourceName = "x-redpanda-cloud-authz-resource-name"
	rfCloudAuthzPolicyFile   = "x-redpanda-cloud-authz-policy-file"
)

var secretsFlag = &cli.StringSliceFlag{
	Name:  "secrets",
	Usage: "Attempt to load secrets from a provided URN. If more than one entry is specified they will be attempted in order until a value is found. Environment variable lookups are specified with the URN `env:`, which by default is the only entry. In order to disable all secret lookups specify a single entry of `none:`.",
	Value: cli.NewStringSlice("env:"),
}

func parseSecretsFlag(logger *slog.Logger, c *cli.Context) (func(context.Context, string) (string, bool), error) {
	if secretsURNs := c.StringSlice("secrets"); len(secretsURNs) > 0 {
		return secrets.ParseLookupURNs(c.Context, logger, secretsURNs...)
	}
	return func(_ context.Context, _ string) (string, bool) {
		return "", false
	}, nil
}

var licenseFlag = &cli.StringFlag{
	Name:  "redpanda-license",
	Usage: "Provide an explicit Redpanda License, which enables enterprise functionality. By default licenses found at the path `/etc/redpanda/redpanda.license` are applied.",
}

func defaultLicenseConfig() license.Config {
	return license.Config{
		License:         os.Getenv("REDPANDA_LICENSE"),
		LicenseFilepath: os.Getenv("REDPANDA_LICENSE_FILEPATH"),
	}
}

func applyLicenseFlag(c *cli.Context, conf *license.Config) {
	if inline := c.String("redpanda-license"); inline != "" {
		conf.License = inline
	}
}

var chrootFlag = &cli.StringFlag{
	Name: "chroot",
	Usage: "Chroot into the provided directory after parsing configuration. " +
		"The directory must not exist and will be created. " +
		"Common /etc/ files are copied to the chroot directory, and the directory is made read-only. " +
		"This flag is only supported on Linux.",
}

var chrootPassthroughFlag = &cli.StringSliceFlag{
	Name: "chroot-passthrough",
	Usage: "Specify additional files to be copied into the chroot directory. " +
		"This flag can be used multiple times. " +
		"It is only supported when --chroot is used.",
}

func shouldAddChrootFlag() bool {
	return runtime.GOOS == "linux"
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
		&cli.StringFlag{
			Name:   rfCloudTokenURL,
			Usage:  "OAuth2 token URL for service-account authentication",
			Hidden: true,
			Value:  "",
		},
		&cli.StringFlag{
			Name:   rfCloudClientID,
			Usage:  "OAuth2 client ID for service-account authentication",
			Hidden: true,
			Value:  "",
		},
		&cli.StringFlag{
			Name:   rfCloudClientSecret,
			Usage:  "OAuth2 client secret for service-account authentication",
			Hidden: true,
			Value:  "",
		},
		&cli.StringFlag{
			Name:   rfCloudAudience,
			Usage:  "OAuth2 audience parameter for service-account authentication",
			Hidden: true,
			Value:  "",
		},
		&cli.StringFlag{
			Name:   rfCloudAuthzResourceName,
			Usage:  "Authorization resource name for scope lookup in the policy file",
			Hidden: true,
			Value:  "",
		},
		&cli.PathFlag{
			Name:   rfCloudAuthzPolicyFile,
			Usage:  "Authorization policy file for enforcing permissions",
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
		// Use strict security level for Redpanda-to-Redpanda communication
		connDetails.TLSConf = securetls.NewConfig(securetls.SecurityLevelStrict)

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
			connDetails.SASL = append(connDetails.SASL, scram.Sha256(func(_ context.Context) (scram.Auth, error) {
				return scram.Auth{
					User: saslUser,
					Pass: saslPass,
				}, nil
			}))
		case "scram-sha-512":
			connDetails.SASL = append(connDetails.SASL, scram.Sha512(func(_ context.Context) (scram.Auth, error) {
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

// resolveSecret resolves a value that may contain a ${secrets.KEY} reference
// using the provided secret lookup function.
func resolveSecret(ctx context.Context, value string, lookupFn secrets.LookupFn) string {
	if value == "" {
		return value
	}

	// Check if value is a secret reference: ${...}
	if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
		key := strings.TrimSuffix(strings.TrimPrefix(value, "${"), "}")
		if resolved, ok := lookupFn(ctx, key); ok {
			return resolved
		}
	}

	return value
}

// parseCloudAuthFlags parses the OAuth2/cloud authentication CLI flags,
// resolves any secret references, and initializes the global service account configuration.
// returns the authz policy file (if specified)
func parseCloudAuthFlags(ctx context.Context, c *cli.Context, secretLookupFn secrets.LookupFn) (authzResourceName, authzPolicyFile string, err error) {
	tokenURL := resolveSecret(ctx, c.String(rfCloudTokenURL), secretLookupFn)
	clientID := resolveSecret(ctx, c.String(rfCloudClientID), secretLookupFn)
	clientSecret := resolveSecret(ctx, c.String(rfCloudClientSecret), secretLookupFn)
	audience := resolveSecret(ctx, c.String(rfCloudAudience), secretLookupFn)
	authzResourceName = resolveSecret(ctx, c.String(rfCloudAuthzResourceName), secretLookupFn)
	authzPolicyFile = resolveSecret(ctx, c.Path(rfCloudAuthzPolicyFile), secretLookupFn)

	// Initialize global service account config if credentials are provided
	if tokenURL != "" && clientID != "" && clientSecret != "" {
		if err := serviceaccount.InitGlobal(ctx, tokenURL, clientID, clientSecret, audience); err != nil {
			return "", "", fmt.Errorf("failed to initialize service account authentication: %w", err)
		}
	}

	return authzResourceName, authzPolicyFile, nil
}
