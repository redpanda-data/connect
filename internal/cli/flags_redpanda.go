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
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/urfave/cli/v2"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
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
	connDetails = &kafka.FranzConnectionDetails{
		ClientID:   "rpcn",
		MetaMaxAge: time.Minute * 5,
	}

	pipelineID = c.String(rfPipelineID)
	logsTopic = c.String(rfLogsTopic)
	statusTopic = c.String(rfStatusTopic)

	connDetails.SeedBrokers = c.StringSlice(rfBrokers)

	if connDetails.TLSEnabled = c.Bool(rfTLSEnabled); connDetails.TLSEnabled {
		connDetails.TLSConf = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if rootCasFile := c.String(rfTLSRootCasFile); rootCasFile != "" {
			var caCert []byte
			if caCert, err = os.ReadFile(rootCasFile); err != nil {
				return
			}

			connDetails.TLSConf.RootCAs = x509.NewCertPool()
			connDetails.TLSConf.RootCAs.AppendCertsFromPEM(caCert)
		}

		if c.Bool(rfTLSSkipCertVerify) {
			connDetails.TLSConf.InsecureSkipVerify = true
		}
	}

	if mech := c.String(rfSASLMechanism); mech != "" {
		user, pass := c.String(rfSASLUsername), c.String(rfSASLPassword)
		switch strings.ToLower(mech) {
		case "scram-sha-256":
			connDetails.SASL = append(connDetails.SASL, scram.Sha256(func(c context.Context) (scram.Auth, error) {
				return scram.Auth{
					User: user,
					Pass: pass,
				}, nil
			}))
		case "scram-sha-512":
			connDetails.SASL = append(connDetails.SASL, scram.Sha512(func(c context.Context) (scram.Auth, error) {
				return scram.Auth{
					User: user,
					Pass: pass,
				}, nil
			}))
		default:
			err = fmt.Errorf("unsupported sasl mechanism: %v", mech)
			return
		}
	}

	return
}
