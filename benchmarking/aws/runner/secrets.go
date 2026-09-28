// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"fmt"
	"os"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// SecretsManagerClient is the narrow slice of AWS Secrets Manager that
// license resolution uses. Tests fake this; production wires an
// awsSecretsManager that calls the real SDK.
type SecretsManagerClient interface {
	GetSecretValue(ctx context.Context, in *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error)
}

// secretsClientFactory builds a SecretsManagerClient for a region. A
// parameter (not a hardcoded call) so tests can inject a fake without
// touching AWS — see resolveLicensePath.
type secretsClientFactory func(ctx context.Context, region string) (SecretsManagerClient, error)

type awsSecretsManager struct {
	client *secretsmanager.Client
}

// NewSecretsManagerClient builds a SecretsManagerClient backed by the AWS
// SDK in the given region.
func NewSecretsManagerClient(ctx context.Context, region string) (SecretsManagerClient, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &awsSecretsManager{client: secretsmanager.NewFromConfig(cfg)}, nil
}

func (a *awsSecretsManager) GetSecretValue(ctx context.Context, in *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error) {
	return a.client.GetSecretValue(ctx, in)
}

// FakeSecretsManagerClient returns a canned GetSecretValueOutput or error —
// for tests.
type FakeSecretsManagerClient struct {
	SecretString string
	// NoSecretString, when true, returns an output with a nil SecretString
	// (simulating a binary secret) instead of SecretString.
	NoSecretString bool
	Err            error
	// Requests records every SecretId this fake was asked for, in call order.
	Requests []string
}

func (f *FakeSecretsManagerClient) GetSecretValue(_ context.Context, in *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error) {
	if in.SecretId != nil {
		f.Requests = append(f.Requests, *in.SecretId)
	}
	if f.Err != nil {
		return nil, f.Err
	}
	if f.NoSecretString {
		return &secretsmanager.GetSecretValueOutput{}, nil
	}
	s := f.SecretString
	return &secretsmanager.GetSecretValueOutput{SecretString: &s}, nil
}

// resolveLicensePath decides which license file the rest of runBench (in
// particular stageArtefacts) uploads. --license-file (or
// $REDPANDA_LICENSE_FILEPATH) wins whenever it's set and actually opens —
// this is the existing local-operator workflow, unchanged. Scheduled runs
// (GitHub Actions) have no license file on disk, so --license-secret (or
// $REDPANDA_LICENSE_SECRET) is the fallback: the named AWS Secrets Manager
// secret's raw SecretString is fetched and staged to a 0600 temp file, whose
// path is returned as if it had been --license-file all along.
//
// The returned cleanup removes that temp file; it is a no-op when the
// file-path source was used, since that file belongs to the operator, not
// us. Callers must defer cleanup after the license has been staged
// (stageArtefacts has already uploaded it to S3 by then).
func resolveLicensePath(ctx context.Context, opts benchOpts, newClient secretsClientFactory) (path string, cleanup func(), err error) {
	noop := func() {}

	fileStatus := "is unset"
	if opts.licenseFile != "" {
		f, openErr := os.Open(opts.licenseFile)
		if openErr == nil {
			f.Close()
			return opts.licenseFile, noop, nil
		}
		fileStatus = fmt.Sprintf("did not open (%v)", openErr)
	}

	if opts.licenseSecret == "" {
		return "", noop, fmt.Errorf(
			"no usable Redpanda Enterprise license: --license-file %s (also checked $REDPANDA_LICENSE_FILEPATH), and --license-secret is unset (also checked $REDPANDA_LICENSE_SECRET); enterprise connectors won't start without one",
			fileStatus)
	}

	sm, err := newClient(ctx, opts.region)
	if err != nil {
		return "", noop, fmt.Errorf("build secrets manager client for --license-secret %q: %w", opts.licenseSecret, err)
	}
	path, cleanup, err = fetchLicenseSecret(ctx, sm, opts.licenseSecret)
	if err != nil {
		return "", noop, fmt.Errorf(
			"no usable Redpanda Enterprise license: --license-file %s, and --license-secret %q failed: %w",
			fileStatus, opts.licenseSecret, err)
	}
	return path, cleanup, nil
}

// fetchLicenseSecret retrieves secretID's raw SecretString from AWS Secrets
// Manager and stages it to a 0600 file under os.TempDir(). The secret value
// itself is never logged — only the temp path, which callers may print
// freely.
func fetchLicenseSecret(ctx context.Context, sm SecretsManagerClient, secretID string) (path string, cleanup func(), err error) {
	noop := func() {}

	out, err := sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &secretID})
	if err != nil {
		return "", noop, fmt.Errorf("get secret value: %w", err)
	}
	if out.SecretString == nil {
		return "", noop, fmt.Errorf("secret %q has no SecretString (binary secrets are not supported)", secretID)
	}

	f, err := os.CreateTemp("", "bench-license-*.jwt")
	if err != nil {
		return "", noop, fmt.Errorf("create temp license file: %w", err)
	}
	tmpPath := f.Name()
	cleanup = func() { _ = os.Remove(tmpPath) }

	if err := f.Chmod(0o600); err != nil {
		f.Close()
		cleanup()
		return "", noop, fmt.Errorf("chmod temp license file: %w", err)
	}
	if _, err := f.WriteString(*out.SecretString); err != nil {
		f.Close()
		cleanup()
		return "", noop, fmt.Errorf("write temp license file: %w", err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", noop, fmt.Errorf("close temp license file: %w", err)
	}
	fmt.Printf("resolved license from secret %q\n", secretID)
	return tmpPath, cleanup, nil
}
