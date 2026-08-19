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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// failingFactory is a secretsClientFactory that always errors — for
// asserting resolveLicensePath never calls it when --license-file already
// resolved.
func failingFactory(t *testing.T) secretsClientFactory {
	return func(context.Context, string) (SecretsManagerClient, error) {
		t.Fatal("secrets client factory called despite --license-file resolving")
		return nil, nil
	}
}

func TestResolveLicensePath_FileWinsWhenItOpens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "license.jwt")
	require.NoError(t, os.WriteFile(path, []byte("from-file"), 0o600))

	opts := benchOpts{licenseFile: path, licenseSecret: "arn:aws:secretsmanager:us-east-2:123:secret:whatever"}
	got, cleanup, err := resolveLicensePath(context.Background(), opts, failingFactory(t))
	require.NoError(t, err)
	defer cleanup()

	require.Equal(t, path, got)
	// The file source is the operator's own file: cleanup must not delete it.
	_, statErr := os.Stat(path)
	require.NoError(t, statErr)
}

func TestResolveLicensePath_FallsBackToSecretWhenFileMissing(t *testing.T) {
	fake := &FakeSecretsManagerClient{SecretString: "license-from-secret"}
	opts := benchOpts{
		licenseFile:   filepath.Join(t.TempDir(), "does-not-exist.jwt"),
		licenseSecret: "my-secret",
	}
	got, cleanup, err := resolveLicensePath(context.Background(), opts, func(context.Context, string) (SecretsManagerClient, error) {
		return fake, nil
	})
	require.NoError(t, err)
	defer cleanup()

	raw, readErr := os.ReadFile(got)
	require.NoError(t, readErr)
	require.Equal(t, "license-from-secret", string(raw))
	require.Equal(t, []string{"my-secret"}, fake.Requests)

	info, statErr := os.Stat(got)
	require.NoError(t, statErr)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestResolveLicensePath_FallsBackToSecretWhenFileUnset(t *testing.T) {
	fake := &FakeSecretsManagerClient{SecretString: "license-from-secret"}
	opts := benchOpts{licenseSecret: "my-secret"}
	got, cleanup, err := resolveLicensePath(context.Background(), opts, func(context.Context, string) (SecretsManagerClient, error) {
		return fake, nil
	})
	require.NoError(t, err)
	defer cleanup()
	require.FileExists(t, got)
}

func TestResolveLicensePath_CleanupRemovesSecretTempFileOnly(t *testing.T) {
	fake := &FakeSecretsManagerClient{SecretString: "license-from-secret"}
	opts := benchOpts{licenseSecret: "my-secret"}
	got, cleanup, err := resolveLicensePath(context.Background(), opts, func(context.Context, string) (SecretsManagerClient, error) {
		return fake, nil
	})
	require.NoError(t, err)
	require.FileExists(t, got)

	cleanup()
	_, statErr := os.Stat(got)
	require.True(t, os.IsNotExist(statErr))
}

func TestResolveLicensePath_ErrorNamesBothOptionsWhenNeitherWorks(t *testing.T) {
	opts := benchOpts{licenseFile: "", licenseSecret: ""}
	_, _, err := resolveLicensePath(context.Background(), opts, failingFactory(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "--license-file")
	require.Contains(t, err.Error(), "--license-secret")
}

func TestResolveLicensePath_ErrorNamesBothOptionsWhenFileFailsAndSecretUnset(t *testing.T) {
	opts := benchOpts{licenseFile: filepath.Join(t.TempDir(), "missing.jwt"), licenseSecret: ""}
	_, _, err := resolveLicensePath(context.Background(), opts, failingFactory(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "--license-file")
	require.Contains(t, err.Error(), "--license-secret")
}

func TestResolveLicensePath_ErrorWhenSecretFetchFails(t *testing.T) {
	fake := &FakeSecretsManagerClient{Err: errors.New("access denied")}
	opts := benchOpts{licenseSecret: "my-secret"}
	_, _, err := resolveLicensePath(context.Background(), opts, func(context.Context, string) (SecretsManagerClient, error) {
		return fake, nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--license-file")
	require.Contains(t, err.Error(), "my-secret")
	require.Contains(t, err.Error(), "access denied")
}

func TestResolveLicensePath_ErrorWhenSecretHasNoSecretString(t *testing.T) {
	fake := &FakeSecretsManagerClient{NoSecretString: true}
	opts := benchOpts{licenseSecret: "my-secret"}
	_, _, err := resolveLicensePath(context.Background(), opts, func(context.Context, string) (SecretsManagerClient, error) {
		return fake, nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "binary secrets are not supported")
}
