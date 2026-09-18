// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/youmark/pkcs8"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// ---------------------------------------------------------------------------
// Auth-path coverage the main suite never exercises: every other test in
// this package authenticates the same way (private_key_file, an unencrypted
// key). Both of this output's other two supported ways to supply key
// material -- private_key (inline) and private_key_pass (an encrypted key)
// -- were previously verified only at the pure-parsing level (auth.go's own
// unit tests), never against a real account's actual token-mint/streaming
// path.
// ---------------------------------------------------------------------------

// TestIntegrationSnowflakeStreamingPipeInlinePrivateKey proves the
// private_key field (the key's PEM text set directly in config, as opposed
// to private_key_file naming a path) actually authenticates against a real
// account. Every other test in this package uses private_key_file --
// private_key is otherwise only exercised by auth.go's own unit tests,
// which never mint a real JWT or reach a real endpoint with the result.
func TestIntegrationSnowflakeStreamingPipeInlinePrivateKey(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)
	pk := loadSnowflakeITPrivateKey(t, env)
	sql := newSSv2SQLClient(t, env, pk)
	ctx := t.Context()

	table := env.Table + "_INLINEKEY"
	pipe := env.Pipe + "_INLINEKEY"
	provisionSSv2Tier1Objects(t, ctx, sql, env, table, pipe)

	keyPEM, err := os.ReadFile(env.PrivateKeyFile)
	require.NoError(t, err, "reading %s", env.PrivateKeyFile)

	opts := defaultSSv2OutputOpts()
	opts.inlinePrivateKey = string(keyPEM)
	rows := []*service.Message{
		ssv2Row(`{"stake": 8.25, "bet_id": "inline-1"}`, 0, 1),
	}
	sendSSv2RowsWithOpts(t, env, pipe, 1, opts, rows)
	waitForSSv2RowCount(t, ctx, sql, env, table, 1, 90*time.Second)
}

// TestIntegrationSnowflakeStreamingPipeEncryptedPrivateKey proves
// private_key_pass actually decrypts a real encrypted key and authenticates
// with it. Re-encrypts the account's own already-registered key under a
// fresh, test-local passphrase rather than requiring a second key to be
// registered with Snowflake -- encrypting a private key changes nothing
// about the RSA key pair itself (still the same public key Snowflake
// already trusts), only how the private half is wrapped at rest, so the
// account setup in internal/impl/snowflake/README.md needs no changes for
// this test to be meaningful.
func TestIntegrationSnowflakeStreamingPipeEncryptedPrivateKey(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)
	pk := loadSnowflakeITPrivateKey(t, env)
	sql := newSSv2SQLClient(t, env, pk)
	ctx := t.Context()

	table := env.Table + "_ENCKEY"
	pipe := env.Pipe + "_ENCKEY"
	provisionSSv2Tier1Objects(t, ctx, sql, env, table, pipe)

	const passphrase = "rpcn-it-encrypted-key-test"
	der, err := pkcs8.MarshalPrivateKey(pk, []byte(passphrase), nil)
	require.NoError(t, err, "encrypting the registered key under a test passphrase")
	encPEM := pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: der})

	encPath := filepath.Join(t.TempDir(), "encrypted_key.p8")
	require.NoError(t, os.WriteFile(encPath, encPEM, 0o600))

	opts := defaultSSv2OutputOpts()
	opts.privateKeyFile = encPath
	opts.privateKeyPass = passphrase
	rows := []*service.Message{
		ssv2Row(`{"stake": 9.75, "bet_id": "enc-1"}`, 0, 1),
	}
	sendSSv2RowsWithOpts(t, env, pipe, 1, opts, rows)
	waitForSSv2RowCount(t, ctx, sql, env, table, 1, 90*time.Second)
}
