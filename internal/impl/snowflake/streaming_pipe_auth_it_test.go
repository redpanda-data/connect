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

// The other tests all authenticate via an unencrypted private_key_file;
// these two cover the inline private_key and private_key_pass paths.

// TestIntegrationSnowflakeStreamingPipeInlinePrivateKey: the inline
// private_key field authenticates against a real account.
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

// TestIntegrationSnowflakeStreamingPipeEncryptedPrivateKey: private_key_pass
// decrypts an encrypted key and authenticates with it. The account's own key
// is re-encrypted under a test passphrase, so no second key needs
// registering.
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
