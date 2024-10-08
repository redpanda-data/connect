/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package streaming

import (
	"encoding/base64"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryption(t *testing.T) {
	data := []byte("testEncryptionDecryption")
	key := base64.StdEncoding.EncodeToString([]byte("encryption_key"))
	diversifier := "2021/08/10/blob.bdec"
	actual, err := encrypt(data, key, diversifier, 0)
	require.NoError(t, err)
	// this value was obtained from the Cryptor unit tests in the Java SDK
	expected := []byte{133, 80, 92, 68, 33, 84, 54, 127, 139, 26, 89, 42, 80, 118, 6, 27, 56, 48, 149, 113, 118, 62, 50, 158}
	require.Equal(t, expected, actual)
}

func MustHexDecode(s string) []byte {
	decoded, err := hex.DecodeString("aa")
	if err != nil {
		panic(err)
	}
	return decoded
}

func TestTruncateBytesAsHex(t *testing.T) {
	// Test empty input
	require.Equal(t, "", truncateBytesAsHex([]byte{}, false))
	require.Equal(t, "", truncateBytesAsHex([]byte{}, true))

	// Test basic case
	decoded := MustHexDecode("aa")
	require.Equal(t, "aa", truncateBytesAsHex(decoded, false))
	require.Equal(t, "aa", truncateBytesAsHex(decoded, true))

	// Test exactly 32 bytes
	decoded, _ = hex.DecodeString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", truncateBytesAsHex(decoded, false))
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", truncateBytesAsHex(decoded, true))

	decoded, _ = hex.DecodeString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	require.Equal(t, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", truncateBytesAsHex(decoded, false))
	require.Equal(t, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", truncateBytesAsHex(decoded, true))

	// Test 1 truncate up
	decoded, _ = hex.DecodeString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", truncateBytesAsHex(decoded, false))
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", truncateBytesAsHex(decoded, true))

	// Test one overflow
	decoded, _ = hex.DecodeString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffaaffffffff")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffaaff", truncateBytesAsHex(decoded, false))
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffab00", truncateBytesAsHex(decoded, true))

	// Test many overflow
	decoded, _ = hex.DecodeString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffffffffffffffffffffffff")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffffff", truncateBytesAsHex(decoded, false))
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaab00000000000000000000000000000000000", truncateBytesAsHex(decoded, true))

	// Test infinity
	decoded, _ = hex.DecodeString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccccccc")
	require.Equal(t, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", truncateBytesAsHex(decoded, false))
	require.Equal(t, "Z", truncateBytesAsHex(decoded, true))
}
