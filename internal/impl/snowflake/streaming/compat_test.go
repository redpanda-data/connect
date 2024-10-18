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
	"crypto/aes"
	"encoding/base64"
	"encoding/hex"
	"slices"
	"testing"
	"time"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
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

func mustHexDecode(s string) []byte {
	decoded, err := hex.DecodeString(s)
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
	decoded := mustHexDecode("aa")
	require.Equal(t, "aa", truncateBytesAsHex(decoded, false))
	require.Equal(t, "aa", truncateBytesAsHex(decoded, true))

	// Test exactly 32 bytes
	decoded = mustHexDecode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", truncateBytesAsHex(decoded, false))
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", truncateBytesAsHex(decoded, true))

	decoded = mustHexDecode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	require.Equal(t, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", truncateBytesAsHex(decoded, false))
	require.Equal(t, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", truncateBytesAsHex(decoded, true))

	// Test 1 truncate up
	decoded = mustHexDecode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", truncateBytesAsHex(decoded, false))
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", truncateBytesAsHex(decoded, true))

	// Test one overflow
	decoded = mustHexDecode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffaaffffffff")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffaaff", truncateBytesAsHex(decoded, false))
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffab00", truncateBytesAsHex(decoded, true))

	// Test many overflow
	decoded = mustHexDecode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffffffffffffffffffffffff")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffffff", truncateBytesAsHex(decoded, false))
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaab00000000000000000000000000000000000", truncateBytesAsHex(decoded, true))

	// Test infinity
	decoded = mustHexDecode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccccccc")
	require.Equal(t, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", truncateBytesAsHex(decoded, false))
	require.Equal(t, "Z", truncateBytesAsHex(decoded, true))
}

func mustBase64Decode(s string) []byte {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

// TestCompat takes each stage of transforms that are applied in the JavaSDK and ensures that this SDK is byte for byte the same.
func TestCompat(t *testing.T) {
	unpadded := mustBase64Decode("UEFSMRUAFUwVPhWUpsKLARwVBBUAFQYVCAAAH4sIAAAAAAAA/2NiYGBgZmZABT7ofABnJDzZJgAAABUAFSgVQhXlo/S6CRwVBBUAFQYVCAAAH4sIAAAAAAAA/2NiYGBgZmYGkoWlFVAKAA+YiDUUAAAAFQAVDhU2FZ/44TAcFQQVABUGFQgAAB+LCAAAAAAAAP9jYmBgYGZmBgB3cpG6BwAAABkRAhkYEAAAAAAAAAAAAAAAAAAAAEwZGBAAAAAAAAAAAAAAAAAAAABMFQIZFgAZFgQZJgAEABkRAhkYA3F1eBkYA3F1eBUCGRYAGRYEGSYABAAZEQIZGAEBGRgBARUCGRYAGRYEGSYABAAZHBYIFWwWAAAAGRwWdBVwFgAAGRYMABkcFuQBFWIWAAAAFQIZTEgEYmRlYxUGABUOFSAVAhgBQSUKFQAVTBUCHFwVABVMAAAAFQwlAhgBQiUANQQcHAAAABUAJQIYAUNVBgAWBBkcGTwmCBwVDhk1BggAGRgBQRUEFgQWehZsJgg8GBAAAAAAAAAAAAAAAAAAAABMGBAAAAAAAAAAAAAAAAAAAABMFgAoEAAAAAAAAAAAAAAAAAAAAEwYEAAAAAAAAAAAAAAAAAAAAEwAGRwVABUAFQIAPCkWBBkmAAQAABaaBBUUFsYCFWwAJnQcFQwZNQYIABkYAUIVBBYEFlYWcCZ0PBgDcXV4GANxdXgWACgDcXV4GANxdXgAGRwVABUAFQIAPBYMGRYEGSYABAAAFq4EFRoWsgMVOAAm5AEcFQAZNQYIABkYAUMVBBYEFjoWYibkATwYAQEYAQEWACgBARgBAQAZHBUAFQAVAgA8KRYEGSYABAAAFsgEFRYW6gMVMAAWigIWBCYIFr4CFAAAGVwYATEYAzIsNQAYATIYAzksOAAYATMYAzEsMQAYBXNmVmVyGAMxLDEAGA1wcmltYXJ5RmlsZUlkGENzbDFpejVfOVFqUVVKRDJZeGhrQ0hOZFZmUVR0dDBoR1JPR2tiMzdJTlIzM3BoRU00c0NDXzMwMDFfMzRfMC5iZGVjABhKcGFycXVldC1tciB2ZXJzaW9uIDEuMTQuMSAoYnVpbGQgOTdlZGU5NjgzNzc0MDBkMWQ3OWUzMTk2NjM2YmEzZGUzOTIxOTZiYSkZPBwAABwAABwAAABFAgAAUEFSMQ==")
	actualPadded := padBuffer(slices.Clone(unpadded), aes.BlockSize)
	padded := mustBase64Decode("UEFSMRUAFUwVPhWUpsKLARwVBBUAFQYVCAAAH4sIAAAAAAAA/2NiYGBgZmZABT7ofABnJDzZJgAAABUAFSgVQhXlo/S6CRwVBBUAFQYVCAAAH4sIAAAAAAAA/2NiYGBgZmYGkoWlFVAKAA+YiDUUAAAAFQAVDhU2FZ/44TAcFQQVABUGFQgAAB+LCAAAAAAAAP9jYmBgYGZmBgB3cpG6BwAAABkRAhkYEAAAAAAAAAAAAAAAAAAAAEwZGBAAAAAAAAAAAAAAAAAAAABMFQIZFgAZFgQZJgAEABkRAhkYA3F1eBkYA3F1eBUCGRYAGRYEGSYABAAZEQIZGAEBGRgBARUCGRYAGRYEGSYABAAZHBYIFWwWAAAAGRwWdBVwFgAAGRYMABkcFuQBFWIWAAAAFQIZTEgEYmRlYxUGABUOFSAVAhgBQSUKFQAVTBUCHFwVABVMAAAAFQwlAhgBQiUANQQcHAAAABUAJQIYAUNVBgAWBBkcGTwmCBwVDhk1BggAGRgBQRUEFgQWehZsJgg8GBAAAAAAAAAAAAAAAAAAAABMGBAAAAAAAAAAAAAAAAAAAABMFgAoEAAAAAAAAAAAAAAAAAAAAEwYEAAAAAAAAAAAAAAAAAAAAEwAGRwVABUAFQIAPCkWBBkmAAQAABaaBBUUFsYCFWwAJnQcFQwZNQYIABkYAUIVBBYEFlYWcCZ0PBgDcXV4GANxdXgWACgDcXV4GANxdXgAGRwVABUAFQIAPBYMGRYEGSYABAAAFq4EFRoWsgMVOAAm5AEcFQAZNQYIABkYAUMVBBYEFjoWYibkATwYAQEYAQEWACgBARgBAQAZHBUAFQAVAgA8KRYEGSYABAAAFsgEFRYW6gMVMAAWigIWBCYIFr4CFAAAGVwYATEYAzIsNQAYATIYAzksOAAYATMYAzEsMQAYBXNmVmVyGAMxLDEAGA1wcmltYXJ5RmlsZUlkGENzbDFpejVfOVFqUVVKRDJZeGhrQ0hOZFZmUVR0dDBoR1JPR2tiMzdJTlIzM3BoRU00c0NDXzMwMDFfMzRfMC5iZGVjABhKcGFycXVldC1tciB2ZXJzaW9uIDEuMTQuMSAoYnVpbGQgOTdlZGU5NjgzNzc0MDBkMWQ3OWUzMTk2NjM2YmEzZGUzOTIxOTZiYSkZPBwAABwAABwAAABFAgAAUEFSMQAAAAA=")
	require.Equal(t, padded, actualPadded)
	encryptionKey := "i3aoKhzaBpbgJ7NtZHagllmUxTDJEbcEObJg+OMbZio="
	blobPath := "2024/10/8/14/1/sl1iz5_9QjQUJD2YxhkCHNdVfQTtt0hGROGkb37INR33phEM4sCC_3001_34_0.bdec"
	actualEncrypted, err := encrypt(slices.Clone(padded), encryptionKey, blobPath, 0)
	require.NoError(t, err)
	encrypted := mustBase64Decode("ZBVRKvbk6yq2rtif+3FeYsuVP6bh0JSvaViL843qnI+Nqcvl74xBYaFQ0YKbxRTg2pBGW2VHDQOPk03Fbg7ENHJGJFbv0Dr7R1sMQyMyHXQdQMEknrpinkomPA04K5EnNlJTY21pDqL4xpTBdeZWzX0SPGvhwQnSCmMPvNWsdeTq5fnqtunNfJES9FwKvVU1DVGoOewOs/sR7j7/IjVkcK8YElO+pqAMbf8OqFsoeVpWcaroT5fxZiSMZQ6jBRoBSRAtkFi9WFwEW6eGq+iMu9CGccumSOb48wj4aa8EuyZRWYa5vDqnJYz76+ea91Akvp1+OKkoA7QTUY7iBi4emH8AdeRlG35F5O/JCbZ1sNUhEoJSTQfRID582lK1MRsVaxwamJw/2Ty3NG80S22dVV2ILhjl38GZjypJHihCFjkU8g9qkEvhuwNrEeK6xwWJ6DF+OtxE6PzVUdNgOWzwFxRMASayZWyAH/+1KCVCIbURS5lDbT/Mv+fEA6waKasgiynqAIw/1z2c39h+ThtxNKWVaZzENGOOjAWpaKTSxQ8UiaiSG7WBtFtAmYJlQ5mAJO+i133Xipv86mVJv8OudRoIzYM8pZMVIP/Y7RD3kCkP3IzGS9QDQOhC8aXomHcEaXK+Z9iCewe9T+atdUX18OSuEr9owcI0Eu7gvWnpRK5fWVRqi3i+uz/HdmKF0qcmEDTzuMs+PvUl84J9kJjR1Savr4UKmZlp3u/i+nXTx0zgrV/NtdX4eXJMeaCaP2AJfKQzY1UCSFZS/5mSzsRzk/R3SiFLee7caWq7HsAQEAdpMz2pvylSxS0YCxL5KivGk/sKAMjaDRvQpblO5zcKH+mFaTgehpVr4oqaIwdMVw5Q7aRrjol97zMNu95kdCk8m2vyFvZKLzk+WWVxK645fJYUE2v/B8M3H3phVDJqn4//gGsQG/xLdwBWFpI1W9GZq4F3qvAxeB3XldKV1IsgH+ygBkxAAvlexba3Qb+rWnE9B+KjX+r8u8qI1WIDObF71NQ0m/bDgCz1KhIyUaYUu7O++U4vUK/e2TD2nX5+m3m3DAxHQousdiodh1C5dr249v0GTcbnKlCNLOMRCLdB222Xd2pQPI5M7p0Dj+yNrecD6FlIeLavEJF3QvE6urwmO8nMaJJ3WmX+euCO1Yia1m5gFBVnaSGSI1RmqxAiSUQ=")
	require.Equal(t, encrypted, actualEncrypted)
	fileMD5Hash := "c211779e08513408f0a8b28a17c230b0"
	require.Equal(t, md5Hash(actualEncrypted), fileMD5Hash)
	chunkMD5Hash := "1ca9f885bedc25ded3abf3df045543be"
	require.Equal(t, md5Hash(actualEncrypted[:len(unpadded)]), chunkMD5Hash)
}

func TestColumnNormalization(t *testing.T) {
	require.Equal(t, "", normalizeColumnName(""))
	require.Equal(t, "FOO", normalizeColumnName("foo"))
	require.Equal(t, `bar`, normalizeColumnName(`"bar"`))
	require.Equal(t, "'BAR'", normalizeColumnName(`'bar'`))
	require.Equal(t, "BAR", normalizeColumnName(`bar`))
	require.Equal(t, `C1`, normalizeColumnName(`"C1"`))
	require.Equal(t, `how are you`, normalizeColumnName(`"how are you"`))
	require.Equal(t, `HOW ARE YOU`, normalizeColumnName(`how are you`))
	require.Equal(t, `how\ are\ you`, normalizeColumnName(`"how\ are\ you"`))
	require.Equal(t, `HOW ARE YOU`, normalizeColumnName(`how\ are\ you`))
	require.Equal(t, `"FOO`, normalizeColumnName(`"foo`))
	require.Equal(t, `FOO"`, normalizeColumnName(`foo"`))
	require.Equal(t, `FOO" BAR "BAZ`, normalizeColumnName(`foo" bar "baz`))
	require.Equal(t, `"FOO \"BAZ"`, normalizeColumnName(`"foo \"baz"`))
	require.Equal(t, `"FOO \"BAZ"`, normalizeColumnName(`"foo \"baz"`))
	require.Equal(t, `foo" bar "baz`, normalizeColumnName(`"foo"" bar ""baz"`))
}

func TestSnowflakeTimestamp(t *testing.T) {
	type TestCase struct {
		timestamp string
		value     int128.Int128
		scale     int32
		keepTZ    bool
		tz        bool
	}
	cases := [...]TestCase{
		{
			timestamp: "2021-01-01 01:00:00.123",
			value:     int128.Int64(1609462800123000000),
			scale:     9,
		},
		{
			timestamp: "1971-01-01 00:00:00.001",
			value:     int128.Mul(int128.Int64(31536000001), int128.Int64(1000000)),
			scale:     9,
		},
		{
			timestamp: "1971-01-01 00:00:00.000",
			value:     int128.Mul(int128.Int64(31536000000), int128.Int64(1000000)),
			scale:     9,
		},
		{
			timestamp: "2021-01-01 01:00:00.123",
			value:     int128.Int64(1609462800123000000),
			scale:     9,
		},
		{
			timestamp: "2021-01-01 01:00:00.123",
			value:     int128.Int64(16094628001230),
			scale:     4,
		},
		{
			timestamp: "2021-01-01 01:00:00.123+01:00",
			value:     int128.Int64(263693795348153820),
			scale:     4,
			keepTZ:    true,
			tz:        true,
		},
		{
			timestamp: "2021-01-01 01:00:00.123+01:00",
			value:     int128.MustParse("26369379534815232001500"),
			scale:     9,
			keepTZ:    true,
			tz:        true,
		},
		{
			timestamp: "2024-01-01 12:00:00.000-08:00",
			value:     int128.MustParse("1704139200000000000"),
			scale:     9,
			keepTZ:    true,
			tz:        false,
		},
		{
			timestamp: "2024-01-01 12:00:00.000-08:00",
			value:     int128.MustParse("27920616652800000000960"),
			scale:     9,
			keepTZ:    true,
			tz:        true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run("", func(t *testing.T) {
			layout := "2006-01-02 15:04:05.000"
			if c.keepTZ {
				layout = "2006-01-02 15:04:05.000-07:00"
			}
			parsed, err := time.Parse(layout, c.timestamp)
			require.NoError(t, err)
			require.Equal(t, c.value, snowflakeTimestampInt(parsed, c.scale, c.tz))
		})
	}
}
