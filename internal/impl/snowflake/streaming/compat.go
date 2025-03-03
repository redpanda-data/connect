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
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
)

var (
	pow10TableInt32 []int32
	pow10TableInt64 []int64
)

func init() {
	{
		pow10TableInt64 = make([]int64, 19)
		n := int64(1)
		pow10TableInt64[0] = n
		for i := range pow10TableInt64[1:] {
			n = 10 * n
			pow10TableInt64[i+1] = n
		}
	}
	{
		pow10TableInt32 = make([]int32, 19)
		n := int32(1)
		pow10TableInt32[0] = n
		for i := range pow10TableInt32[1:] {
			n = 10 * n
			pow10TableInt32[i+1] = n
		}
	}
}

func deriveKey(encryptionKey, diversifier string) ([]byte, error) {
	decodedKey, err := base64.StdEncoding.DecodeString(encryptionKey)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	hash.Write(decodedKey)
	hash.Write([]byte(diversifier))
	return hash.Sum(nil)[:], nil
}

// See Encyptor.encrypt in the Java SDK
func encrypt(buf []byte, encryptionKey string, diversifier string, iv int64) ([]byte, error) {
	// Derive the key from the diversifier and the original encryptionKey from server
	key, err := deriveKey(encryptionKey, diversifier)
	if err != nil {
		return nil, err
	}
	// Using our derived key and padded input, encrypt the thing.
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	// Create our cypher using the iv
	ivBytes := make([]byte, aes.BlockSize)
	binary.BigEndian.PutUint64(ivBytes[8:], uint64(iv))
	stream := cipher.NewCTR(block, ivBytes)
	// Actually do the encryption in place
	stream.XORKeyStream(buf, buf)
	return buf, nil
}

func padBuffer(buf []byte, alignmentSize int) []byte {
	padding := alignmentSize - len(buf)%alignmentSize
	return append(buf, make([]byte, padding)...)
}

func md5Hash(b []byte) string {
	s := md5.Sum(b)
	return hex.EncodeToString(s[:])
}

// Generate the path for a blob when uploading to an internal snowflake table.
//
// Never change, this must exactly match the java SDK, don't think you can be fancy and change something.
func generateBlobPath(clientPrefix string, threadID, counter int64) string {
	now := time.Now().UTC()
	year := now.Year()
	month := int(now.Month())
	day := now.Day()
	hour := now.Hour()
	minute := now.Minute()
	blobShortName := fmt.Sprintf("%s_%s_%d_%d.bdec", strconv.FormatInt(now.Unix(), 36), clientPrefix, threadID, counter)
	return fmt.Sprintf("%d/%d/%d/%d/%d/%s", year, month, day, hour, minute, blobShortName)
}

// truncateBytesAsHex truncates an array of bytes up to 32 bytes and optionally increment the last byte(s).
// More the one byte can be incremented in case it overflows.
func truncateBytesAsHex(bytes []byte, truncateUp bool) string {
	const maxLobLen int = 32
	if len(bytes) <= maxLobLen {
		return hex.EncodeToString(bytes)
	}
	bytes = slices.Clone(bytes)
	if truncateUp {
		var i int
		for i = maxLobLen - 1; i >= 0; i-- {
			bytes[i]++
			if bytes[i] != 0 {
				break
			}
		}
		if i < 0 {
			return "Z"
		}
	}
	return hex.EncodeToString(bytes[:maxLobLen])
}

// normalizeColumnName normalizes the column to the same as Snowflake's
// internal representation. See LiteralQuoteUtils.unquoteColumnName in
// the Java SDK for reference, although that code is quite hard to read.
func normalizeColumnName(name string) string {
	if strings.HasPrefix(name, `"`) && strings.HasSuffix(name, `"`) {
		unquoted := name[1 : len(name)-1]
		noDoubleQuotes := strings.ReplaceAll(unquoted, `""`, ``)
		if !strings.ContainsRune(noDoubleQuotes, '"') {
			return strings.ReplaceAll(unquoted, `""`, `"`)
		}
		if !strings.ContainsRune(unquoted, '"') {
			return unquoted
		}
		// fallthrough
	}
	// Add a fast path if there is no escaping note that this is an optimized version of
	//   strings.ToUpper(strings.ReplaceAll(name, `\ `, ` `))
	// which indeed we fallback to that if we get unicode or any escaped spaces.

	// First check to see if the name is already normalized, in that case we can save
	// an alloc however most strings I assume are in snake or camel casing so those
	// will likely just check the first byte in this loop then bail, so this extra
	// loop allows for still optimizing performance over just calling into the stdlib.
	hasLower := false
	for _, c := range []byte(name) {
		if 'a' <= c && c <= 'z' {
			hasLower = true
			break // must alloc
		} else if c >= utf8.RuneSelf || c == '\\' {
			// Fallback
			return strings.ToUpper(strings.ReplaceAll(name, `\ `, ` `))
		}
	}
	if !hasLower {
		return name
	}
	transformed := []byte(name)
	for i, c := range transformed {
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
			transformed[i] = c
		} else if c >= utf8.RuneSelf || c == '\\' {
			// Fallback
			return strings.ToUpper(strings.ReplaceAll(name, `\ `, ` `))
		}
	}
	return string(transformed)
}

// quoteColumnName escapes an object identifier according to the
// rules in Snowflake.
//
// https://docs.snowflake.com/en/sql-reference/identifiers-syntax
func quoteColumnName(name string) string {
	var quoted strings.Builder
	// Default to assume we're just going to add quotes and there won't
	// be any double quotes inside the string that needs escaped.
	quoted.Grow(len(name) + 2)
	quoted.WriteByte('"')
	for _, r := range strings.ToUpper(name) {
		if r == '"' {
			quoted.WriteString(`""`)
		} else {
			quoted.WriteRune(r)
		}
	}
	quoted.WriteByte('"')
	return quoted.String()
}

// snowflakeTimestampInt computes the same result as the logic in TimestampWrapper
// in the Java SDK. It converts a timestamp to the integer representation that
// is used internally within Snowflake.
func snowflakeTimestampInt(t time.Time, scale int32, includeTZ bool) int128.Num {
	epoch := int128.FromInt64(t.Unix())
	// this calculation is intentionally done at low resolution to truncate the nanoseconds
	// according to our scale.
	fraction := (int32(t.Nanosecond()) / pow10TableInt32[9-scale]) * pow10TableInt32[9-scale]
	timeInNanos := int128.Add(
		int128.Mul(epoch, int128.Pow10Table[9]),
		int128.FromInt64(int64(fraction)),
	)
	scaledTime := int128.Div(timeInNanos, int128.Pow10Table[9-scale])
	if includeTZ {
		_, tzOffsetSec := t.Zone()
		offsetMinutes := tzOffsetSec / 60
		offsetMinutes += 1440
		scaledTime = int128.Shl(scaledTime, 14)
		const tzMask = (1 << 14) - 1
		scaledTime = int128.Add(scaledTime, int128.FromInt64(int64(offsetMinutes&tzMask)))
	}
	return scaledTime
}
