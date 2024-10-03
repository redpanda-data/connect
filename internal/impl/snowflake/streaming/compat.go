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
	"math/bits"
	"strconv"
	"strings"
	"time"
)

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
	// Actually do the encryption
	// TODO(perf): write in place?
	encrypted := make([]byte, len(buf))
	stream.XORKeyStream(encrypted, buf)
	return encrypted, nil
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
func generateBlobPath(clientPrefix string, threadID, counter int) string {
	now := time.Now().UTC()
	year := now.Year()
	month := int(now.Month())
	day := now.Day()
	hour := now.Hour()
	minute := now.Minute()
	blobShortName := fmt.Sprintf("%s_%s_%d_%d.bdec", strconv.FormatInt(now.Unix(), 36), clientPrefix, threadID, counter)
	return fmt.Sprintf("%d/%d/%d/%d/%d/%s", year, month, day, hour, minute, blobShortName)
}

// Get the filename from an above generated blobPath
func getShortname(blobPath string) string {
	idx := strings.LastIndexByte(blobPath, '/')
	return blobPath[idx+1:]
}

// truncateBytesAsHex truncates an array of bytes up to 32 bytes and optionally increment the last byte(s).
// More the one byte can be incremented in case it overflows.
//
// NOTE: This can mutate `bytes`
func truncateBytesAsHex(bytes []byte, truncateUp bool) string {
	const maxLobLen int = 32
	if len(bytes) <= maxLobLen {
		return hex.EncodeToString(bytes)
	}
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

func int64ToInt128Binary(v int64) [16]byte {
	var rawBytes [8]byte
	binary.BigEndian.PutUint64(rawBytes[:], uint64(v))
	bytesNeeded := (bits.Len64(uint64(v)) + 7) / 8
	bytes := rawBytes[8-bytesNeeded:]
	var be [16]byte
	overwriteLen := 16 - len(bytes)
	if v < 0 {
		for i := 0; i < overwriteLen; i++ {
			be[i] = 0xFF
		}
	}
	copy(be[overwriteLen:], bytes)
	return be
}
