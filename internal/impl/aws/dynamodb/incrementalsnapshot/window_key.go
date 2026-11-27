// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"encoding/base64"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// WindowKeyFromItem encodes a Scan item's primary key for the incremental
// snapshot window. Unlike the dynamodb package's buildItemKeyString it is
// injective: every part is length-prefixed and typed, and binary values are
// base64 encoded, so two distinct keys can never share a string. A
// collision would let one item's stream event drop another item that no
// event replaces.
func WindowKeyFromItem(item map[string]dynamodbtypes.AttributeValue, keySchema []dynamodbtypes.KeySchemaElement) (string, bool) {
	if len(keySchema) == 0 {
		return "", false
	}
	names := make([]string, 0, len(keySchema))
	for _, e := range keySchema {
		names = append(names, aws.ToString(e.AttributeName))
	}
	sort.Strings(names)

	var sb strings.Builder
	for _, name := range names {
		var tag byte
		var val string
		switch v := item[name].(type) {
		case *dynamodbtypes.AttributeValueMemberS:
			tag, val = 'S', v.Value
		case *dynamodbtypes.AttributeValueMemberN:
			tag, val = 'N', v.Value
		case *dynamodbtypes.AttributeValueMemberB:
			tag, val = 'B', base64.StdEncoding.EncodeToString(v.Value)
		default:
			return "", false
		}
		writeWindowKeyPart(&sb, name, tag, val)
	}
	return sb.String(), true
}

// WindowKeyFromStream encodes a stream record's Keys exactly as
// WindowKeyFromItem encodes the same key from a Scan item.
func WindowKeyFromStream(keys map[string]streamstypes.AttributeValue) (string, bool) {
	if len(keys) == 0 {
		return "", false
	}
	names := make([]string, 0, len(keys))
	for name := range keys {
		names = append(names, name)
	}
	sort.Strings(names)

	var sb strings.Builder
	for _, name := range names {
		var tag byte
		var val string
		switch v := keys[name].(type) {
		case *streamstypes.AttributeValueMemberS:
			tag, val = 'S', v.Value
		case *streamstypes.AttributeValueMemberN:
			tag, val = 'N', v.Value
		case *streamstypes.AttributeValueMemberB:
			tag, val = 'B', base64.StdEncoding.EncodeToString(v.Value)
		default:
			return "", false
		}
		writeWindowKeyPart(&sb, name, tag, val)
	}
	return sb.String(), true
}

func writeWindowKeyPart(sb *strings.Builder, name string, tag byte, val string) {
	sb.WriteString(strconv.Itoa(len(name)))
	sb.WriteByte(':')
	sb.WriteString(name)
	sb.WriteByte(tag)
	sb.WriteString(strconv.Itoa(len(val)))
	sb.WriteByte(':')
	sb.WriteString(val)
}
