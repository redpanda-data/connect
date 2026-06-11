// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// validateChangeType normalises and validates a Bloblang-resolved _CHANGE_TYPE
// value against BigQuery's CDC contract. BigQuery only accepts UPSERT and
// DELETE; INSERT is expressed by omitting the pseudo-column entirely, and
// mixing INSERT with UPSERT/DELETE in the same write is unsupported.
// allowDelete is true for write_mode=upsert_delete.
func validateChangeType(raw string, allowDelete bool) (string, error) {
	v := strings.ToUpper(strings.TrimSpace(raw))
	switch v {
	case "UPSERT":
		return v, nil
	case "DELETE":
		if !allowDelete {
			return "", errors.New("change_type DELETE is only valid when write_mode is upsert_delete")
		}
		return v, nil
	case "":
		return "", errors.New("change_type resolved to an empty value")
	default:
		return "", fmt.Errorf("change_type %q is not UPSERT or DELETE", raw)
	}
}

// changeSequenceNumberPattern matches BigQuery's _CHANGE_SEQUENCE_NUMBER format:
// 1 to 4 sections of 1 to 16 hexadecimal characters each, separated by '/'.
var changeSequenceNumberPattern = regexp.MustCompile(`^[0-9A-Fa-f]{1,16}(/[0-9A-Fa-f]{1,16}){0,3}$`)

// validateChangeSequenceNumber checks that raw conforms to BigQuery's CDC
// sequence-number format. BigQuery rejects values that do not match this
// format with INVALID_ARGUMENT at AppendRows time, so we validate per-message
// before the network round trip.
func validateChangeSequenceNumber(raw string) error {
	if !changeSequenceNumberPattern.MatchString(raw) {
		return fmt.Errorf("change_sequence_number %q does not match BigQuery format (1-4 sections of 1-16 hex chars separated by /)", raw)
	}
	return nil
}

// wrapDescriptorForCDC returns a copy of base with `_CHANGE_TYPE` and
// optionally `_CHANGE_SEQUENCE_NUMBER` appended as STRING fields at the next
// free field numbers (max existing + 1, +2). The base descriptor is left
// untouched. Injection is name-based (see injectCDCJSON), so the field numbers
// don't need to be returned — they only need to be valid and unused.
func wrapDescriptorForCDC(base *descriptorpb.DescriptorProto, withSequenceNumber bool) (*descriptorpb.DescriptorProto, error) {
	if base == nil {
		return nil, errors.New("wrapDescriptorForCDC: nil base descriptor")
	}
	var maxNum int32
	for _, f := range base.Field {
		if n := f.GetNumber(); n > maxNum {
			maxNum = n
		}
	}
	changeTypeFieldNumber := maxNum + 1
	wrapped := proto.Clone(base).(*descriptorpb.DescriptorProto)
	wrapped.Field = append(wrapped.Field, &descriptorpb.FieldDescriptorProto{
		Name:   new("_CHANGE_TYPE"),
		Number: new(changeTypeFieldNumber),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	})
	if withSequenceNumber {
		changeSeqFieldNumber := maxNum + 2
		wrapped.Field = append(wrapped.Field, &descriptorpb.FieldDescriptorProto{
			Name:   new("_CHANGE_SEQUENCE_NUMBER"),
			Number: new(changeSeqFieldNumber),
			Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		})
	}
	return wrapped, nil
}

// cdcInjector holds the immutable per-output CDC configuration: validated
// Bloblang fields and the allowDelete flag. Safe to share across concurrent
// WriteBatch goroutines because all fields are read-only after construction.
type cdcInjector struct {
	changeType  *service.InterpolatedString
	changeSeq   *service.InterpolatedString // nil when not configured
	allowDelete bool                        // true for write_mode=upsert_delete
}

// injectCDCJSON injects _CHANGE_TYPE (and _CHANGE_SEQUENCE_NUMBER when seq is
// non-empty) into the JSON object bytes without round-tripping through a Go
// value tree. Using json.RawMessage preserves the exact byte representation of
// every user field — int64 strings stay strings, NUMERIC strings keep their
// precision, base64 BYTES stay base64. Returns an error if jsonBytes is not a
// JSON object, or if the payload already carries a key the injector would
// otherwise overwrite (forces explicit user remediation rather than silent
// data loss).
func injectCDCJSON(jsonBytes []byte, changeType, changeSeq string) ([]byte, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(jsonBytes, &raw); err != nil {
		return nil, fmt.Errorf("parsing JSON: %w", err)
	}
	if raw == nil {
		// Empty map after decoding a JSON null — reject; CDC needs an object.
		return nil, errors.New("payload is not a JSON object")
	}
	if _, exists := raw["_CHANGE_TYPE"]; exists {
		return nil, errors.New("payload already contains _CHANGE_TYPE; remove it before CDC injection")
	}
	encodedCT, err := json.Marshal(changeType)
	if err != nil {
		return nil, fmt.Errorf("encoding change_type: %w", err)
	}
	raw["_CHANGE_TYPE"] = encodedCT
	if changeSeq != "" {
		if _, exists := raw["_CHANGE_SEQUENCE_NUMBER"]; exists {
			return nil, errors.New("payload already contains _CHANGE_SEQUENCE_NUMBER; remove it before CDC injection")
		}
		encodedSeq, err := json.Marshal(changeSeq)
		if err != nil {
			return nil, fmt.Errorf("encoding change_sequence_number: %w", err)
		}
		raw["_CHANGE_SEQUENCE_NUMBER"] = encodedSeq
	}
	return json.Marshal(raw)
}

// validateCDCPrimaryKeys enforces BigQuery's CDC contract that the destination
// table must have a PRIMARY KEY. configPKs is the user-declared list (may be
// nil), tablePKs is what BigQuery reports for the table (may be nil if no
// constraint is declared). The function fails if neither source has PKs, and
// fails on mismatch when both are present.
func validateCDCPrimaryKeys(configPKs, tablePKs []string, tableID string) error {
	if len(configPKs) == 0 && len(tablePKs) == 0 {
		return fmt.Errorf("CDC mode requires a PRIMARY KEY on table %q; declare one via primary_keys config or `ALTER TABLE … ADD PRIMARY KEY`", tableID)
	}
	if len(configPKs) > 0 && len(tablePKs) > 0 {
		// Order-sensitive: BigQuery PKs are positional (column order matches the
		// declaration in ALTER TABLE … ADD PRIMARY KEY), so the config list must
		// match the table list exactly, not just as a set.
		if !slices.Equal(configPKs, tablePKs) {
			return fmt.Errorf("primary_keys config %v does not match table %q PRIMARY KEY %v",
				configPKs, tableID, tablePKs)
		}
	}
	return nil
}

// validateAndResolveCDC validates the resolved Bloblang values for a single
// row and returns the normalised change_type plus the original sequence
// number string. The caller is responsible for passing the validated values
// to injectCDCJSON. Returns an error suitable for BatchError.Failed.
func (i *cdcInjector) validateAndResolveCDC(rawChangeType, rawSeq string) (changeType string, seq string, err error) {
	ct, err := validateChangeType(rawChangeType, i.allowDelete)
	if err != nil {
		return "", "", err
	}
	if i.changeSeq != nil {
		if err := validateChangeSequenceNumber(rawSeq); err != nil {
			return "", "", err
		}
		return ct, rawSeq, nil
	}
	return ct, "", nil
}
