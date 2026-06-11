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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestValidateChangeType(t *testing.T) {
	t.Run("upsert mode accepts UPSERT only", func(t *testing.T) {
		v, err := validateChangeType("UPSERT", false)
		require.NoError(t, err)
		assert.Equal(t, "UPSERT", v)

		v, err = validateChangeType("upsert", false)
		require.NoError(t, err)
		assert.Equal(t, "UPSERT", v)

		_, err = validateChangeType("DELETE", false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DELETE")
	})

	t.Run("upsert_delete mode accepts UPSERT and DELETE", func(t *testing.T) {
		v, err := validateChangeType("UPSERT", true)
		require.NoError(t, err)
		assert.Equal(t, "UPSERT", v)

		v, err = validateChangeType("delete", true)
		require.NoError(t, err)
		assert.Equal(t, "DELETE", v)
	})

	t.Run("rejects INSERT", func(t *testing.T) {
		_, err := validateChangeType("INSERT", true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "INSERT")
	})

	t.Run("rejects empty", func(t *testing.T) {
		_, err := validateChangeType("", true)
		require.Error(t, err)
	})

	t.Run("rejects arbitrary string", func(t *testing.T) {
		_, err := validateChangeType("MAYBE", true)
		require.Error(t, err)
	})
}

func TestValidateChangeSequenceNumber(t *testing.T) {
	t.Run("accepts valid formats", func(t *testing.T) {
		for _, in := range []string{
			"0",
			"FF",
			"abcd",
			"FFFFFFFFFFFFFFFF",
			"0/0",
			"A/B/C",
			"A/B/C/D",
			"FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF",
		} {
			t.Run(in, func(t *testing.T) {
				require.NoError(t, validateChangeSequenceNumber(in))
			})
		}
	})

	t.Run("rejects invalid formats", func(t *testing.T) {
		for _, in := range []string{
			"",
			"G",
			"/0",
			"0/",
			"0/0/0/0/0",
			"FFFFFFFFFFFFFFFFA",
			"0/FFFFFFFFFFFFFFFFA",
			"0 0",
			"0/0 /0",
		} {
			t.Run(in, func(t *testing.T) {
				err := validateChangeSequenceNumber(in)
				require.Error(t, err, "expected error for input %q", in)
			})
		}
	})
}

func TestWrapDescriptorForCDC(t *testing.T) {
	base := &descriptorpb.DescriptorProto{
		Name: new("TestMessage"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   new("id"),
				Number: new(int32(1)),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			},
		},
	}

	t.Run("appends _CHANGE_TYPE only when no sequence configured", func(t *testing.T) {
		wrapped, err := wrapDescriptorForCDC(base, false)
		require.NoError(t, err)
		require.Len(t, wrapped.Field, 2)
		assert.Equal(t, "_CHANGE_TYPE", wrapped.Field[1].GetName())
		assert.Equal(t, int32(2), wrapped.Field[1].GetNumber())
		assert.Equal(t, descriptorpb.FieldDescriptorProto_TYPE_STRING, wrapped.Field[1].GetType())
	})

	t.Run("appends both pseudo-columns when sequence configured", func(t *testing.T) {
		wrapped, err := wrapDescriptorForCDC(base, true)
		require.NoError(t, err)
		require.Len(t, wrapped.Field, 3)
		assert.Equal(t, "_CHANGE_TYPE", wrapped.Field[1].GetName())
		assert.Equal(t, int32(2), wrapped.Field[1].GetNumber())
		assert.Equal(t, "_CHANGE_SEQUENCE_NUMBER", wrapped.Field[2].GetName())
		assert.Equal(t, int32(3), wrapped.Field[2].GetNumber())
	})

	t.Run("preserves base descriptor", func(t *testing.T) {
		_, err := wrapDescriptorForCDC(base, true)
		require.NoError(t, err)
		require.Len(t, base.Field, 1)
		assert.Equal(t, "id", base.Field[0].GetName())
	})

	t.Run("picks next field number above max", func(t *testing.T) {
		withGap := &descriptorpb.DescriptorProto{
			Name: new("WithGap"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: new("a"), Number: new(int32(1)), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: new("b"), Number: new(int32(7)), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			},
		}
		wrapped, err := wrapDescriptorForCDC(withGap, true)
		require.NoError(t, err)
		assert.Equal(t, int32(8), wrapped.Field[2].GetNumber())
		assert.Equal(t, int32(9), wrapped.Field[3].GetNumber())
	})
}

func TestCDCInjectorValidateAndResolve(t *testing.T) {
	t.Run("returns normalised change_type and sequence", func(t *testing.T) {
		inj := &cdcInjector{
			allowDelete: true,
			changeSeq:   &service.InterpolatedString{},
		}
		ct, seq, err := inj.validateAndResolveCDC("upsert", "0/0")
		require.NoError(t, err)
		assert.Equal(t, "UPSERT", ct)
		assert.Equal(t, "0/0", seq)
	})

	t.Run("rejects bad change_type", func(t *testing.T) {
		inj := &cdcInjector{allowDelete: true}
		_, _, err := inj.validateAndResolveCDC("MAYBE", "")
		require.Error(t, err)
	})

	t.Run("rejects bad sequence number", func(t *testing.T) {
		inj := &cdcInjector{allowDelete: true, changeSeq: &service.InterpolatedString{}}
		_, _, err := inj.validateAndResolveCDC("UPSERT", "not-hex")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "change_sequence_number")
	})

	t.Run("returns empty seq when not configured", func(t *testing.T) {
		inj := &cdcInjector{allowDelete: true}
		ct, seq, err := inj.validateAndResolveCDC("UPSERT", "")
		require.NoError(t, err)
		assert.Equal(t, "UPSERT", ct)
		assert.Empty(t, seq)
	})
}

func TestValidateCDCPrimaryKeys(t *testing.T) {
	t.Run("config only", func(t *testing.T) {
		require.NoError(t, validateCDCPrimaryKeys([]string{"id"}, nil, "t"))
	})
	t.Run("table only", func(t *testing.T) {
		require.NoError(t, validateCDCPrimaryKeys(nil, []string{"id"}, "t"))
	})
	t.Run("matching config and table", func(t *testing.T) {
		require.NoError(t, validateCDCPrimaryKeys([]string{"tenant_id", "id"}, []string{"tenant_id", "id"}, "t"))
	})
	t.Run("mismatched", func(t *testing.T) {
		err := validateCDCPrimaryKeys([]string{"id"}, []string{"tenant_id"}, "t")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not match")
	})
	t.Run("mismatched order", func(t *testing.T) {
		err := validateCDCPrimaryKeys([]string{"a", "b"}, []string{"b", "a"}, "t")
		require.Error(t, err)
	})
	t.Run("neither set", func(t *testing.T) {
		err := validateCDCPrimaryKeys(nil, nil, "events")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "PRIMARY KEY")
		assert.Contains(t, err.Error(), "events")
	})
}

func TestInjectCDCJSON(t *testing.T) {
	t.Run("appends pseudo-columns to non-empty object", func(t *testing.T) {
		out, err := injectCDCJSON([]byte(`{"id":"x","age":"30"}`), "UPSERT", "0/1")
		require.NoError(t, err)
		var got map[string]any
		require.NoError(t, json.Unmarshal(out, &got))
		assert.Equal(t, "x", got["id"])
		assert.Equal(t, "30", got["age"]) // string preserved, not coerced to number
		assert.Equal(t, "UPSERT", got["_CHANGE_TYPE"])
		assert.Equal(t, "0/1", got["_CHANGE_SEQUENCE_NUMBER"])
	})

	t.Run("omits sequence when empty", func(t *testing.T) {
		out, err := injectCDCJSON([]byte(`{"id":"x"}`), "DELETE", "")
		require.NoError(t, err)
		assert.Contains(t, string(out), `"_CHANGE_TYPE":"DELETE"`)
		assert.NotContains(t, string(out), `_CHANGE_SEQUENCE_NUMBER`)
	})

	t.Run("preserves int-as-string fidelity", func(t *testing.T) {
		// 2^53 + 1 — would lose precision through float64 round-trip.
		out, err := injectCDCJSON([]byte(`{"id":"9007199254740993"}`), "UPSERT", "")
		require.NoError(t, err)
		assert.Contains(t, string(out), `"9007199254740993"`)
	})

	t.Run("rejects non-object payload", func(t *testing.T) {
		_, err := injectCDCJSON([]byte(`[1,2,3]`), "UPSERT", "")
		require.Error(t, err)
	})

	t.Run("rejects malformed JSON", func(t *testing.T) {
		_, err := injectCDCJSON([]byte(`{not json`), "UPSERT", "")
		require.Error(t, err)
	})

	t.Run("rejects JSON null", func(t *testing.T) {
		_, err := injectCDCJSON([]byte(`null`), "UPSERT", "")
		require.Error(t, err)
	})

	t.Run("rejects user payload already containing _CHANGE_TYPE", func(t *testing.T) {
		_, err := injectCDCJSON([]byte(`{"id":"x","_CHANGE_TYPE":"DELETE"}`), "UPSERT", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "_CHANGE_TYPE")
	})

	t.Run("rejects user payload already containing _CHANGE_SEQUENCE_NUMBER", func(t *testing.T) {
		_, err := injectCDCJSON([]byte(`{"id":"x","_CHANGE_SEQUENCE_NUMBER":"FF"}`), "UPSERT", "0/1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "_CHANGE_SEQUENCE_NUMBER")
	})

	t.Run("ignores _CHANGE_SEQUENCE_NUMBER in payload when sequence not configured", func(t *testing.T) {
		// changeSeq="" means user didn't configure change_sequence_number,
		// so a pre-existing field is not a collision the injector cares about.
		// (The descriptor was wrapped without _CHANGE_SEQUENCE_NUMBER, so the
		// AppendRows call will reject it server-side as an unknown field —
		// that's the right error surface, not a client-side rewrite.)
		out, err := injectCDCJSON([]byte(`{"id":"x","_CHANGE_SEQUENCE_NUMBER":"FF"}`), "UPSERT", "")
		require.NoError(t, err)
		assert.Contains(t, string(out), `"_CHANGE_TYPE":"UPSERT"`)
	})
}

func TestErrPermanentPropagatesThroughClassify(t *testing.T) {
	// Sanity check: classifyBQError must recognize errPermanent-wrapped errors
	// as permanent so CDC config failures route the batch to DLQ instead of
	// being retried forever.
	wrapped := fmt.Errorf("%w: %w", errPermanent, errors.New("simulated CDC config failure"))
	assert.True(t, classifyBQError(wrapped).IsPermanent())
}
