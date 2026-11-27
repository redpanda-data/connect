// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/replication"
)

func signalRecord(op types.OperationType, image map[string]types.AttributeValue) types.Record {
	return types.Record{
		EventName: op,
		Dynamodb:  &types.StreamRecord{NewImage: image},
	}
}

func strAttr(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

func TestDecodeSignalRecord(t *testing.T) {
	snapData := `{"tables":["a"]}`
	tests := []struct {
		name         string
		rec          types.Record
		wantSignal   bool
		wantErr      bool
		wantRejected bool
		wantID       string
		wantType     string
		wantMessage  string
		wantData     string
	}{
		{
			name: "log insert",
			rec: signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
				"id": strAttr("s1"), "type": strAttr("log"), "data": strAttr(`{"message":"hi"}`),
			}),
			wantSignal: true, wantID: "s1", wantType: "log", wantMessage: "hi", wantData: `{"message":"hi"}`,
		},
		{
			name: "snapshot insert",
			rec: signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
				"id": strAttr("s2"), "type": strAttr(replication.SnapshotSignalType), "data": strAttr(snapData),
			}),
			wantSignal: true, wantID: "s2", wantType: replication.SnapshotSignalType, wantData: snapData,
		},
		{
			name: "modify ignored",
			rec: signalRecord(types.OperationTypeModify, map[string]types.AttributeValue{
				"id": strAttr("s3"), "type": strAttr("log"), "data": strAttr("{}"),
			}),
		},
		{
			name: "remove ignored",
			rec:  signalRecord(types.OperationTypeRemove, nil),
		},
		{
			name: "insert without new image ignored",
			rec:  types.Record{EventName: types.OperationTypeInsert, Dynamodb: &types.StreamRecord{}},
		},
		{
			name: "missing type",
			rec: signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
				"id": strAttr("s4"), "data": strAttr("{}"),
			}),
			wantErr: true, wantRejected: true,
		},
		{
			name: "type as number",
			rec: signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
				"id": strAttr("s7"), "type": &types.AttributeValueMemberN{Value: "1"}, "data": strAttr("{}"),
			}),
			wantErr: true, wantRejected: true,
		},
		{
			name: "data as number",
			rec: signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
				"id": strAttr("s5"), "type": strAttr("log"), "data": &types.AttributeValueMemberN{Value: "1"},
			}),
			wantErr: true, wantRejected: true,
		},
		{
			name: "numeric id",
			rec: signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberN{Value: "42"}, "type": strAttr("log"), "data": strAttr(`{"message":"m"}`),
			}),
			wantSignal: true, wantID: "42", wantType: "log", wantMessage: "m", wantData: `{"message":"m"}`,
		},
		{
			name: "missing id",
			rec: signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
				"type": strAttr("log"), "data": strAttr(`{"message":"m"}`),
			}),
			wantSignal: true, wantID: "", wantType: "log", wantMessage: "m", wantData: `{"message":"m"}`,
		},
		{
			name: "log with bad json",
			rec: signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
				"id": strAttr("s6"), "type": strAttr("log"), "data": strAttr("{nope"),
			}),
			wantSignal: true, wantErr: true, wantRejected: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sig, data, isSignal, err := decodeSignalRecord(tc.rec)
			if tc.wantErr {
				require.Error(t, err)
				// Callers check err before isSignal, so a rejected record
				// still reports isSignal true.
				assert.True(t, isSignal, "a rejected record is still a signal")
				assert.Equal(t, tc.wantRejected, errors.Is(err, replication.ErrSignalRejected))
				assert.Nil(t, sig)
				assert.Nil(t, data)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantSignal, isSignal)
			if !tc.wantSignal {
				assert.Nil(t, sig)
				assert.Nil(t, data)
				return
			}
			require.NotNil(t, sig)
			assert.Equal(t, tc.wantID, sig.ID)
			assert.Equal(t, tc.wantType, sig.SignalType)
			assert.Equal(t, tc.wantMessage, sig.Message)
			assert.Equal(t, tc.wantData, string(data))
		})
	}
}
