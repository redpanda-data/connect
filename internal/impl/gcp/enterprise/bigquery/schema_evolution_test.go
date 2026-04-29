// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestProtoKindToBQFieldType(t *testing.T) {
	tests := []struct {
		name     string
		kind     protoreflect.Kind
		expected bigquery.FieldType
	}{
		{"string", protoreflect.StringKind, bigquery.StringFieldType},
		{"int64", protoreflect.Int64Kind, bigquery.IntegerFieldType},
		{"int32", protoreflect.Int32Kind, bigquery.IntegerFieldType},
		{"sint64", protoreflect.Sint64Kind, bigquery.IntegerFieldType},
		{"sint32", protoreflect.Sint32Kind, bigquery.IntegerFieldType},
		{"sfixed64", protoreflect.Sfixed64Kind, bigquery.IntegerFieldType},
		{"sfixed32", protoreflect.Sfixed32Kind, bigquery.IntegerFieldType},
		{"uint64", protoreflect.Uint64Kind, bigquery.IntegerFieldType},
		{"uint32", protoreflect.Uint32Kind, bigquery.IntegerFieldType},
		{"fixed64", protoreflect.Fixed64Kind, bigquery.IntegerFieldType},
		{"fixed32", protoreflect.Fixed32Kind, bigquery.IntegerFieldType},
		{"double", protoreflect.DoubleKind, bigquery.FloatFieldType},
		{"float", protoreflect.FloatKind, bigquery.FloatFieldType},
		{"bool", protoreflect.BoolKind, bigquery.BooleanFieldType},
		{"bytes", protoreflect.BytesKind, bigquery.BytesFieldType},
		{"enum", protoreflect.EnumKind, bigquery.StringFieldType},
		{"message", protoreflect.MessageKind, bigquery.RecordFieldType},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, protoKindToBQFieldType(tc.kind))
		})
	}
}

func TestDescriptorToBQSchema(t *testing.T) {
	dp := &descriptorpb.DescriptorProto{
		Name: new("TestMessage"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   new("name"),
				Number: new(int32(1)),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			},
			{
				Name:   new("age"),
				Number: new(int32(2)),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			},
		},
	}

	md, err := descriptorProtoToMessageDescriptor(dp)
	require.NoError(t, err)

	schema := descriptorToBQSchema(md)
	require.Len(t, schema, 2)

	assert.Equal(t, "name", schema[0].Name)
	assert.Equal(t, bigquery.StringFieldType, schema[0].Type)
	assert.False(t, schema[0].Required)

	assert.Equal(t, "age", schema[1].Name)
	assert.Equal(t, bigquery.IntegerFieldType, schema[1].Type)
	assert.False(t, schema[1].Required)
}

func TestDiffMissingColumns(t *testing.T) {
	dp := &descriptorpb.DescriptorProto{
		Name: new("TestMessage"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: new("name"), Number: new(int32(1)), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: new("age"), Number: new(int32(2)), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: new("email"), Number: new(int32(3)), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
		},
	}
	md, err := descriptorProtoToMessageDescriptor(dp)
	require.NoError(t, err)

	existing := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
	}

	missing := diffMissingColumns(md, existing)
	require.Len(t, missing, 1)
	assert.Equal(t, "email", missing[0].Name)
	assert.Equal(t, bigquery.StringFieldType, missing[0].Type)
	assert.False(t, missing[0].Required)
}

func TestDiffMissingColumnsNoMissing(t *testing.T) {
	dp := &descriptorpb.DescriptorProto{
		Name: new("TestMessage"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: new("name"), Number: new(int32(1)), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
		},
	}
	md, err := descriptorProtoToMessageDescriptor(dp)
	require.NoError(t, err)

	existing := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
	}

	missing := diffMissingColumns(md, existing)
	assert.Empty(t, missing)
}
