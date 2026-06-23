// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// smtCarrier builds a Convert() input using an S3 sink connector as the carrier
// for a single SMT identified by alias "t".
func smtCarrier(extra string) []byte {
	return []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "topics":"orders",
	  "transforms":"t",
	  ` + extra + `
	}}`)
}

func TestSMTExtractField(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ExtractField$Value",
	  "transforms.t.field":"name"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "root = this.name")
}

func TestSMTHoistField(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.HoistField$Value",
	  "transforms.t.field":"data"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), `root = {"data": this}`)
}

func TestSMTMaskField(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.MaskField$Value",
	  "transforms.t.fields":"ssn,ccn"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `root.ssn = ""`)
	assert.Contains(t, y, `root.ccn = ""`)
	// TODO comment must appear in the rendered YAML.
	assert.Contains(t, y, "TODO: MaskField sets masked fields to an empty string; numeric/boolean fields will be stringified")
	// A warning must be present for the MaskField alias.
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "numeric/boolean fields will be stringified") {
			warned = true
		}
	}
	assert.True(t, warned, "expected MaskField numeric/boolean stringify warning")
}

func TestSMTMaskFieldReplacement(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.MaskField$Value",
	  "transforms.t.fields":"secret",
	  "transforms.t.replacement":"REDACTED"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), `root.secret = "REDACTED"`)
}

func TestSMTCast(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Cast$Value",
	  "transforms.t.spec":"age:int32,score:float64"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "this.age.number()")
	assert.Contains(t, y, "this.score.number()")
}

func TestSMTCastString(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Cast$Value",
	  "transforms.t.spec":"name:string,active:boolean"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, "this.name.string()")
	assert.Contains(t, y, "this.active.bool()")
}

func TestSMTTimestampConverterUnix(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.TimestampConverter$Value",
	  "transforms.t.field":"ts",
	  "transforms.t.target.type":"unix"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "this.ts.ts_unix()")
}

func TestSMTValueToKey(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ValueToKey",
	  "transforms.t.fields":"id"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "meta key = this.id.string()")
}

func TestSMTValueToKeyMultiField(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ValueToKey",
	  "transforms.t.fields":"id,region"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), "meta key = this.id.string()")
	var warned bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "multiple") {
			warned = true
		}
	}
	assert.True(t, warned, "expected multi-field ValueToKey warning")
}

func TestSMTKeyVariantWarns(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ExtractField$Key",
	  "transforms.t.field":"name"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	var found bool
	for _, w := range res.Warnings {
		if w.Field == "t" && strings.Contains(w.Message, "KEY") {
			found = true
		}
	}
	assert.True(t, found, "expected a $Key variant warning mentioning KEY")
}
