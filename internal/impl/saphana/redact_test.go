// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"testing"

	gohdb "github.com/SAP/go-hdb/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testSecret = "S3cretPass"

func TestBadDSNRedacted(t *testing.T) {
	dsn := "hdb://u:" + testSecret + "/x@h:39017"

	// go-hdb must still echo the DSN for this test to mean anything.
	_, err := gohdb.NewDSNConnector(dsn)
	require.ErrorContains(t, err, testSecret)

	conf, err := sapHANAInputConfigSpec.ParseYAML("dsn: '"+dsn+"'\nmode: bulk\ntable: T\n", nil)
	require.NoError(t, err)
	_, err = newSAPHANAInput(conf, enterpriseResources())
	require.ErrorContains(t, err, "parsing dsn")
	assert.NotContains(t, err.Error(), testSecret)
}

func TestConnectBadDSNParamRedacted(t *testing.T) {
	dsn := "hdb://u:p@h:39017?timeout=" + testSecret

	_, err := gohdb.NewDSNConnector(dsn)
	require.ErrorContains(t, err, testSecret)

	in := &sapHANAInput{conf: sapHANAInputConfig{dsn: dsn}}
	err = in.Connect(t.Context())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), testSecret)
}
