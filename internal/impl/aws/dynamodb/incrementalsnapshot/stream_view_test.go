// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequireNewImage(t *testing.T) {
	ok := []dynamodbtypes.StreamViewType{dynamodbtypes.StreamViewTypeNewImage, dynamodbtypes.StreamViewTypeNewAndOldImages}
	bad := []dynamodbtypes.StreamViewType{dynamodbtypes.StreamViewTypeKeysOnly, dynamodbtypes.StreamViewTypeOldImage}
	for _, v := range ok {
		assert.NoError(t, RequireNewImage("t", &dynamodbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: v}))
	}
	for _, v := range bad {
		assert.ErrorContains(t, RequireNewImage("t", &dynamodbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: v}), string(v))
	}
	assert.Error(t, RequireNewImage("t", nil))
	assert.Error(t, RequireNewImage("t", &dynamodbtypes.StreamSpecification{StreamEnabled: aws.Bool(false), StreamViewType: dynamodbtypes.StreamViewTypeNewImage}))
}

// TestRequireSignalStreamView: the signal table error names the signal table
// rather than reusing the incremental snapshot wording.
func TestRequireSignalStreamView(t *testing.T) {
	for _, v := range []dynamodbtypes.StreamViewType{dynamodbtypes.StreamViewTypeNewImage, dynamodbtypes.StreamViewTypeNewAndOldImages} {
		assert.NoError(t, RequireSignalStreamView("sig", &dynamodbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: v}))
	}
	err := RequireSignalStreamView("sig", &dynamodbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: dynamodbtypes.StreamViewTypeKeysOnly})
	require.EqualError(t, err, "signal_table_name: table sig needs a stream with view NEW_IMAGE or NEW_AND_OLD_IMAGES, got KEYS_ONLY")
	err = RequireSignalStreamView("sig", nil)
	require.EqualError(t, err, "signal_table_name: table sig needs a stream with view NEW_IMAGE or NEW_AND_OLD_IMAGES, got no stream enabled")
}
