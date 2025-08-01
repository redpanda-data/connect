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
	"testing"

	"github.com/stretchr/testify/require"
)

type s3EndpointTestCase struct {
	name string
	info fileLocationInfo
	want *string
}

func TestBuildS3Endpoint(t *testing.T) {
	t.Run("custom endpoints", func(t *testing.T) {
		tests := []s3EndpointTestCase{
			{
				name: "returns nil if endpoint is empty",
				info: fileLocationInfo{UseS3RegionalURL: false, Region: "us-east-1", EndPoint: ""},
				want: nil,
			},
			{
				name: "supports custom endpoint",
				info: fileLocationInfo{UseS3RegionalURL: false, Region: "us-east-1", EndPoint: "localhost:8080"},
				want: strPtr("https://localhost:8080"),
			},
			{
				name: "supports custom endpoint - prioritised over regional flag",
				info: fileLocationInfo{UseS3RegionalURL: true, Region: "us-east-1", EndPoint: "localhost:8080"},
				want: strPtr("https://localhost:8080"),
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				endpoint := buildS3Endpoint(tt.info)
				require.Equal(t, tt.want, endpoint)
			})
		}
	})

	t.Run("regional endpoints", func(t *testing.T) {
		tests := []s3EndpointTestCase{
			{
				name: "returns regional endpoint",
				info: fileLocationInfo{UseS3RegionalURL: true, Region: "us-east-1"},
				want: strPtr("https://s3.us-east-1.amazonaws.com"),
			},
			{
				name: "supports cn prefix",
				info: fileLocationInfo{UseS3RegionalURL: true, Region: "cn-north-1"},
				want: strPtr("https://s3.cn-north-1.amazonaws.com.cn"),
			},
			{
				name: "empty region returns nil",
				info: fileLocationInfo{UseS3RegionalURL: true, Region: ""},
				want: nil,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				endpoint := buildS3Endpoint(tt.info)
				require.Equal(t, tt.want, endpoint)
			})
		}
	})
}

func strPtr(v string) *string {
	return &v
}
