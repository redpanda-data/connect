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
	want string
}

func TestBuildS3Endpoint(t *testing.T) {
	t.Run("custom endpoints", func(t *testing.T) {
		tests := []s3EndpointTestCase{
			{
				name: "retains current behaviour",
				info: fileLocationInfo{UseS3RegionalURL: false, Region: "us-east-1", EndPoint: ""},
				want: "",
			},
			{
				name: "supports custom endpoint",
				info: fileLocationInfo{UseS3RegionalURL: false, Region: "us-east-1", EndPoint: "s3.amazonaws.com"},
				want: "https://s3.amazonaws.com",
			},
			{
				name: "custom endpoint has priority over regional endpoint",
				info: fileLocationInfo{UseS3RegionalURL: true, Region: "us-east-1", EndPoint: "s3.amazonaws.com"},
				want: "https://s3.amazonaws.com",
			},
			{
				name: "returns empty if endpoint is not set",
				info: fileLocationInfo{UseS3RegionalURL: false, Region: "us-east-1", EndPoint: ""},
				want: "",
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
				want: "https://s3.us-east-1.amazonaws.com",
			},
			{
				name: "supports cn prefix",
				info: fileLocationInfo{UseS3RegionalURL: true, Region: "cn-north-1"},
				want: "https://s3.cn-north-1.amazonaws.com.cn",
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
