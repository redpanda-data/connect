// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

type ChangeMessage struct {
	IsStreaming bool           `json:"is_streaming"`
	Table       string         `json:"table"`
	Event       string         `json:"event"`
	Data        map[string]any `json:"data"`
}
