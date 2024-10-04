// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import "runtime"

func GetAvailableMemory() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	// You can use memStats.Sys or another appropriate memory metric.
	// Consider leaving some memory unused for other processes.
	availableMemory := memStats.Sys - memStats.HeapInuse
	return availableMemory
}
