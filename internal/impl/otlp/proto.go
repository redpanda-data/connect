// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package otlp

//go:generate ./proto/gen.sh
import _ "embed"

//go:embed proto/common.proto
var commonProtoSchema string

//go:embed proto/trace.proto
var spanProtoSchema string

//go:embed proto/log.proto
var logRecordProtoSchema string

//go:embed proto/metric.proto
var metricProtoSchema string
