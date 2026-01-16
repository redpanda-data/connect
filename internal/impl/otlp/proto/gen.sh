#!/usr/bin/env bash
# Copyright 2026 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TAG="redpanda-otel-exporter/$(go list -m -f '{{.Version}}' github.com/redpanda-data/common-go/redpanda-otel-exporter)"
curl -fsSL "https://raw.githubusercontent.com/redpanda-data/common-go/${TAG}/redpanda-otel-exporter/proto/common.proto" -o "${SCRIPT_DIR}/common.proto"
curl -fsSL "https://raw.githubusercontent.com/redpanda-data/common-go/${TAG}/redpanda-otel-exporter/proto/trace.proto" -o "${SCRIPT_DIR}/trace.proto"
curl -fsSL "https://raw.githubusercontent.com/redpanda-data/common-go/${TAG}/redpanda-otel-exporter/proto/log.proto" -o "${SCRIPT_DIR}/log.proto"
curl -fsSL "https://raw.githubusercontent.com/redpanda-data/common-go/${TAG}/redpanda-otel-exporter/proto/metric.proto" -o "${SCRIPT_DIR}/metric.proto"
