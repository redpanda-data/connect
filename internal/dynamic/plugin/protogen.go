/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package plugin

//go:generate protoc -I=../../../proto --go_opt=module=github.com/redpanda-data/connect/v4 --go-grpc_opt=module=github.com/redpanda-data/connect/v4 --go_out=../../.. --go-grpc_out=../../.. redpanda/runtime/v1alpha1/message.proto redpanda/runtime/v1alpha1/input.proto redpanda/runtime/v1alpha1/output.proto redpanda/runtime/v1alpha1/processor.proto
