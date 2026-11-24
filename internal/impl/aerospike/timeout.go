// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"context"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// limitDuration returns the shorter of the configured timeout and the time
// remaining on ctx. The Aerospike client takes no context, so a command's
// TotalTimeout is the only way to honour a pipeline deadline.
func limitDuration(ctx context.Context, configured time.Duration) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return configured, nil
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return 0, context.DeadlineExceeded
	}
	if configured > 0 && configured < remaining {
		return configured, nil
	}
	return remaining, nil
}

// batchPolicyForContext copies p and caps TotalTimeout (and SocketTimeout)
// so a BatchOperate cannot outlive ctx. The original policy is left unchanged
// so concurrent batches do not race.
func batchPolicyForContext(ctx context.Context, p *as.BatchPolicy) (*as.BatchPolicy, error) {
	if p == nil {
		p = as.NewBatchPolicy()
	}
	cp := *p
	total, err := limitDuration(ctx, cp.TotalTimeout)
	if err != nil {
		return nil, err
	}
	cp.TotalTimeout = total
	// total is 0 when the total timeout is configured as unlimited and the
	// pipeline has no deadline. Clamping the socket timeout to it there would
	// turn "no overall limit" into "no limit of any kind".
	if total > 0 && (cp.SocketTimeout <= 0 || cp.SocketTimeout > total) {
		cp.SocketTimeout = total
	}
	return &cp, nil
}
