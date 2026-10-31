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
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// longBinNameError is the local rejection of a top-level bin name longer than
// 15 bytes. The record is never sent, so the server does not return
// BIN_NAME_TOO_LONG. ignore_error_codes 21 acknowledges this error because
// that is the result code the same name would produce.
type longBinNameError struct {
	key  *as.Key
	name string
	n    int
}

func (e *longBinNameError) Error() string {
	return fmt.Sprintf("bin name %q is %d bytes, which exceeds the Aerospike limit of %d; rename it in the '%v' mapping", e.name, e.n, maxBinNameLen, aoFieldBins)
}

// classifyRecord converts a per-key batch result into an error, or nil when the
// key was handled successfully.
//
// Two non-OK codes are successes in context:
//
//   - FILTERED_OUT means a fencing expression rejected the write because the
//     record already holds newer data. That is the fence doing its job.
//   - KEY_NOT_FOUND on a plain delete means the record was already gone, which
//     makes deletes idempotent under redelivery. A delete folded from
//     replace_only still fails: that operation required the record.
func classifyRecord(rec *as.BatchRecord, op *pendingOp) error {
	switch rec.ResultCode {
	case types.OK, types.FILTERED_OUT:
		return nil
	case types.KEY_NOT_FOUND_ERROR:
		if op.kind == opDelete && !op.deleteRequiresRecord {
			return nil
		}
	}

	err := fmt.Errorf("aerospike write failed for key %v: %s (code %d: %s)",
		rec.Key, explain(rec.ResultCode, op), rec.ResultCode, types.ResultCodeToString(rec.ResultCode))

	if rec.InDoubt {
		// The client could not confirm the outcome. Surfacing this matters
		// because a retry of a non-idempotent op could double-apply.
		err = fmt.Errorf("%w [in doubt: the write may have been applied]", err)
	}
	return err
}

// explain adds write-specific context to the shared result code explanations,
// where knowing the operation makes the message more useful.
func explain(code types.ResultCode, op *pendingOp) string {
	kind := op.kind
	switch code {
	case types.KEY_EXISTS_ERROR:
		if kind == opCreateOnly {
			return "the record already exists and the operation is create_only"
		}
	case types.KEY_NOT_FOUND_ERROR:
		if kind == opUpdateOnly {
			return "the record does not exist and the operation is update_only"
		}
		if kind == opReplaceOnly || (kind == opDelete && op.deleteRequiresRecord) {
			return "the record does not exist and the operation is replace_only"
		}
	case types.KEY_BUSY:
		return explainResultCode(code) + ". Enabling coalesce_batch_keys collapses repeats of a key within a batch"
	case types.BIN_NAME_TOO_LONG:
		return explainResultCode(code) + fmt.Sprintf("; rename it in the '%v' mapping", aoFieldBins)
	case types.BIN_TYPE_ERROR:
		return explainResultCode(code) + fmt.Sprintf("; check '%v'", aoFieldCoerceIntegralFloats)
	}
	return explainResultCode(code)
}
