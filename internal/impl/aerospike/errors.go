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
	"errors"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// connectionCodes mean the cluster is unreachable rather than that a particular
// command was rejected, so the component should reconnect rather than nack
// individual messages.
var connectionCodes = []types.ResultCode{
	types.NETWORK_ERROR,
	types.SERVER_NOT_AVAILABLE,
	types.INVALID_NODE_ERROR,
	types.NOT_AUTHENTICATED,
	types.INVALID_CREDENTIAL,
}

// retryableResultCodes are results a later attempt can still write, plus OK,
// which is not an error. Listing one acknowledges the message and drops it.
var retryableResultCodes = map[types.ResultCode]struct{}{
	types.OK:                   {},
	types.TIMEOUT:              {},
	types.KEY_BUSY:             {},
	types.DEVICE_OVERLOAD:      {},
	types.XDR_KEY_BUSY:         {},
	types.MAX_RETRIES_EXCEEDED: {},
	types.NO_RESPONSE:          {},
	types.BATCH_FAILED:         {},
	types.NETWORK_ERROR:        {},
	types.SERVER_NOT_AVAILABLE: {},
	types.INVALID_NODE_ERROR:   {},
	types.NOT_AUTHENTICATED:    {},
	types.INVALID_CREDENTIAL:   {},
}

func retryableIgnore(code int) bool {
	_, exists := retryableResultCodes[types.ResultCode(code)]
	return exists
}

// isConnectionError reports whether err is a transport or auth failure that
// should trigger a reconnect rather than a per-message nack.
func isConnectionError(err error) bool {
	var asErr as.Error
	if !errors.As(err, &asErr) {
		return false
	}
	return asErr.Matches(connectionCodes...)
}

// explainResultCode turns a server result code into something a person can act
// on, rather than a bare number they have to look up. Callers add
// operation-specific detail before falling back to this.
func explainResultCode(code types.ResultCode) string {
	switch code {
	case types.KEY_BUSY:
		return "hot key: too many concurrent commands against this one record (see transaction-pending-limit, default 20). " +
			"Reduce concurrency against this key, or split it across multiple records"

	case types.RECORD_TOO_BIG:
		return "the record exceeds the namespace max-record-size (8 MB ceiling). " +
			"Every update rewrites the whole record, so a record this large is also a throughput problem — " +
			"split the data across multiple keys, for example by time bucket"

	case types.BIN_NAME_TOO_LONG:
		return "a bin name exceeds the 15 character server limit"

	case types.FAIL_FORBIDDEN:
		return "operation not allowed at this time. The usual cause is a positive ttl written to a namespace with " +
			"nsup-period 0, which disables expiration entirely — set nsup-period > 0 on the namespace, or set ttl to 0s"

	case types.INVALID_NAMESPACE:
		return "the namespace does not exist on this cluster. Namespaces are declared in aerospike.conf and " +
			"cannot be created at runtime"

	case types.KEY_EXISTS_ERROR:
		return "the record already exists"

	case types.KEY_NOT_FOUND_ERROR:
		return "the record does not exist"

	case types.BIN_TYPE_ERROR:
		return "a bin already holds a value of a different type. Bins are typed per record, so switching a bin " +
			"between, say, integer and double is rejected"

	case types.GENERATION_ERROR:
		return "the record was modified concurrently and the generation check failed"

	case types.DEVICE_OVERLOAD:
		return "the node's storage device cannot keep up with the write rate; this is retryable but indicates " +
			"the cluster is at its write ceiling"

	case types.TIMEOUT:
		return "the command timed out; retryable"

	case types.MAX_RETRIES_EXCEEDED:
		return "the client exhausted its retries; retryable"

	case types.SERVER_NOT_AVAILABLE, types.NETWORK_ERROR, types.INVALID_NODE_ERROR:
		return "the cluster node was unreachable; retryable"

	case types.NO_RESPONSE:
		return "the server returned no result for this key, so the command did not reach it; retryable"

	case types.BATCH_FAILED:
		return "the batch command failed before this key was processed; retryable"

	case types.LOST_CONFLICT:
		return "the write lost a conflict resolution against a concurrent write"

	case types.PARAMETER_ERROR:
		return "the server rejected a parameter of the command"

	case types.NOT_AUTHENTICATED, types.INVALID_CREDENTIAL:
		return "authentication failed; check the credentials field"

	case types.ROLE_VIOLATION:
		return "the authenticated user lacks permission for this operation"
	}

	return "the server rejected the command"
}
