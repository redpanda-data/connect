// Copyright 2024 Redpanda Data, Inc.
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

package jirahttp

import (
	"fmt"
	"net/http"
)

// HTTPError wraps non-2xx responses with useful context.
type HTTPError struct {
	StatusCode int
	Reason     string
	Body       string
	Headers    http.Header
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http error: status=%d reason=%s", e.StatusCode, e.Reason)
}

// AuthHeaderPolicy allows callers to declare a header that signals an auth problem
// even on 200 OK responses (e.g., "X-Seraph-LoginReason").
type AuthHeaderPolicy struct {
	HeaderName string                // case-insensitive
	IsProblem  func(val string) bool // return true if the header value indicates auth failure
}
