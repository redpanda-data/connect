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

package crypto

import "fmt"

// valueAsString preserves V1's StringMethod behaviour, which coerced []byte to
// string. V2's StringMethod is strict and would reject bytes receivers; the
// V1 crypto plugins relied on the V1 lenient form. Shared here across the V2
// ports of the connect crypto plugin set.
func valueAsString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	}
	return "", fmt.Errorf("expected string or bytes, got %T", v)
}
