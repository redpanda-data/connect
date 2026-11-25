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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// parseTTL converts a configured TTL into the server's expiration encoding.
//
// Sentinels and Go durations (24h, 90s) are accepted. S/M/H/D units are
// case-insensitive and a bare number is seconds, so 24H, 1D and 3600 parse
// the same way.
func parseTTL(s string) (uint32, error) {
	s = strings.TrimSpace(s)
	lower := strings.ToLower(s)
	switch lower {
	case "", "0", "0s", "default":
		return as.TTLServerDefault, nil
	case "never", "-1":
		return as.TTLDontExpire, nil
	case "keep", "-2":
		return as.TTLDontUpdate, nil
	}

	secs, err := parseTTLSeconds(lower)
	if err != nil {
		return 0, fmt.Errorf("invalid ttl %q: expected a duration, an S/M/H/D unit, bare seconds, 'never' or 'keep': %w", s, err)
	}
	if secs < 0 {
		return 0, fmt.Errorf("invalid ttl %q: must not be negative", s)
	}
	if secs >= math.MaxUint32-1 {
		return 0, fmt.Errorf("invalid ttl %q: exceeds the maximum expiration", s)
	}
	return uint32(secs), nil
}

func parseTTLSeconds(lower string) (int64, error) {
	if n, err := strconv.ParseInt(lower, 10, 64); err == nil {
		return n, nil
	}
	if daysStr, ok := strings.CutSuffix(lower, "d"); ok {
		days, err := strconv.ParseInt(daysStr, 10, 64)
		if err != nil {
			return 0, errors.New("invalid day duration")
		}
		if days > math.MaxInt64/86400 {
			return 0, errors.New("exceeds the maximum expiration")
		}
		if days < math.MinInt64/86400 {
			return 0, errors.New("must not be negative")
		}
		return days * 86400, nil
	}
	d, err := time.ParseDuration(lower)
	if err != nil {
		return 0, err
	}
	secs := int64(d / time.Second)
	if d < 0 {
		// Leave the negative check to parseTTL so -1m and -1d share one message.
		// A negative sub-second duration truncates to 0, which would otherwise
		// look like "use the namespace default".
		if secs == 0 {
			return -1, nil
		}
		return secs, nil
	}
	if d > 0 && secs == 0 {
		// A positive sub-second TTL would round to "use namespace default",
		// which is the opposite of what was asked for. A zero duration such as
		// 0m or 0H is the namespace default and must pass through.
		return 0, errors.New("the minimum resolution is one second")
	}
	return secs, nil
}
