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

// Package tzdata embeds the IANA Time Zone database into the binary.
//
// Components that resolve named locations with time.LoadLocation (for example
// the jira input, which renders JQL date predicates in the account's timezone)
// otherwise depend on a system tz database being present at runtime. The
// release container images are busybox-based and do not ship
// /usr/share/zoneinfo, so without an embedded copy LoadLocation fails for any
// non-UTC zone and callers silently fall back to UTC — reintroducing the very
// data-loss windows those features close.
//
// Blank-importing time/tzdata embeds the database (~450 KiB) and registers it
// as a fallback used only when no system database is found, so hosts that
// already ship tzdata are unaffected. Distribution entrypoints blank-import
// this package so the guarantee travels with every shipped binary rather than
// depending on the runtime image.
package tzdata

import _ "time/tzdata"
