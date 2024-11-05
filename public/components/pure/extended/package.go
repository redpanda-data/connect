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

// Package extended contains component implementations that have a larger
// dependency footprint but do not interact with external systems (so an
// extension of pure components)
//
// EXPERIMENTAL: The specific components excluded by this package may change
// outside of major version releases. This means we may choose to remove certain
// plugins if we determine that their dependencies are likely to interfere with
// the goals of this package.
package extended

import (
	// Import pure but larger packages.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure/extended"

	_ "github.com/redpanda-data/connect/v4/internal/impl/awk"
	_ "github.com/redpanda-data/connect/v4/internal/impl/benchmark"
	_ "github.com/redpanda-data/connect/v4/internal/impl/html"
	_ "github.com/redpanda-data/connect/v4/internal/impl/jsonpath"
	_ "github.com/redpanda-data/connect/v4/internal/impl/lang"
	_ "github.com/redpanda-data/connect/v4/internal/impl/msgpack"
	_ "github.com/redpanda-data/connect/v4/internal/impl/parquet"
	_ "github.com/redpanda-data/connect/v4/internal/impl/protobuf"
	_ "github.com/redpanda-data/connect/v4/internal/impl/xml"
)
