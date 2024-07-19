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

// Package free imports all free, open source plugin implementations that ship
// with Redpanda Connect. This is a convenient way of importing every single
// free connector at the cost of a larger dependency tree for your application.
package free

import (
	// Import all public sub-categories.
	_ "github.com/redpanda-data/connect/v4/public/components/community"
)
