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

package plugins

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInfoCSV(t *testing.T) {
	// This test parses the base csv and checks for any malformed fields.
	for k, v := range BaseInfo {
		assert.NotEmpty(t, v.Type, "plugin %v type field", k)
		assert.NotEmpty(t, v.Support, "plugin %v support field", k)
	}
}
