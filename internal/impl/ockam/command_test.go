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

package ockam

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandNameOmitsArguments(t *testing.T) {
	assert.Equal(t, "ockam node create",
		commandName("ockam", []string{"node", "create", "--node-config", `{"ticket":"S3cretTicket"}`}))
	assert.Equal(t, "ockam node delete",
		commandName("ockam", []string{"node", "delete", "n1", "--yes"}))
	assert.Equal(t, "/bin/ockam identity",
		commandName("/bin/ockam", []string{"identity", "--output", "json"}))
	assert.Equal(t, "ockam", commandName("ockam", nil))
}
