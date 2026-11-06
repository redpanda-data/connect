// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildablePackages(t *testing.T) {
	listOut := "errors\n" +
		"github.com/stretchr/testify/assert\n" +
		"\n" +
		"github.com/redpanda-data/connect/v4/internal/impl/nats\n" +
		"github.com/redpanda-data/connect/v4/internal/impl/nats.test\n"
	want := []string{
		"errors",
		"github.com/stretchr/testify/assert",
		"github.com/redpanda-data/connect/v4/internal/impl/nats",
	}
	assert.Equal(t, want, buildablePackages(listOut))
}
