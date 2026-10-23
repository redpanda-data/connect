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

package sentry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestCaptureProcessorDSNErrorRedacted(t *testing.T) {
	conf, err := newCaptureProcessorConfig().ParseYAML(`
dsn: "https://pub:S3cretPass@bad host/42"
message: hello
`, service.GlobalEnvironment())
	require.NoError(t, err)

	_, err = newCaptureProcessor(conf, service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating sentry client")
	assert.NotContains(t, err.Error(), "S3cretPass")
}
