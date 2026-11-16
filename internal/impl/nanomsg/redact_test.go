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

package nanomsg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestConstructorsRedactCredentials(t *testing.T) {
	const yaml = `urls: [ "tcp://u:S3cretPass@bad host:5555" ]`

	inConf, err := inputConfigSpec().ParseYAML(yaml, nil)
	require.NoError(t, err)
	_, err = newNanomsgReaderFromParsed(inConf, service.MockResources())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")

	outConf, err := outputConfigSpec().ParseYAML(yaml, nil)
	require.NoError(t, err)
	_, err = newNanomsgWriterFromParsed(outConf, service.MockResources())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}
