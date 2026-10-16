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

package amqp1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestReDialRedactsCredentials(t *testing.T) {
	badURL := "amqp://u:S3cretPass@bad host:5672"

	inConf, err := amqp1InputSpec().ParseYAML(`
urls: [ "`+badURL+`" ]
source_address: /foo
`, nil)
	require.NoError(t, err)
	r, err := amqp1ReaderFromParsed(inConf, service.MockResources())
	require.NoError(t, err)
	_, err = r.reDial(t.Context(), r.urls)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")

	outConf, err := amqp1OutputSpec().ParseYAML(`
urls: [ "`+badURL+`" ]
target_address: /foo
`, nil)
	require.NoError(t, err)
	w, err := amqp1WriterFromParsed(outConf, service.MockResources())
	require.NoError(t, err)
	_, err = w.reDial(t.Context(), w.urls)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}
