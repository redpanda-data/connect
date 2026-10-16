// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlp_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func newTestHTTPOutput(t *testing.T, yaml string) (service.BatchOutput, error) {
	t.Helper()
	pConf, err := otlp.HTTPOutputSpec().ParseYAML(yaml, nil)
	require.NoError(t, err)
	res := service.MockResources()
	license.InjectTestService(res)
	return otlp.HTTPOutputFromParsed(pConf, res)
}

func TestHTTPOutputEndpointRedacted(t *testing.T) {
	_, err := newTestHTTPOutput(t, `endpoint: "http://u:S3cretPass@bad host:4318"`)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}

func TestHTTPOutputProxyURLRedacted(t *testing.T) {
	out, err := newTestHTTPOutput(t, `
endpoint: "http://localhost:4318"
proxy_url: "http://u:S3cretPass@bad host:8080"
`)
	require.NoError(t, err)

	err = out.Connect(context.Background())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}
