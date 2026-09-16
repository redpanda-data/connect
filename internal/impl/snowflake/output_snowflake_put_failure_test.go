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

package snowflake

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// failingDB stands in for a gosnowflake connection whose PUT fails. In v2 the
// driver surfaces a failed upload as an error from ExecContext (its
// snowflakeFileTransferAgent.result raises ErrFailedToUploadToStage whenever a
// file carries errorDetails, with no opt-in flag), so this is the shape the
// writer sees when a stage upload does not succeed.
type failingDB struct {
	err   error
	calls int
}

func (db *failingDB) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	db.calls++
	return nil, db.err
}

func (*failingDB) Close() error { return nil }

// TestSnowflakePutFailedUploadReturnsError pins the contract that made the v1
// RaisePutGetError opt-in necessary: a failed PUT must reach WriteBatch as an
// error so the batch is nacked instead of silently acknowledged
// (gosnowflake#701).
func TestSnowflakePutFailedUploadReturnsError(t *testing.T) {
	conf, err := snowflakePutOutputConfig().ParseYAML(`
account: benthos
region: east-us-2
cloud: azure
user: foobar
private_key_file: resources/ssh_keys/snowflake_rsa_key.pem
role: test_role
database: test_db
warehouse: test_warehouse
schema: test_schema
path: foo/bar/baz
stage: '@test_stage'
compression: NONE
`, service.NewEnvironment())
	require.NoError(t, err)

	w, err := newSnowflakeWriterFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	putErr := &gosnowflake.SnowflakeError{
		Number:  gosnowflake.ErrFailedToUploadToStage,
		Message: "open /tmp/data.json: permission denied",
	}
	db := &failingDB{err: putErr}
	w.db = db
	w.uuidGenerator = MockUUIDGenerator{}

	err = w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo"}`)),
	})
	require.ErrorContains(t, err, "running query:")
	require.ErrorContains(t, err, "open /tmp/data.json: permission denied")
	assert.Equal(t, 1, db.calls)
}

// TestGosnowflakeHasNoRaisePutGetErrorOptIn guards the assumption behind the
// PUT call in WriteBatch: gosnowflake v2 removed RaisePutGetError and always
// raises PUT/GET failures. If a future driver version reintroduces an opt-in
// for surfacing upload errors, this test fails and the option must be set
// again, otherwise a failed PUT would be acknowledged silently.
func TestGosnowflakeHasNoRaisePutGetErrorOptIn(t *testing.T) {
	typ := reflect.TypeFor[gosnowflake.SnowflakeFileTransferOptions]()
	_, found := typ.FieldByName("RaisePutGetError")
	assert.False(t, found, "gosnowflake reintroduced RaisePutGetError; set it to true in WriteBatch")
}
