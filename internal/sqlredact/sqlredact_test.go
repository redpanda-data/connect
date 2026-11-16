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

package sqlredact

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// echoDriver fails to open connections with an error that echoes the DSN.
type echoDriver struct{}

func (echoDriver) Open(dsn string) (driver.Conn, error) {
	return nil, fmt.Errorf("cannot connect to %s", dsn)
}

// echoContextDriver echoes the DSN when opening a connector.
type echoContextDriver struct{ echoDriver }

func (echoContextDriver) OpenConnector(dsn string) (driver.Connector, error) {
	return nil, fmt.Errorf("invalid dsn %q", dsn)
}

// lazyContextDriver opens a connector that fails to connect lazily.
type lazyContextDriver struct{ echoDriver }

type lazyConnector struct {
	dsn    string
	closed *bool
}

func (c lazyConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, fmt.Errorf("dial %s: refused", c.dsn)
}

func (lazyConnector) Driver() driver.Driver { return lazyContextDriver{} }

func (c lazyConnector) Close() error {
	*c.closed = true
	return nil
}

var lazyClosed bool

func (lazyContextDriver) OpenConnector(dsn string) (driver.Connector, error) {
	return lazyConnector{dsn: dsn, closed: &lazyClosed}, nil
}

const secret = "S3cretPass"

func init() {
	sql.Register("sqlredact_echo", echoDriver{})
	sql.Register("sqlredact_echo_context", echoContextDriver{})
	sql.Register("sqlredact_lazy_context", lazyContextDriver{})
}

func TestOpen(t *testing.T) {
	dsn := "postgres://u:" + secret + "@host/db"

	db, err := Open("sqlredact_echo", dsn)
	require.NoError(t, err)
	err = db.PingContext(t.Context())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret)
	assert.Contains(t, err.Error(), "postgres://u:xxxxx@host/db")
	require.NoError(t, db.Close())

	_, err = Open("sqlredact_echo_context", dsn)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret)

	db, err = Open("sqlredact_lazy_context", dsn)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "SELECT 1")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret)
	require.NoError(t, db.Close())
	assert.True(t, lazyClosed, "closing the DB must close the connector")

	_, err = Open("sqlredact_missing", dsn)
	require.ErrorContains(t, err, "unknown driver")
}

// The fake drivers must echo the DSN for the tests above to mean anything.
func TestEchoDriversLeakWithoutOpen(t *testing.T) {
	dsn := "postgres://u:" + secret + "@host/db"

	db, err := sql.Open("sqlredact_echo", dsn)
	require.NoError(t, err)
	require.ErrorContains(t, db.PingContext(t.Context()), secret)
	require.NoError(t, db.Close())

	_, err = sql.Open("sqlredact_echo_context", dsn)
	require.ErrorContains(t, err, secret)
}
