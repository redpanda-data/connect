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

// Package sqlredact opens SQL databases so that credentials in the DSN are
// redacted from driver errors.
package sqlredact

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"

	"github.com/redpanda-data/benthos/v4/public/utils/redact"
)

// Open is sql.Open, except that credentials in dsn are redacted from errors.
// Drivers often parse the DSN lazily, so the connector is wrapped.
func Open(driverName, dsn string) (*sql.DB, error) {
	// sql.Open only looks up the driver. Its DB is closed after the connector
	// is opened, so that drivers that cache connectors, such as spanner, reuse it.
	probe, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, redact.Error(err, redact.Conns(dsn))
	}
	defer probe.Close()
	drv := probe.Driver()

	var conn driver.Connector = dsnConnector{dsn: dsn, drv: drv}
	if dc, ok := drv.(driver.DriverContext); ok {
		if conn, err = dc.OpenConnector(dsn); err != nil {
			return nil, redact.Error(err, redact.Conns(dsn))
		}
	}
	return sql.OpenDB(redactedConnector{Connector: conn, dsn: dsn}), nil
}

// dsnConnector mirrors the connector that database/sql uses for drivers that
// do not implement driver.DriverContext.
type dsnConnector struct {
	dsn string
	drv driver.Driver
}

func (c dsnConnector) Connect(context.Context) (driver.Conn, error) {
	return c.drv.Open(c.dsn)
}

func (c dsnConnector) Driver() driver.Driver {
	return c.drv
}

type redactedConnector struct {
	driver.Connector
	dsn string
}

func (c redactedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, redact.Error(err, redact.Conns(c.dsn))
	}
	return conn, nil
}

// Close is called by (*sql.DB).Close, and must be forwarded so that
// connectors that hold resources release them.
func (c redactedConnector) Close() error {
	if cl, ok := c.Connector.(io.Closer); ok {
		return cl.Close()
	}
	return nil
}
