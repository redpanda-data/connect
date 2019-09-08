// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/ory/dockertest"
)

func TestSQLPostgresIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "postgres",
		ExposedPorts: []string{"5432/tcp"},
		Env: []string{
			"POSTGRES_USER=testuser",
			"POSTGRES_PASSWORD=testpass",
			"POSTGRES_DB=testdb",
		},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%v/testdb?sslmode=disable", resource.GetPort("5432/tcp"))
	if err = pool.Retry(func() error {
		db, dberr := sql.Open("postgres", dsn)
		if dberr != nil {
			return dberr
		}
		if dberr = db.Ping(); err != nil {
			return dberr
		}
		if _, dberr = db.Exec(`create table footable (
  foo varchar(50) not null,
  bar varchar(50) not null,
  baz varchar(50) not null,
  primary key (foo)
);`); dberr != nil {
			return dberr
		}
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	t.Run("testSQLPostgres", func(t *testing.T) {
		testSQLPostgres(t, dsn)
	})
}

func testSQLPostgres(t *testing.T, dsn string) {
	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "postgres"
	conf.SQL.DSN = dsn
	conf.SQL.Query = "INSERT INTO footable (foo, bar, baz) VALUES ($1, $2, $3);"
	conf.SQL.Args = []string{
		"${!json_field:foo,1}",
		"${!json_field:bar,1}",
		"${!json_field:baz,1}",
	}

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":"bar1","baz":"baz1"}`),
		[]byte(`{"foo":"foo2","bar":"bar2","baz":"baz2"}`),
	}

	resMsgs, response := s.ProcessMessage(message.New(parts))
	if response != nil {
		if response.Error() != nil {
			t.Fatal(response.Error())
		}
		t.Fatal("Expected nil response")
	}
	if len(resMsgs) != 1 {
		t.Fatalf("Wrong resulting msgs: %v != %v", len(resMsgs), 1)
	}
	if act, exp := message.GetAllBytes(resMsgs[0]), parts; !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong result: %s != %s", act, exp)
	}

	conf.SQL.Query = "SELECT * FROM footable WHERE foo = $1;"
	conf.SQL.Args = []string{
		"${!json_field:foo,1}",
	}
	conf.SQL.ResultCodec = "json_array"
	s, err = NewSQL(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	resMsgs, response = s.ProcessMessage(message.New(parts))
	if response != nil {
		if response.Error() != nil {
			t.Fatal(response.Error())
		}
		t.Fatal("Expected nil response")
	}
	if len(resMsgs) != 1 {
		t.Fatalf("Wrong resulting msgs: %v != %v", len(resMsgs), 1)
	}
	expParts := [][]byte{
		[]byte(`[{"bar":"bar2","baz":"baz2","foo":"foo2"}]`),
	}
	if act, exp := message.GetAllBytes(resMsgs[0]), expParts; !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong result: %s != %s", act, exp)
	}
}

func TestSQLMySQLIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "mysql",
		ExposedPorts: []string{"3306/tcp"},
		Env: []string{
			"MYSQL_USER=testuser",
			"MYSQL_PASSWORD=testpass",
			"MYSQL_DATABASE=testdb",
			"MYSQL_RANDOM_ROOT_PASSWORD=yes",
		},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	dsn := fmt.Sprintf("testuser:testpass@tcp(localhost:%v)/testdb", resource.GetPort("3306/tcp"))
	if err = pool.Retry(func() error {
		db, dberr := sql.Open("mysql", dsn)
		if dberr != nil {
			return dberr
		}
		if dberr = db.Ping(); err != nil {
			return dberr
		}
		if _, dberr = db.Exec(`create table footable (
  foo varchar(50) not null,
  bar varchar(50) not null,
  baz varchar(50) not null,
  primary key (foo)
);`); dberr != nil {
			return dberr
		}
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	t.Run("testSQLMySQL", func(t *testing.T) {
		testSQLMySQL(t, dsn)
	})
}

func testSQLMySQL(t *testing.T, dsn string) {
	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "mysql"
	conf.SQL.DSN = dsn
	conf.SQL.Query = "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
	conf.SQL.Args = []string{
		"${!json_field:foo,1}",
		"${!json_field:bar,1}",
		"${!json_field:baz,1}",
	}

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":"bar1","baz":"baz1"}`),
		[]byte(`{"foo":"foo2","bar":"bar2","baz":"baz2"}`),
	}

	resMsgs, response := s.ProcessMessage(message.New(parts))
	if response != nil {
		if response.Error() != nil {
			t.Fatal(response.Error())
		}
		t.Fatal("Expected nil response")
	}
	if len(resMsgs) != 1 {
		t.Fatalf("Wrong resulting msgs: %v != %v", len(resMsgs), 1)
	}
	if act, exp := message.GetAllBytes(resMsgs[0]), parts; !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong result: %s != %s", act, exp)
	}

	conf.SQL.Query = "SELECT * FROM footable WHERE foo = ?;"
	conf.SQL.Args = []string{
		"${!json_field:foo,1}",
	}
	conf.SQL.ResultCodec = "json_array"
	s, err = NewSQL(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	resMsgs, response = s.ProcessMessage(message.New(parts))
	if response != nil {
		if response.Error() != nil {
			t.Fatal(response.Error())
		}
		t.Fatal("Expected nil response")
	}
	if len(resMsgs) != 1 {
		t.Fatalf("Wrong resulting msgs: %v != %v", len(resMsgs), 1)
	}
	expParts := [][]byte{
		[]byte(`[{"bar":"bar2","baz":"baz2","foo":"foo2"}]`),
	}
	if act, exp := message.GetAllBytes(resMsgs[0]), expParts; !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong result: %s != %s", act, exp)
	}
}
