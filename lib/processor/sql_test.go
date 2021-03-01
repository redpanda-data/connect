package processor

import (
	"database/sql"
	"flag"
	"fmt"
	"reflect"
	"regexp"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLIntegration(t *testing.T) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || !regexp.MustCompile(m).MatchString(t.Name()) {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^TestIntegration$")
	}

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("TestSQLClickhouseIntegration", SQLClickhouseIntegration)
	t.Run("TestSQLPostgresIntegration", SQLPostgresIntegration)
	t.Run("TestSQLMySQLIntegration", SQLMySQLIntegration)
}

func SQLClickhouseIntegration(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "yandex/clickhouse-server",
		ExposedPorts: []string{"9000/tcp"},
	})
	require.NoError(t, err)

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	dsn := fmt.Sprintf("tcp://localhost:%v/", resource.GetPort("9000/tcp"))
	if err = pool.Retry(func() error {
		db, dberr := sql.Open("clickhouse", dsn)
		if dberr != nil {
			return dberr
		}
		if dberr = db.Ping(); err != nil {
			return dberr
		}
		if _, dberr = db.Exec(`create table footable (
  foo String,
  bar String,
  baz String
) engine=Memory;`); dberr != nil {
			return dberr
		}
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("testSQLClickhouse", func(t *testing.T) {
		testSQLClickhouse(t, dsn)
	})
}

func testSQLClickhouse(t *testing.T, dsn string) {
	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "clickhouse"
	conf.SQL.DataSourceName = dsn
	conf.SQL.Query = "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
	conf.SQL.Args = []string{
		"${! json(\"foo\") }",
		"${! json(\"bar\") }",
		"${! json(\"baz\") }",
	}

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":"bar1","baz":"baz1"}`),
		[]byte(`{"foo":"foo2","bar":"bar2","baz":"baz2"}`),
	}

	resMsgs, response := s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)
	assert.Equal(t, parts, message.GetAllBytes(resMsgs[0]))

	conf.SQL.Query = "SELECT * FROM footable WHERE foo = ?;"
	conf.SQL.Args = []string{
		"${! json(\"foo\") }",
	}
	conf.SQL.ResultCodec = "json_array"
	s, err = NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts = [][]byte{
		[]byte(`{"foo":"foo1"}`),
		[]byte(`{"foo":"foo2"}`),
	}

	resMsgs, response = s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expParts := [][]byte{
		[]byte(`[{"bar":"bar1","baz":"baz1","foo":"foo1"}]`),
		[]byte(`[{"bar":"bar2","baz":"baz2","foo":"foo2"}]`),
	}
	assert.Equal(t, expParts, message.GetAllBytes(resMsgs[0]))
}

func SQLPostgresIntegration(t *testing.T) {
	t.Parallel()

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

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

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

	t.Run("testSQLPostgres", func(t *testing.T) {
		testSQLPostgres(t, dsn)
	})
	t.Run("testSQLPostgresDeprecated", func(t *testing.T) {
		testSQLPostgresDeprecated(t, dsn)
	})
}

func testSQLPostgres(t *testing.T, dsn string) {
	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "postgres"
	conf.SQL.DataSourceName = dsn
	conf.SQL.Query = "INSERT INTO footable (foo, bar, baz) VALUES ($1, $2, $3);"
	conf.SQL.Args = []string{
		"${! json(\"foo\") }",
		"${! json(\"bar\") }",
		"${! json(\"baz\") }",
	}

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":"bar1","baz":"baz1"}`),
		[]byte(`{"foo":"foo2","bar":"bar2","baz":"baz2"}`),
	}

	resMsgs, response := s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)
	assert.Equal(t, parts, message.GetAllBytes(resMsgs[0]))

	conf.SQL.Query = "SELECT * FROM footable WHERE foo = $1;"
	conf.SQL.Args = []string{
		"${! json(\"foo\") }",
	}
	conf.SQL.ResultCodec = "json_array"
	s, err = NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts = [][]byte{
		[]byte(`{"foo":"foo1"}`),
		[]byte(`{"foo":"foo2"}`),
	}

	resMsgs, response = s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expParts := [][]byte{
		[]byte(`[{"bar":"bar1","baz":"baz1","foo":"foo1"}]`),
		[]byte(`[{"bar":"bar2","baz":"baz2","foo":"foo2"}]`),
	}
	assert.Equal(t, expParts, message.GetAllBytes(resMsgs[0]))
}

func testSQLPostgresDeprecated(t *testing.T, dsn string) {
	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "postgres"
	conf.SQL.DSN = dsn
	conf.SQL.Query = "INSERT INTO footable (foo, bar, baz) VALUES ($1, $2, $3);"
	conf.SQL.Args = []string{
		"${! json(\"foo\").from(1) }",
		"${! json(\"bar\").from(1) }",
		"${! json(\"baz\").from(1) }",
	}

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte(`{"foo":"foo3","bar":"bar3","baz":"baz3"}`),
		[]byte(`{"foo":"foo4","bar":"bar4","baz":"baz4"}`),
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
		"${! json(\"foo\").from(1) }",
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
		[]byte(`[{"bar":"bar4","baz":"baz4","foo":"foo4"}]`),
	}
	if act, exp := message.GetAllBytes(resMsgs[0]), expParts; !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong result: %s != %s", act, exp)
	}
}

func SQLMySQLIntegration(t *testing.T) {
	t.Parallel()

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

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

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

	t.Run("testSQLMySQL", func(t *testing.T) {
		testSQLMySQL(t, dsn)
	})
	t.Run("testSQLMySQLDeprecated", func(t *testing.T) {
		testSQLMySQLDeprecated(t, dsn)
	})
}

func testSQLMySQL(t *testing.T, dsn string) {
	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "mysql"
	conf.SQL.DataSourceName = dsn
	conf.SQL.Query = "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
	conf.SQL.Args = []string{
		"${! json(\"foo\") }",
		"${! json(\"bar\") }",
		"${! json(\"baz\") }",
	}

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":"bar1","baz":"baz1"}`),
		[]byte(`{"foo":"foo2","bar":"bar2","baz":"baz2"}`),
	}

	resMsgs, response := s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)
	assert.Equal(t, parts, message.GetAllBytes(resMsgs[0]))

	conf.SQL.Query = "SELECT * FROM footable WHERE foo = ?;"
	conf.SQL.Args = []string{
		"${! json(\"foo\") }",
	}
	conf.SQL.ResultCodec = "json_array"
	s, err = NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts = [][]byte{
		[]byte(`{"foo":"foo1"}`),
		[]byte(`{"foo":"foo2"}`),
	}

	resMsgs, response = s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expParts := [][]byte{
		[]byte(`[{"bar":"bar1","baz":"baz1","foo":"foo1"}]`),
		[]byte(`[{"bar":"bar2","baz":"baz2","foo":"foo2"}]`),
	}
	assert.Equal(t, expParts, message.GetAllBytes(resMsgs[0]))
}

func testSQLMySQLDeprecated(t *testing.T, dsn string) {
	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "mysql"
	conf.SQL.DSN = dsn
	conf.SQL.Query = "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
	conf.SQL.Args = []string{
		"${! json(\"foo\").from(1) }",
		"${! json(\"bar\").from(1) }",
		"${! json(\"baz\").from(1) }",
	}

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte(`{"foo":"foo3","bar":"bar3","baz":"baz3"}`),
		[]byte(`{"foo":"foo4","bar":"bar4","baz":"baz4"}`),
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
		"${! json(\"foo\").from(1) }",
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
		[]byte(`[{"bar":"bar4","baz":"baz4","foo":"foo4"}]`),
	}
	if act, exp := message.GetAllBytes(resMsgs[0]), expParts; !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong result: %s != %s", act, exp)
	}
}
