package processor

import (
	"database/sql"
	"flag"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLIntegration(t *testing.T) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || regexp.MustCompile(strings.Split(m, "/")[0]).FindString(t.Name()) == "" {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^TestIntegration$")
	}

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("TestSQLClickhouseIntegration", SQLClickhouseIntegration)
	t.Run("TestSQLPostgresIntegration", SQLPostgresIntegration)
	t.Run("TestSQLMySQLIntegration", SQLMySQLIntegration)
	t.Run("TestSQLMSSQLIntegration", SQLMSSQLIntegration)
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

	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	})

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
  bar Int64,
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
	conf.SQL.ArgsMapping = `root = [ this.foo, this.bar.floor(), this.baz ]`

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":11,"baz":"baz1"}`),
		[]byte(`{"foo":"foo2","bar":12,"baz":"baz2"}`),
	}

	resMsgs, response := s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)
	assert.Equal(t, parts, message.GetAllBytes(resMsgs[0]))
	require.Empty(t, GetFail(resMsgs[0].Get(0)))
	require.Empty(t, GetFail(resMsgs[0].Get(1)))

	conf.SQL.Query = "SELECT * FROM footable WHERE foo = ?;"
	conf.SQL.ArgsMapping = `[ this.foo ]`
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
		[]byte(`[{"bar":11,"baz":"baz1","foo":"foo1"}]`),
		[]byte(`[{"bar":12,"baz":"baz2","foo":"foo2"}]`),
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

	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	})

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
  bar integer not null,
  baz varchar(50) not null,
  primary key (foo)
);`); dberr != nil {
			return dberr
		}
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("testSQLPostgresArgsMapping", func(t *testing.T) {
		testSQLPostgresArgsMapping(t, dsn)
	})
}

func testSQLPostgresArgsMapping(t *testing.T, dsn string) {
	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "postgres"
	conf.SQL.DataSourceName = dsn
	conf.SQL.Query = "INSERT INTO footable (foo, bar, baz) VALUES ($1, $2, $3);"
	conf.SQL.ArgsMapping = `[ this.foo, this.bar, this.baz ]`

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":11,"baz":"baz1"}`),
		[]byte(`{"foo":"foo2","bar":12,"baz":"baz2"}`),
	}

	resMsgs, response := s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)
	assert.Equal(t, parts, message.GetAllBytes(resMsgs[0]))

	conf.SQL.Query = "SELECT * FROM footable WHERE foo = $1;"
	conf.SQL.ArgsMapping = `[ this.foo ]`
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
		[]byte(`[{"bar":11,"baz":"baz1","foo":"foo1"}]`),
		[]byte(`[{"bar":12,"baz":"baz2","foo":"foo2"}]`),
	}
	assert.Equal(t, expParts, message.GetAllBytes(resMsgs[0]))
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

	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	})

	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	dsn := fmt.Sprintf("testuser:testpass@tcp(localhost:%v)/testdb", resource.GetPort("3306/tcp"))
	if err = pool.Retry(func() error {
		var dberr error
		if db, dberr = sql.Open("mysql", dsn); dberr != nil {
			return dberr
		}
		if dberr = db.Ping(); dberr != nil {
			db.Close()
			db = nil
			return dberr
		}
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("testSQLMySQLArgsMapping", func(t *testing.T) {
		testSQLMySQLArgsMapping(t, db, dsn)
	})
	t.Run("testSQLMySQLDynamicQueries", func(t *testing.T) {
		testSQLMySQLDynamicQueries(t, db, dsn)
	})
}

func testSQLMySQLArgsMapping(t *testing.T, db *sql.DB, dsn string) {
	_, err := db.Exec(`create table footable (
  foo varchar(50) not null,
  bar integer not null,
  baz varchar(50) not null,
  primary key (foo)
);`)
	require.NoError(t, err)

	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "mysql"
	conf.SQL.DataSourceName = dsn
	conf.SQL.Query = "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
	conf.SQL.ArgsMapping = `[ this.foo, this.bar, this.baz ]`

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":11,"baz":"baz1"}`),
		[]byte(`{"foo":"foo2","bar":12,"baz":"baz2"}`),
	}

	resMsgs, response := s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)
	assert.Equal(t, parts, message.GetAllBytes(resMsgs[0]))

	conf.SQL.Query = "SELECT * FROM footable WHERE foo = ?;"
	conf.SQL.ArgsMapping = `[ this.foo ]`
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
		[]byte(`[{"bar":11,"baz":"baz1","foo":"foo1"}]`),
		[]byte(`[{"bar":12,"baz":"baz2","foo":"foo2"}]`),
	}
	assert.Equal(t, expParts, message.GetAllBytes(resMsgs[0]))
}

func testSQLMySQLDynamicQueries(t *testing.T, db *sql.DB, dsn string) {
	_, err := db.Exec(`create table bartable (
  foo varchar(50) not null,
  bar integer not null,
  baz varchar(50) not null,
  primary key (foo)
);`)
	require.NoError(t, err)

	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "mysql"
	conf.SQL.DataSourceName = dsn
	conf.SQL.Query = `${! json("query") }`
	conf.SQL.UnsafeDynamicQuery = true
	conf.SQL.ArgsMapping = `[ this.foo, this.bar, this.baz ]`

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"query":"INSERT INTO bartable (foo, bar, baz) VALUES (?, ?, ?);","foo":"foo1","bar":11,"baz":"baz1"}`),
		[]byte(`{"query":"INSERT INTO bartable (foo, bar, baz) VALUES (?, ?, ?);","foo":"foo2","bar":12,"baz":"baz2"}`),
	}

	resMsgs, response := s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)
	assert.Equal(t, parts, message.GetAllBytes(resMsgs[0]))

	conf.SQL.Query = `${! json("query") }`
	conf.SQL.ArgsMapping = `[ this.foo ]`
	conf.SQL.UnsafeDynamicQuery = true
	conf.SQL.ResultCodec = "json_array"
	s, err = NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts = [][]byte{
		[]byte(`{"query":"SELECT * FROM bartable WHERE foo = ?;","foo":"foo1"}`),
		[]byte(`{"query":"SELECT * FROM bartable WHERE foo = ?;","foo":"foo2"}`),
	}

	resMsgs, response = s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)

	expParts := [][]byte{
		[]byte(`[{"bar":11,"baz":"baz1","foo":"foo1"}]`),
		[]byte(`[{"bar":12,"baz":"baz2","foo":"foo2"}]`),
	}
	assert.Equal(t, expParts, message.GetAllBytes(resMsgs[0]))
}

func SQLMSSQLIntegration(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	testPassword := "ins4n3lyStrongP4ssword"
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "mcr.microsoft.com/mssql/server",
		ExposedPorts: []string{"1433/tcp"},
		Env: []string{
			"ACCEPT_EULA=Y",
			"SA_PASSWORD=" + testPassword,
		},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	})
	dsn := fmt.Sprintf("sqlserver://sa:"+testPassword+"@localhost:%v?database=master", resource.GetPort("1433/tcp"))
	if err = pool.Retry(func() error {
		db, dberr := sql.Open("mssql", dsn)
		if dberr != nil {
			return dberr
		}
		if dberr = db.Ping(); err != nil {
			return dberr
		}
		if _, dberr = db.Exec(`create table footable (
  foo varchar(50) not null,
  bar integer not null,
  baz varchar(50) not null,
  primary key (foo)
		);`); dberr != nil {
			return dberr
		}
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("testSQLMSSQLArgsMapping", func(t *testing.T) {
		testSQLMSSQLArgsMapping(t, dsn)
	})
}

func testSQLMSSQLArgsMapping(t *testing.T, dsn string) {
	conf := NewConfig()
	conf.Type = TypeSQL
	conf.SQL.Driver = "mssql"
	conf.SQL.DataSourceName = dsn
	conf.SQL.Query = "INSERT INTO footable (foo, bar, baz) VALUES ($1, $2, $3);"
	conf.SQL.ArgsMapping = `[ this.foo, this.bar, this.baz ]`

	s, err := NewSQL(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	parts := [][]byte{
		[]byte(`{"foo":"foo1","bar":11,"baz":"baz1"}`),
		[]byte(`{"foo":"foo2","bar":12,"baz":"baz2"}`),
	}

	resMsgs, response := s.ProcessMessage(message.New(parts))
	require.Nil(t, response)
	require.Len(t, resMsgs, 1)
	assert.Equal(t, parts, message.GetAllBytes(resMsgs[0]))

	conf.SQL.Query = "SELECT * FROM footable WHERE foo = $1;"
	conf.SQL.ArgsMapping = `[ this.foo ]`
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
		[]byte(`[{"bar":11,"baz":"baz1","foo":"foo1"}]`),
		[]byte(`[{"bar":12,"baz":"baz2","foo":"foo2"}]`),
	}
	assert.Equal(t, expParts, message.GetAllBytes(resMsgs[0]))
}
