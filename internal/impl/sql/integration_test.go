package sql

import (
	"database/sql"
	"flag"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || regexp.MustCompile(strings.Split(m, "/")[0]).FindString(t.Name()) == "" {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^TestIntegration$")
	}

	t.Run("clickhouse", clickhouseIntegration)
	t.Run("postgres", postgresIntegration)
	t.Run("mysql", mySQLIntegration)
	t.Run("mssql", msSQLIntegration)
}

func clickhouseIntegration(t *testing.T) {
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

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) error {
		_, err := db.Exec(fmt.Sprintf(`create table %v (
  foo String,
  bar Int64,
  baz String
) engine=Memory;`, name))
		return err
	}

	dsn := fmt.Sprintf("tcp://localhost:%v/", resource.GetPort("9000/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("clickhouse", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if err = createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	// TODO
}

func postgresIntegration(t *testing.T) {
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
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) error {
		_, err := db.Exec(fmt.Sprintf(`create table %v (
  foo varchar(50) not null,
  bar integer not null,
  baz varchar(50) not null,
  primary key (foo)
		);`, name))
		return err
	}

	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%v/testdb?sslmode=disable", resource.GetPort("5432/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if err = createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	// TODO
}

func mySQLIntegration(t *testing.T) {
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
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) error {
		_, err := db.Exec(fmt.Sprintf(`create table %v (
  foo varchar(50) not null,
  bar integer not null,
  baz varchar(50) not null,
  primary key (foo)
		);`, name))
		return err
	}

	dsn := fmt.Sprintf("testuser:testpass@tcp(localhost:%v)/testdb", resource.GetPort("3306/tcp"))
	require.NoError(t, pool.Retry(func() error {
		if db, err = sql.Open("mysql", dsn); err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if err = createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	// TODO
}

func msSQLIntegration(t *testing.T) {
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

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) error {
		_, err := db.Exec(fmt.Sprintf(`create table %v (
  foo varchar(50) not null,
  bar integer not null,
  baz varchar(50) not null,
  primary key (foo)
		);`, name))
		return err
	}

	dsn := fmt.Sprintf("sqlserver://sa:"+testPassword+"@localhost:%v?database=master", resource.GetPort("1433/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("mssql", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if err = createTable("footable"); err != nil {
			return err
		}
		return nil
	}))
}
