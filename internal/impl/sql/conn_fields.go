package sql

import (
	"database/sql"
	"net/url"
	"strings"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
)

var driverField = service.NewStringEnumField("driver", "mysql", "postgres", "clickhouse", "mssql").
	Description("A database [driver](#drivers) to use.")

var dsnField = service.NewStringField("dsn").
	Description(`A Data Source Name to identify the target database.

#### Drivers

The following is a list of supported drivers, their placeholder style, and their respective DSN formats:

| Driver | Data Source Name Format |
|---|---|
` + "| `clickhouse` | [`clickhouse://[username[:password]@][netloc][:port]/dbname[?param1=value1&...&paramN=valueN]`](https://github.com/ClickHouse/clickhouse-go#dsn) |" + `
` + "| `mysql` | `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]` |" + `
` + "| `postgres` | `postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]` |" + `
` + "| `mssql` | `sqlserver://[user[:password]@][netloc][:port][?database=dbname&param1=value1&...]` |" + `

Please note that the ` + "`postgres`" + ` driver enforces SSL by default, you can override this with the parameter ` + "`sslmode=disable`" + ` if required.`).
	Example("clickhouse://username:password@host1:9000,host2:9000/database?dial_timeout=200ms&max_execution_time=60").
	Example("foouser:foopassword@tcp(localhost:3306)/foodb").
	Example("postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable")

func connFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewDurationField("conn_max_idle_time").
			Description(`An optional maximum amount of time a connection may be idle. Expired connections may be closed lazily before reuse. If value <= 0, connections are not closed due to a connection's idle time.`).
			Optional().
			Advanced(),
		service.NewDurationField("conn_max_life_time").
			Description(`An optional maximum amount of time a connection may be reused. Expired connections may be closed lazily before reuse. If value <= 0, connections are not closed due to a connection's age.`).
			Optional().
			Advanced(),
		service.NewIntField("conn_max_idle").
			Description(`An optional maximum number of connections in the idle connection pool. If conn_max_open is greater than 0 but less than the new conn_max_idle, then the new conn_max_idle will be reduced to match the conn_max_open limit. If value <= 0, no idle connections are retained. The default max idle connections is currently 2. This may change in a future release.`).
			Optional().
			Advanced(),
		service.NewIntField("conn_max_open").
			Description(`An optional maximum number of open connections to the database. If conn_max_idle is greater than 0 and the new conn_max_open is less than conn_max_idle, then conn_max_idle will be reduced to match the new conn_max_open limit. If value <= 0, then there is no limit on the number of open connections. The default is 0 (unlimited).`).
			Optional().
			Advanced(),
	}
}

func rawQueryField() *service.ConfigField {
	return service.NewStringField("query").
		Description("The query to execute. The style of placeholder to use depends on the driver, some drivers require question marks (`?`) whereas others expect incrementing dollar signs (`$1`, `$2`, and so on). The style to use is outlined in this table:" + `

| Driver | Placeholder Style |
|---|---|
` + "| `clickhouse` | Dollar sign |" + `
` + "| `mysql` | Question mark |" + `
` + "| `postgres` | Dollar sign |" + `
` + "| `mssql` | Question mark |" + `
`)
}

type connSettings struct {
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
	maxIdleConns    int
	maxOpenConns    int
}

func (c connSettings) apply(db *sql.DB) {
	db.SetConnMaxIdleTime(c.connMaxIdleTime)
	db.SetConnMaxLifetime(c.connMaxLifetime)
	db.SetMaxIdleConns(c.maxIdleConns)
	db.SetMaxOpenConns(c.maxOpenConns)
}

func connSettingsFromParsed(conf *service.ParsedConfig) (c connSettings, err error) {
	if conf.Contains("conn_max_life_time") {
		if c.connMaxLifetime, err = conf.FieldDuration("conn_max_life_time"); err != nil {
			return
		}
	}

	if conf.Contains("conn_max_idle_time") {
		if c.connMaxIdleTime, err = conf.FieldDuration("conn_max_idle_time"); err != nil {
			return
		}
	}

	if conf.Contains("conn_max_idle") {
		if c.maxIdleConns, err = conf.FieldInt("conn_max_idle"); err != nil {
			return
		}
	}

	if conf.Contains("conn_max_open") {
		if c.maxOpenConns, err = conf.FieldInt("conn_max_open"); err != nil {
			return
		}
	}
	return
}

func sqlOpenWithReworks(logger *service.Logger, driver, dsn string) (*sql.DB, error) {
	if driver == "clickhouse" && strings.HasPrefix("tcp", dsn) {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, err
		}

		u.Scheme = "clickhouse"

		u.Path = u.Query().Get("database")
		u.Query().Del("database")

		if username, password := u.Query().Get("username"), u.Query().Get("password"); username != "" {
			if password != "" {
				u.User = url.User(username)
			} else {
				u.User = url.UserPassword(username, password)
			}
		}

		newDSN := u.String()
		logger.Warnf("Detected old-style Clickhouse Data Source Name: '%v', replacing with new style: '%v'", dsn, newDSN)
		dsn = newDSN
	}
	return sql.Open(driver, dsn)
}
