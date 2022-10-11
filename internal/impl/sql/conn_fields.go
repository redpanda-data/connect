package sql

import (
	"database/sql"
	"net/url"
	"strings"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
)

var driverField = service.NewStringEnumField("driver", "mysql", "postgres", "clickhouse", "mssql", "sqlite", "oracle", "snowflake").
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
` + "| `sqlite` | `file:/path/to/filename.db[?param&=value1&...]` |" + `
` + "| `oracle` | `oracle://[username[:password]@][netloc][:port]/service_name?server=server2&server=server3` |" + `
` + "| `snowflake` | `username[:password]@account_identifier/dbname/schemaname[?param1=value&...&paramN=valueN]` |" + `

Please note that the ` + "`postgres`" + ` driver enforces SSL by default, you can override this with the parameter ` + "`sslmode=disable`" + ` if required.

The ` + "`snowflake`" + ` driver supports multiple DSN formats. Please consult [the docs](https://pkg.go.dev/github.com/snowflakedb/gosnowflake#hdr-Connection_String) for more details. For [key pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html#configuring-key-pair-authentication), the DSN has the following format: ` + "`<snowflake_user>@<snowflake_account>/<db_name>/<schema_name>?warehouse=<warehouse>&role=<role>&authenticator=snowflake_jwt&privateKey=<base64_url_encoded_private_key>`" + `, where the value for the ` + "`privateKey`" + ` parameter can be constructed from an unencrypted RSA private key file ` + "`rsa_key.p8`" + ` using ` + "`openssl enc -d -base64 -in rsa_key.p8 | basenc --base64url -w0`" + ` (you can use ` + "`gbasenc`" + ` insted of ` + "`basenc`" + ` on OSX if you install ` + "`coreutils`" + ` via Homebrew). If you have a password-encrypted private key, you can decrypt it using ` + "`openssl pkcs8 -in rsa_key_encrypted.p8 -out rsa_key.p8`. Also, make sure fields such as the username are URL-encoded.").
	Example("clickhouse://username:password@host1:9000,host2:9000/database?dial_timeout=200ms&max_execution_time=60").
	Example("foouser:foopassword@tcp(localhost:3306)/foodb").
	Example("postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable").
	Example("oracle://foouser:foopass@localhost:1521/service_name")

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
` + "| `sqlite` | Question mark |" + `
` + "| `oracle` | Colon |" + `
` + "| `snowflake` | Question mark |" + `
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
	if driver == "clickhouse" && strings.HasPrefix(dsn, "tcp") {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, err
		}

		u.Scheme = "clickhouse"

		uq := u.Query()
		u.Path = uq.Get("database")
		if username, password := uq.Get("username"), uq.Get("password"); username != "" {
			if password != "" {
				u.User = url.User(username)
			} else {
				u.User = url.UserPassword(username, password)
			}
		}

		uq.Del("database")
		uq.Del("username")
		uq.Del("password")

		u.RawQuery = uq.Encode()
		newDSN := u.String()

		logger.Warnf("Detected old-style Clickhouse Data Source Name: '%v', replacing with new style: '%v'", dsn, newDSN)
		dsn = newDSN
	}
	return sql.Open(driver, dsn)
}
