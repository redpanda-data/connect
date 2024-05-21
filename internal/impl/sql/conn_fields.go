package sql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
)

type DynamicCredentials struct {
	cache    string
	cacheKey string
}

var dynamicCredentialsField = service.NewObjectField("dynamic_credentials",
	service.NewStringField("cache").
		Description(`Specifies the cache resource to use for looking up dynamic credentials.`),
	service.NewStringField("cache_key").
		Description(`Specifies the key to use when looking up dynamic credentials in the cache.`),
).
	Description(`Specifies a cache resource for looking up credentials dynamically.
This can be useful in situations where credentials are rotated frequently, allowing re-authentication without a restart.
The value is read from the cache as a JSON message and is used to template the DSN.

Credentials are fetched when a new connection is created meaning that stale connections will persist unless` + "`conn_max_idle`" + ` is set to zero.
Similarly, if ` + "`conn_max_idle_time`" + ` is set to a low value then connections will be closed and re-authenticated more frequently

The specified init statement and files are executed only once overall and not per re-authentication.
`).
	Advanced().
	Optional()

var driverField = service.NewStringEnumField("driver", "mysql", "postgres", "clickhouse", "mssql", "sqlite", "oracle", "snowflake", "trino", "gocosmos").
	Description("A database [driver](#drivers) to use.")

var dsnField = service.NewInterpolatedStringField("dsn").
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
` + "| `trino` | [`http[s]://user[:pass]@host[:port][?parameters]`](https://github.com/trinodb/trino-go-client#dsn-data-source-name) |" + `
` + "| `gocosmos` | [`AccountEndpoint=<cosmosdb-endpoint>;AccountKey=<cosmosdb-account-key>[;TimeoutMs=<timeout-in-ms>][;Version=<cosmosdb-api-version>][;DefaultDb/Db=<db-name>][;AutoId=<true/false>][;InsecureSkipVerify=<true/false>]`](https://pkg.go.dev/github.com/microsoft/gocosmos#readme-example-usage) |" + `

Please note that the ` + "`postgres`" + ` driver enforces SSL by default, you can override this with the parameter ` + "`sslmode=disable`" + ` if required.

This value supports interpolations, but they are not evaluated on a per message basis.
Instead, you can use the ` + "`dynamic_credentials`" + ` configuration field to pull a message from a cache resource that will be used to provide templating fields.
New connections will use the latest values from the cache.

The ` + "`snowflake`" + ` driver supports multiple DSN formats. Please consult [the docs](https://pkg.go.dev/github.com/snowflakedb/gosnowflake#hdr-Connection_String) for more details. For [key pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html#configuring-key-pair-authentication), the DSN has the following format: ` + "`<snowflake_user>@<snowflake_account>/<db_name>/<schema_name>?warehouse=<warehouse>&role=<role>&authenticator=snowflake_jwt&privateKey=<base64_url_encoded_private_key>`" + `, where the value for the ` + "`privateKey`" + ` parameter can be constructed from an unencrypted RSA private key file ` + "`rsa_key.p8`" + ` using ` + "`openssl enc -d -base64 -in rsa_key.p8 | basenc --base64url -w0`" + ` (you can use ` + "`gbasenc`" + ` insted of ` + "`basenc`" + ` on OSX if you install ` + "`coreutils`" + ` via Homebrew). If you have a password-encrypted private key, you can decrypt it using ` + "`openssl pkcs8 -in rsa_key_encrypted.p8 -out rsa_key.p8`" + `. Also, make sure fields such as the username are URL-encoded.

The ` + "[`gocosmos`](https://pkg.go.dev/github.com/microsoft/gocosmos)" + ` driver is still experimental, but it has support for [hierarchical partition keys](https://learn.microsoft.com/en-us/azure/cosmos-db/hierarchical-partition-keys) as well as [cross-partition queries](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-query-container#cross-partition-query). Please refer to the [SQL notes](https://github.com/microsoft/gocosmos/blob/main/SQL.md) for details.`).
	Example("clickhouse://username:password@host1:9000,host2:9000/database?dial_timeout=200ms&max_execution_time=60").
	Example("foouser:foopassword@tcp(localhost:3306)/foodb").
	Example("postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable").
	Example("oracle://foouser:foopass@localhost:1521/service_name")

func connFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("init_files").
			Description(`
An optional list of file paths containing SQL statements to execute immediately upon the first connection to the target database. This is a useful way to initialise tables before processing data. Glob patterns are supported, including super globs (double star).

Care should be taken to ensure that the statements are idempotent, and therefore would not cause issues when run multiple times after service restarts. If both ` + "`init_statement` and `init_files` are specified the `init_statement` is executed _after_ the `init_files`." + `

If a statement fails for any reason a warning log will be emitted but the operation of this component will not be stopped.
`).
			Example([]any{`./init/*.sql`}).
			Example([]any{`./foo.sql`, `./bar.sql`}).
			Optional().
			Advanced().
			Version("4.10.0"),
		service.NewStringField("init_statement").
			Description(`
An optional SQL statement to execute immediately upon the first connection to the target database. This is a useful way to initialise tables before processing data. Care should be taken to ensure that the statement is idempotent, and therefore would not cause issues when run multiple times after service restarts.

If both ` + "`init_statement` and `init_files` are specified the `init_statement` is executed _after_ the `init_files`." + `

If the statement fails for any reason a warning log will be emitted but the operation of this component will not be stopped.
`).
			Example(`
CREATE TABLE IF NOT EXISTS some_table (
  foo varchar(50) not null,
  bar integer,
  baz varchar(50),
  primary key (foo)
) WITHOUT ROWID;
`).
			Optional().
			Advanced().
			Version("4.10.0"),
		service.NewDurationField("conn_max_idle_time").
			Description("An optional maximum amount of time a connection may be idle. Expired connections may be closed lazily before reuse. If `value <= 0`, connections are not closed due to a connections idle time.").
			Optional().
			Advanced(),
		service.NewDurationField("conn_max_life_time").
			Description("An optional maximum amount of time a connection may be reused. Expired connections may be closed lazily before reuse. If `value <= 0`, connections are not closed due to a connections age.").
			Optional().
			Advanced(),
		service.NewIntField("conn_max_idle").
			Description("An optional maximum number of connections in the idle connection pool. If conn_max_open is greater than 0 but less than the new conn_max_idle, then the new conn_max_idle will be reduced to match the conn_max_open limit. If `value <= 0`, no idle connections are retained. The default max idle connections is currently 2. This may change in a future release.").
			Default(2).
			Optional().
			Advanced(),
		service.NewIntField("conn_max_open").
			Description("An optional maximum number of open connections to the database. If conn_max_idle is greater than 0 and the new conn_max_open is less than conn_max_idle, then conn_max_idle will be reduced to match the new conn_max_open limit. If `value <= 0`, then there is no limit on the number of open connections. The default is 0 (unlimited).").
			Optional().
			Advanced(),
	}
}

func rawQueryField() *service.ConfigField {
	return service.NewStringField("query").
		Description("The query to execute. The style of placeholder to use depends on the driver, some drivers require question marks (`?`) whereas others expect incrementing dollar signs (`$1`, `$2`, and so on) or colons (`:1`, `:2` and so on). The style to use is outlined in this table:" + `

| Driver | Placeholder Style |
|---|---|
` + "| `clickhouse` | Dollar sign |" + `
` + "| `mysql` | Question mark |" + `
` + "| `postgres` | Dollar sign |" + `
` + "| `mssql` | Question mark |" + `
` + "| `sqlite` | Question mark |" + `
` + "| `oracle` | Colon |" + `
` + "| `snowflake` | Question mark |" + `
` + "| `trino` | Question mark |" + `
` + "| `gocosmos` | Colon |" + `
`)
}

type connSettings struct {
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
	maxIdleConns    int
	maxOpenConns    int

	initOnce           sync.Once
	initFileStatements [][2]string // (path,statement)
	initStatement      string
	dynamicCredentials *DynamicCredentials
}

func (c *connSettings) apply(ctx context.Context, db *sql.DB, log *service.Logger) {
	db.SetConnMaxIdleTime(c.connMaxIdleTime)
	db.SetConnMaxLifetime(c.connMaxLifetime)
	db.SetMaxIdleConns(c.maxIdleConns)
	db.SetMaxOpenConns(c.maxOpenConns)

	c.initOnce.Do(func() {
		for _, fileStmt := range c.initFileStatements {
			if _, err := db.ExecContext(ctx, fileStmt[1]); err != nil {
				log.Warnf("Failed to execute init_file '%v': %v", fileStmt[0], err)
			} else {
				log.Debugf("Successfully ran init_file '%v'", fileStmt[0])
			}
		}
		if c.initStatement != "" {
			if _, err := db.ExecContext(ctx, c.initStatement); err != nil {
				log.Warnf("Failed to execute init_statement: %v", err)
			} else {
				log.Debug("Successfully ran init_statement")
			}
		}
	})
}

func connSettingsFromParsed(
	conf *service.ParsedConfig,
	mgr *service.Resources,
) (c *connSettings, err error) {
	c = &connSettings{}

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

	if conf.Contains("init_statement") {
		if c.initStatement, err = conf.FieldString("init_statement"); err != nil {
			return
		}
	}

	if conf.Contains("init_files") {
		var tmpFiles []string
		if tmpFiles, err = conf.FieldStringList("init_files"); err != nil {
			return
		}
		if tmpFiles, err = service.Globs(mgr.FS(), tmpFiles...); err != nil {
			err = fmt.Errorf("failed to expand init_files glob patterns: %w", err)
			return
		}
		for _, p := range tmpFiles {
			var statementBytes []byte
			if statementBytes, err = service.ReadFile(mgr.FS(), p); err != nil {
				return
			}
			c.initFileStatements = append(c.initFileStatements, [2]string{
				p, string(statementBytes),
			})
		}
	}

	if conf.Contains("dynamic_credentials") {
		var creds DynamicCredentials
		if creds.cache, err = conf.FieldString("dynamic_credentials", "cache"); err != nil {
			return
		}
		if creds.cacheKey, err = conf.FieldString("dynamic_credentials", "cache_key"); err != nil {
			return
		}
		c.dynamicCredentials = &creds
	}
	return
}

func sqlOpenWithReworks(manager *service.Resources, driver string, dsn *service.InterpolatedString, dynamicCredentials *DynamicCredentials) (*sql.DB, error) {
	connector, err := NewDynamicCredentialConnector(
		manager,
		driver,
		dsn,
		dynamicCredentials,
	)
	if err != nil {
		return nil, err
	}

	return sql.OpenDB(connector), nil
}
