package sql

import "github.com/benthosdev/benthos/v4/public/service"

var driverField = service.NewStringEnumField("driver", "mysql", "postgres", "clickhouse", "mssql").
	Description("A database [driver](#drivers) to use.")

var dsnField = service.NewStringField("dsn").
	Description(`A Data Source Name to identify the target database.

#### Drivers

The following is a list of supported drivers and their respective DSN formats:

| Driver | Data Source Name Format |
|---|---|
` + "| `clickhouse` | [`tcp://[netloc][:port][?param1=value1&...&paramN=valueN]`](https://github.com/ClickHouse/clickhouse-go#dsn)" + `
` + "| `mysql` | `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]` |" + `
` + "| `postgres` | `postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]` |" + `
` + "| `mssql` | `sqlserver://[user[:password]@][netloc][:port][?database=dbname&param1=value1&...]` |" + `

Please note that the ` + "`postgres`" + ` driver enforces SSL by default, you
can override this with the parameter ` + "`sslmode=disable`" + ` if required.`).
	Example("tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000").
	Example("foouser:foopassword@tcp(localhost:3306)/foodb").
	Example("postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable")
