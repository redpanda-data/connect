---
title: sql
type: processor
---

```yaml
sql:
  args: []
  driver: mysql
  dsn: ""
  query: ""
  result_codec: none
```

SQL is a processor that runs a query against a target database for each message
batch and, for queries that return rows, replaces the batch with the result.

If a query contains arguments they can be set as an array of strings supporting
[interpolation functions](/docs/configuration/interpolation#functions) in the
`args` field.

In order to execute an SQL query for each message of the batch use this
processor within a [`for_each`](for_each) processor:

``` yaml
for_each:
- sql:
    driver: mysql
    dsn: foouser:foopassword@tcp(localhost:3306)/foodb
    query: "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
    args:
    - ${!json_field:document.foo}
    - ${!json_field:document.bar}
    - ${!metadata:kafka_topic}
```

### Result Codecs

When a query returns rows they are serialised according to a chosen codec, and
the batch contents are replaced with the serialised result.

#### `none`

The result of the query is ignored and the message batch remains unchanged. If
your query does not return rows then this is the appropriate codec.

#### `json_array`

The resulting rows are serialised into an array of JSON objects, where each
object represents a row, where the key is the column name and the value is that
columns value in the row.

### Drivers

The following is a list of supported drivers and their respective DSN formats:

- `mysql`: `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]`
- `postgres`: `postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]`

Please note that the `postgres` driver enforces SSL by default, you
can override this with the parameter `sslmode=disable` if required.


