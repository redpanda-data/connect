package cassandra

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	ciFieldQuery = "query"
)

func inputConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Categories("Services").
		Summary("Executes a find query and creates a message for each row received.").
		Fields(clientFields()...).
		Field(service.NewStringField(ciFieldQuery).
			Description("A query to execute.")).
		Field(service.NewAutoRetryNacksToggleField()).
		Example("Minimal Select (Cassandra/Scylla)",
			`
Let's presume that we have 3 Cassandra nodes, like in this tutorial by Sebastian Sigl from freeCodeCamp:

https://www.freecodecamp.org/news/the-apache-cassandra-beginner-tutorial/

Then if we want to select everything from the table users_by_country, we should use the configuration below.
If we specify the stdin output, the result will look like:

`+"```json"+`
{"age":23,"country":"UK","first_name":"Bob","last_name":"Sandler","user_email":"bob@email.com"}
`+"```"+`

This configuration also works for Scylla.
`,
			`
input:
  cassandra:
    addresses:
      - 172.17.0.2
    query:
      'SELECT * FROM learn_cassandra.users_by_country'
`,
		)
	return spec
}

func init() {
	err := service.RegisterInput(
		"cassandra", inputConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newCassandraInput(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newCassandraInput(conf *service.ParsedConfig) (service.Input, error) {
	query, err := conf.FieldString(ciFieldQuery)
	if err != nil {
		return nil, err
	}

	clientConf, err := clientConfFromParsed(conf)
	if err != nil {
		return nil, err
	}

	return service.AutoRetryNacksToggled(conf, &cassandraInput{
		query:      query,
		clientConf: clientConf,
	})
}

type cassandraInput struct {
	query      string
	clientConf clientConf

	session *gocql.Session
	iter    *gocql.Iter
}

func (c *cassandraInput) Connect(ctx context.Context) error {
	if c.session != nil {
		return nil
	}

	conn, err := c.clientConf.Create()
	if err != nil {
		return err
	}

	session, err := conn.CreateSession()
	if err != nil {
		return fmt.Errorf("creating Cassandra session: %w", err)
	}

	c.session = session
	c.iter = session.Query(c.query).Iter()
	return nil
}

func (c *cassandraInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	mp := make(map[string]interface{})
	if !c.iter.MapScan(mp) {
		return nil, nil, service.ErrEndOfInput
	}

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(mp)
	return msg, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (c *cassandraInput) Close(ctx context.Context) error {
	if c.session != nil {
		c.session.Close()
		c.session = nil
	}
	return nil
}
