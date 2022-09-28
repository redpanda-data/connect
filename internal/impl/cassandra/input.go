package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/benthosdev/benthos/v4/public/service"
)

func cassandraConfigSpec() *service.ConfigSpec {
		return service.NewConfigSpec().
				Categories("Services").
				Summary("Executes a find query and creates a message for each row received.").
				Field(service.NewStringListField("addresses").
						Description("A list of Cassandra nodes to connect to.")).
				Field(service.NewBoolField("auth_enabled").
						Description("Whether to use password authentification.").
						Default(false)).
				Field(service.NewStringField("username").
						Description("A username.")).
				Field(service.NewStringField("password").
						Description("A password.")).
				Field(service.NewStringField("query").
						Description("A query to execute.")).
				Field(service.NewStringField("timeout"))
}

func init() {
		err := service.RegisterInput(
				"cassandra", cassandraConfigSpec(),
				func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
						return newCassandraInput(conf)
				})
		if err != nil {
			panic(err)
		}
}

func newCassandraInput(conf *service.ParsedConfig) (service.Input, error) {
		addrs, err := conf.FieldStringList("addresses")
		if err != nil {
				return nil, err
		}

		enabled, err := conf.FieldBool("auth_enabled")
		if err != nil {
			return nil, err
		}

		var (
			username string = ""
			password string = ""
		)
		if enabled == true {
			fmt.Printf("AUTH ENABLED\n")
			username, err = conf.FieldString("username")
			if err != nil {
				return nil, err
			}
			password, err = conf.FieldString("password")
			if err != nil {
				return nil, err
			}
		}

		query, err := conf.FieldString("query")
		if err != nil {
				return nil, err
		}
		tout, err := conf.FieldString("timeout")
		if err != nil {
				return nil, err
		}
		timeout, err := time.ParseDuration(tout)
		if err != nil {
				return nil, err
		}

		return service.AutoRetryNacks(&cassandraInput{
				addresses:	addrs,
				auth:		PasswordAuthenticator{Enabled: enabled, Username: username, Password: password},
				query:		query,
				timeout:	timeout,
		}), nil
}

type PasswordAuthenticator struct {
		Enabled 	bool
		Username 	string
		Password	string
}

type cassandraInput struct {
		addresses	[]string
		auth		PasswordAuthenticator
		query		string
		timeout		time.Duration

		session		*gocql.Session
		iter		*gocql.Iter
}

func (c *cassandraInput) Connect(ctx context.Context) error {
		if c.session != nil {
			return nil
		}

		var err error
		conn := gocql.NewCluster(c.addresses...)
		if c.auth.Enabled {
			conn.Authenticator = gocql.PasswordAuthenticator{
				Username: c.auth.Username,
				Password: c.auth.Password,
			}
		}
		
		conn.Timeout = c.timeout

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
