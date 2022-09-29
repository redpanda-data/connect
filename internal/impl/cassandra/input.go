package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/public/service"
)

func cassandraConfigSpec() *service.ConfigSpec {
		return service.NewConfigSpec().
				Categories("Services").
				Summary("Executes a find query and creates a message for each row received.").
				Field(service.NewStringListField("addresses").
						Description("A list of Cassandra nodes to connect to.")).
				Field(service.NewInternalField(FieldAuth())).
				Field(service.NewBoolField("disable_initial_host_lookup").
						Description("If enabled the driver will not attempt to get host info from the system.peers table. This can speed up queries but will mean that data_centre, rack and token information will not be available.").
						Optional()).
				Field(service.NewStringField("query").
						Description("A query to execute.")).
				Field(service.NewStringField("timeout").
						Description("").
						Advanced().
						Default("600ms").
						Example("600ms"))
}

func FieldAuth() docs.FieldSpec {
		return docs.FieldObject("password_authenticator", "Optional configuration of Cassandra authentication parameters.").WithChildren(
				docs.FieldBool("enabled", "Whether to use password authentication").Optional(),
				docs.FieldString("username", "A username").Optional(),
				docs.FieldString("password", "A password").Optional(),
		).Advanced()
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

		pAuth, err := AuthFromParsedConfig(conf.Namespace("password_authenticator"))
		if err != nil {
				return nil, err
		}

		var disable bool
		if conf.Contains("disable_initial_host_lookup") {
				disable, err = conf.FieldBool("disable_initial_host_lookup")
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
				auth:		pAuth,
				disableIHL: disable,
				query:		query,
				timeout:	timeout,
		}), nil
}

func NewAuth() PasswordAuthenticator {
	return PasswordAuthenticator{
		Enabled:	false,
		Username:	"",
		Password:	"",
	}
}

func AuthFromParsedConfig(p *service.ParsedConfig) (pa PasswordAuthenticator, err error) {
	pa = NewAuth()
	if p.Contains("enabled") {
		if pa.Enabled, err = p.FieldBool("enabled"); err != nil {
			return pa, err
		}
	}
	if p.Contains("username") {
		if pa.Username, err = p.FieldString("username"); err != nil {
			return pa, err
		}
	}
	if p.Contains("password") {
		if pa.Password, err = p.FieldString("password"); err != nil {
			return pa, err
		}
	}
	return pa, nil
}

type PasswordAuthenticator struct {
		Enabled 	bool
		Username 	string
		Password	string
}

type cassandraInput struct {
		addresses	[]string
		auth		PasswordAuthenticator
		disableIHL	bool
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
		
		conn.DisableInitialHostLookup = c.disableIHL
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
