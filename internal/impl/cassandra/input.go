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
	spec := service.NewConfigSpec().
		Categories("Services").
		Summary("Executes a find query and creates a message for each row received.").
		Field(service.NewStringListField("addresses").
			Description("A list of Cassandra nodes to connect to.")).
		Field(service.NewInternalField(fieldAuth())).
		Field(service.NewBoolField("disable_initial_host_lookup").
			Description("If enabled the driver will not attempt to get host info from the system.peers table. This can speed up queries but will mean that data_centre, rack and token information will not be available.").
			Advanced().
			Optional()).
		Field(service.NewStringField("query").
			Description("A query to execute.")).
		Field(service.NewIntField("max_retries").
			Description("The maximum number of retries before giving up on a request.").
			Advanced().
			Optional()).
		Field(service.NewInternalField(fieldBackoff())).
		Field(service.NewStringField("timeout").
			Description("").
			Default("600ms").
			Example("600ms"))
	spec = spec.
		Example("Minimal Select (Cassandra/Scylla)",
			`
Let's presume that we have 3 Cassandra nodes, like in this tutorial by Sebastian Sigl from freeCodeCamp:

https://www.freecodecamp.org/news/the-apache-cassandra-beginner-tutorial/

Then if we want to select everything from the table users_by_country, we should use the configuration below.
If we specify the stdin output, the result will look like:

{"age":23,"country":"UK","first_name":"Bob","last_name":"Sandler","user_email":"bob@email.com"}

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

func fieldAuth() docs.FieldSpec {
	return docs.FieldObject("password_authenticator", "Optional configuration of Cassandra authentication parameters.").WithChildren(
		docs.FieldBool("enabled", "Whether to use password authentication").Optional(),
		docs.FieldString("username", "A username").Optional(),
		docs.FieldString("password", "A password").Optional().Secret(),
	).Advanced()
}

func fieldBackoff() docs.FieldSpec {
	return docs.FieldObject("backoff", "Control time intervals between retry attempts.").WithChildren(
		docs.FieldString("initial_interval", "The initial period to wait between retry attempts.").Optional(),
		docs.FieldString("max_interval", "The maximum period to wait between retry attempts.").Optional(),
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

	pAuth, err := authFromParsedConfig(conf.Namespace("password_authenticator"))
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

	var retries int
	if conf.Contains("max_retries") {
		retries, err = conf.FieldInt("max_retries")
		if err != nil {
			return nil, err
		}
	}

	backoff, err := backoffFromParsedConfig(conf.Namespace("backoff"))
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
		addresses:  addrs,
		auth:       pAuth,
		disableIHL: disable,
		query:      query,
		maxRetries: retries,
		backoff:    backoff,
		timeout:    timeout,
	}), nil
}

func newAuth() passwordAuthenticator {
	return passwordAuthenticator{
		Enabled:  false,
		Username: "",
		Password: "",
	}
}

func newBackoff() backOff {
	return backOff{
		InitInterval: 0,
		MaxInterval:  0,
	}
}

func authFromParsedConfig(p *service.ParsedConfig) (pa passwordAuthenticator, err error) {
	pa = newAuth()
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

func backoffFromParsedConfig(p *service.ParsedConfig) (b backOff, err error) {
	b = newBackoff()
	var init, max string
	if p.Contains("initial_interval") {
		if init, err = p.FieldString("initial_interval"); err != nil {
			return b, err
		}
		if b.InitInterval, err = time.ParseDuration(init); err != nil {
			return b, err
		}
	}
	if p.Contains("max_interval") {
		if max, err = p.FieldString("max_interval"); err != nil {
			return b, err
		}
		if b.MaxInterval, err = time.ParseDuration(max); err != nil {
			return b, err
		}
	}
	return b, nil
}

type passwordAuthenticator struct {
	Enabled  bool
	Username string
	Password string
}

type backOff struct {
	InitInterval time.Duration
	MaxInterval  time.Duration
}

type cassandraInput struct {
	addresses  []string
	auth       passwordAuthenticator
	disableIHL bool
	query      string
	maxRetries int
	backoff    backOff
	timeout    time.Duration

	session *gocql.Session
	iter    *gocql.Iter
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

	conn.RetryPolicy = &decorator{
		NumRetries: c.maxRetries,
		Min:        c.backoff.InitInterval,
		Max:        c.backoff.MaxInterval,
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
