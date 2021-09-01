package output

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/gocql/gocql"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCassandra] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			c, err := newCassandraWriter(conf.Cassandra, log, stats)
			if err != nil {
				return nil, err
			}
			w, err := NewAsyncWriter(
				TypeCassandra, conf.Cassandra.MaxInFlight, c, log, stats,
			)
			if err != nil {
				return nil, err
			}
			return NewBatcherFromConfig(conf.Cassandra.Batching, w, mgr, log, stats)
		}),
		Status:  docs.StatusBeta,
		Batches: true,
		Async:   true,
		Summary: `
Runs a query against a Cassandra database for each message in order to insert data.`,
		Description: `
Query arguments are set using [interpolation functions](/docs/configuration/interpolation#bloblang-queries) in the ` + "`args`" + ` field.

When populating timestamp columns the value must either be a string in ISO 8601 format (2006-01-02T15:04:05Z07:00), or an integer representing unix time in seconds.`,
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Basic Inserts",
				Summary: "If we were to create a table with some basic columns with `CREATE TABLE foo.bar (id int primary key, content text, created_at timestamp);`, and were processing JSON documents of the form `{\"id\":\"342354354\",\"content\":\"hello world\",\"timestamp\":1605219406}`, we could populate our table with the following config:",
				Config: `
output:
  cassandra:
    addresses:
      - localhost:9042
    query: 'INSERT INTO foo.bar (id, content, created_at) VALUES (?, ?, ?)'
    args:
      - ${! json("id") }
      - ${! json("content") }
      - ${! json("timestamp").format_timestamp() }
    batching:
      count: 500
`,
			},
			{
				Title:   "Insert JSON Documents",
				Summary: "The following example inserts JSON documents into the table `footable` of the keyspace `foospace` using INSERT JSON (https://cassandra.apache.org/doc/latest/cql/json.html#insert-json).",
				Config: `
output:
  cassandra:
    addresses:
      - localhost:9042
    query: 'INSERT INTO foospace.footable JSON ?'
    args:
      - ${! content() }
    batching:
      count: 500
`,
			},
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldString(
				"addresses",
				"A list of Cassandra nodes to connect to. Multiple comma separated addresses can be specified on a single line.",
				[]string{"localhost:9042"},
				[]string{"foo:9042", "bar:9042"},
				[]string{"foo:9042,bar:9042"},
			).Array(),
			btls.FieldSpec(),
			docs.FieldAdvanced(
				"password_authenticator",
				"An object containing the username and password.",
			).WithChildren(
				docs.FieldCommon("enabled", "Whether to use password authentication."),
				docs.FieldCommon("username", "A username."),
				docs.FieldCommon("password", "A password."),
			),
			docs.FieldAdvanced(
				"disable_initial_host_lookup",
				"If enabled the driver will not attempt to get host info from the system.peers table. This can speed up queries but will mean that data_centre, rack and token information will not be available.",
			),
			docs.FieldCommon("query", "A query to execute for each message."),
			docs.FieldString(
				"args",
				"A list of arguments for the query to be resolved for each message.",
			).IsInterpolated().Array(),
			docs.FieldBloblang(
				"args_mapping",
				"A [Bloblang mapping](/docs/guides/bloblang/about) that can be used to provide arguments to Cassandra queries.",
				`
output:
	cassandra:
		addresses: ["localhost:9042"]
		query: INSERT INTO SOME_KEYSPACE.SOME_TABLE (id, first_name, last_name) VALUES (now(), ?, ?)
		args_mapping: |
			root = [
				this.firstName,
				this.lastName,
			]
`),
			docs.FieldAdvanced(
				"consistency",
				"The consistency level to use.",
			).HasOptions(
				"ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM", "LOCAL_ONE",
			),
			docs.FieldAdvanced("max_retries", "The maximum number of retries before giving up on a request."),
			docs.FieldAdvanced("backoff", "Control time intervals between retry attempts.").WithChildren(
				docs.FieldAdvanced("initial_interval", "The initial period to wait between retry attempts."),
				docs.FieldAdvanced("max_interval", "The maximum period to wait between retry attempts."),
				docs.FieldDeprecated("max_elapsed_time"),
			),
		}.Merge(docs.FieldSpecs{
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			batch.FieldSpec(),
		}),
	}
}

//------------------------------------------------------------------------------

// PasswordAuthenticator contains the fields that will be used to authenticate with
// the Cassandra cluster.
type PasswordAuthenticator struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// CassandraConfig contains configuration fields for the Cassandra output type.
type CassandraConfig struct {
	Addresses                []string              `json:"addresses" yaml:"addresses"`
	TLS                      btls.Config           `json:"tls" yaml:"tls"`
	PasswordAuthenticator    PasswordAuthenticator `json:"password_authenticator" yaml:"password_authenticator"`
	DisableInitialHostLookup bool                  `json:"disable_initial_host_lookup" yaml:"disable_initial_host_lookup"`
	Query                    string                `json:"query" yaml:"query"`
	Args                     []string              `json:"args" yaml:"args"`
	ArgsMapping              string                `json:"args_mapping" yaml:"args_mapping"`
	Consistency              string                `json:"consistency" yaml:"consistency"`
	// TODO: V4 Remove this and replace with explicit values.
	retries.Config `json:",inline" yaml:",inline"`
	MaxInFlight    int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching       batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewCassandraConfig creates a new CassandraConfig with default values.
func NewCassandraConfig() CassandraConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = ""

	return CassandraConfig{
		Addresses: []string{},
		TLS:       btls.NewConfig(),
		PasswordAuthenticator: PasswordAuthenticator{
			Enabled:  false,
			Username: "",
			Password: "",
		},
		DisableInitialHostLookup: false,
		Query:                    "",
		Args:                     []string{},
		ArgsMapping:              "",
		Consistency:              gocql.Quorum.String(),
		Config:                   rConf,
		MaxInFlight:              1,
		Batching:                 batch.NewPolicyConfig(),
	}
}

type cassandraWriter struct {
	conf    CassandraConfig
	log     log.Modular
	stats   metrics.Type
	tlsConf *tls.Config

	backoffMin time.Duration
	backoffMax time.Duration

	args          []*field.Expression
	argsMapping   *mapping.Executor
	session       *gocql.Session
	mQueryLatency metrics.StatTimer
	connLock      sync.RWMutex
}

func newCassandraWriter(conf CassandraConfig, log log.Modular, stats metrics.Type) (*cassandraWriter, error) {
	// Allow only args or args_mapping for now.
	if len(conf.Args) > 0 && conf.ArgsMapping != "" {
		return nil, fmt.Errorf("can only specify one of [args, args_mapping]")
	}

	var args []*field.Expression
	for i, v := range conf.Args {
		expr, err := bloblang.NewField(v)
		if err != nil {
			return nil, fmt.Errorf("failed to parse arg %v expression: %v", i, err)
		}
		args = append(args, expr)
	}

	c := cassandraWriter{
		log:           log,
		stats:         stats,
		conf:          conf,
		args:          args,
		mQueryLatency: stats.GetTimer("query.latency"),
	}

	var err error
	if conf.TLS.Enabled {
		if c.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	if c.backoffMin, err = time.ParseDuration(c.conf.Config.Backoff.InitialInterval); err != nil {
		return nil, fmt.Errorf("parsing backoff initial interval: %w", err)
	}
	if c.backoffMax, err = time.ParseDuration(c.conf.Config.Backoff.MaxInterval); err != nil {
		return nil, fmt.Errorf("parsing backoff max interval: %w", err)
	}
	if c.argsMapping, err = bloblang.NewMapping("", conf.ArgsMapping); err != nil {
		return nil, fmt.Errorf("failed to parse args_mapping: %v", err)
	}
	return &c, nil
}

// ConnectWithContext establishes a connection to Cassandra.
func (c *cassandraWriter) ConnectWithContext(ctx context.Context) error {
	c.connLock.Lock()
	defer c.connLock.Unlock()
	if c.session != nil {
		return nil
	}

	var err error
	conn := gocql.NewCluster(c.conf.Addresses...)
	if c.tlsConf != nil {
		conn.SslOpts = &gocql.SslOptions{
			Config: c.tlsConf,
			CaPath: c.conf.TLS.RootCAsFile,
		}
		conn.DisableInitialHostLookup = c.conf.TLS.InsecureSkipVerify
	}
	if c.conf.PasswordAuthenticator.Enabled {
		conn.Authenticator = gocql.PasswordAuthenticator{
			Username: c.conf.PasswordAuthenticator.Username,
			Password: c.conf.PasswordAuthenticator.Password,
		}
	}
	conn.DisableInitialHostLookup = c.conf.DisableInitialHostLookup
	if conn.Consistency, err = gocql.ParseConsistencyWrapper(c.conf.Consistency); err != nil {
		return fmt.Errorf("parsing consistency: %w", err)
	}

	conn.RetryPolicy = &decorator{
		NumRetries: int(c.conf.Config.MaxRetries),
		Min:        c.backoffMin,
		Max:        c.backoffMax,
	}
	session, err := conn.CreateSession()
	if err != nil {
		return fmt.Errorf("creating Cassandra session: %w", err)
	}

	c.session = session
	c.log.Infof("Sending messages to Cassandra: %v\n", c.conf.Addresses)
	return nil
}

// WriteWithContext writes a message to Cassandra.
func (c *cassandraWriter) WriteWithContext(ctx context.Context, msg types.Message) error {
	c.connLock.RLock()
	session := c.session
	c.connLock.RUnlock()

	if c.session == nil {
		return types.ErrNotConnected
	}

	if msg.Len() == 1 {
		return c.writeRow(session, msg)
	}
	return c.writeBatch(session, msg)
}

type stringValue string

func (c *cassandraWriter) writeRow(session *gocql.Session, msg types.Message) error {
	t0 := time.Now()

	values, err := c.parseArgs(msg, 0)
	if err != nil {
		return fmt.Errorf("parsing args: %w", err)
	}

	if err := session.Query(c.conf.Query, values...).Exec(); err != nil {
		return err
	}

	c.mQueryLatency.Timing(time.Since(t0).Nanoseconds())
	return nil
}

func (c *cassandraWriter) writeBatch(session *gocql.Session, msg types.Message) error {
	batch := session.NewBatch(gocql.UnloggedBatch)
	t0 := time.Now()

	msg.Iter(func(i int, p types.Part) error {
		values, err := c.parseArgs(msg, i)
		if err != nil {
			return fmt.Errorf("parsing args for part: %d: %w", i, err)
		}
		batch.Query(c.conf.Query, values...)
		return nil
	})

	err := session.ExecuteBatch(batch)
	if err != nil {
		return err
	}
	c.mQueryLatency.Timing(time.Since(t0).Nanoseconds())
	return nil
}

func (c *cassandraWriter) parseArgs(msg types.Message, index int) ([]interface{}, error) {
	// If we've been given the "args" field, extract values from there.
	if len(c.args) > 0 {
		values := make([]interface{}, 0, len(c.args))
		for _, arg := range c.args {
			values = append(values, stringValue(arg.String(0, msg)))
		}

		return values, nil
	}

	// We've got an "args_mapping" field, extract values from there.
	part, err := c.argsMapping.MapPart(index, msg)
	if err != nil {
		return nil, fmt.Errorf("executing bloblang mapping: %w", err)
	}

	jraw, err := part.JSON()
	if err != nil {
		return nil, fmt.Errorf("parsing bloblang mapping result as json: %w", err)
	}

	j, ok := jraw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected bloblang mapping result to be []interface{} but was %T", jraw)
	}

	return j, nil
}

// CloseAsync shuts down the Cassandra output and stops processing messages.
func (c *cassandraWriter) CloseAsync() {
	go func() {
		c.connLock.Lock()
		if c.session != nil {
			c.session.Close()
			c.session = nil
		}
		c.connLock.Unlock()
	}()
}

// WaitForClose blocks until the Cassandra output has closed down.
func (c *cassandraWriter) WaitForClose(timeout time.Duration) error {
	return nil
}

type decorator struct {
	NumRetries int
	Min, Max   time.Duration
}

func (d *decorator) Attempt(q gocql.RetryableQuery) bool {
	if q.Attempts() > d.NumRetries {
		return false
	}
	time.Sleep(getExponentialTime(d.Min, d.Max, q.Attempts()))
	return true
}

func getExponentialTime(min, max time.Duration, attempts int) time.Duration {
	minFloat := float64(min)
	napDuration := minFloat * math.Pow(2, float64(attempts-1))

	// Add some jitter
	napDuration += rand.Float64()*minFloat - (minFloat / 2)
	if napDuration > float64(max) {
		return max
	}
	return time.Duration(napDuration)
}

func (d *decorator) GetRetryType(err error) gocql.RetryType {
	switch t := err.(type) {
	// not enough replica alive to perform query with required consistency
	case *gocql.RequestErrUnavailable:
		if t.Alive > 0 {
			return gocql.RetryNextHost
		}
		return gocql.Retry
	// write timeout - uncertain whetever write was succesful or not
	case *gocql.RequestErrWriteTimeout:
		if t.Received > 0 {
			return gocql.Ignore
		}
		return gocql.Retry
	default:
		return gocql.Rethrow
	}
}
