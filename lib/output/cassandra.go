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
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/cenkalti/backoff/v4"
	"github.com/gocql/gocql"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCassandra] = TypeSpec{
		constructor: func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
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
			return newBatcherFromConf(conf.Cassandra.Batching, w, mgr, log, stats)
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.Cassandra, conf.Cassandra.Batching)
		},
		Status:  docs.StatusBeta,
		Batches: true,
		Async:   true,
		Summary: `
Runs a query against a Cassandra database for each message in order to insert data.`,
		Description: `
Query arguments are set using [interpolation functions](/docs/configuration/interpolation#bloblang-queries) in the ` + "`args`" + ` field.`,
		Examples: []docs.AnnotatedExample{
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
			docs.FieldAdvanced(
				"addresses",
				"A list of Cassandra nodes to connect to. Multiple comma separated addresses can be specified on a single line.",
				[]string{"localhost:9042"},
				[]string{"foo:9042", "bar:9042"},
				[]string{"foo:9042,bar:9042"},
			),
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
			docs.FieldAdvanced("query", "A query to execute for each message."),
			docs.FieldCommon(
				"args",
				"A list of arguments for the query to be resolved for each message.",
			).SupportsInterpolation(true),
			docs.FieldAdvanced(
				"consistency",
				"The consistency level to use.",
			).HasOptions(
				"ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM", "LOCAL_ONE",
			),
		}.Merge(retries.FieldSpecs()).Merge(docs.FieldSpecs{
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
	Consistency              string                `json:"consistency" yaml:"consistency"`
	retries.Config           `json:",inline" yaml:",inline"`
	MaxInFlight              int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching                 batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewCassandraConfig creates a new CassandraConfig with default values.
func NewCassandraConfig() CassandraConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

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

	args          []field.Expression
	session       *gocql.Session
	boff          backoff.ExponentialBackOff
	mQueryLatency metrics.StatTimer
	connLock      sync.RWMutex
}

func newCassandraWriter(conf CassandraConfig, log log.Modular, stats metrics.Type) (*cassandraWriter, error) {
	var args []field.Expression
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

	min, err := time.ParseDuration(c.conf.Config.Backoff.InitialInterval)
	if err != nil {
		return fmt.Errorf("parsing backoff initial interval: %w", err)
	}

	max, err := time.ParseDuration(c.conf.Config.Backoff.MaxInterval)
	if err != nil {
		return fmt.Errorf("parsing backoff max interval: %w", err)
	}

	conn.RetryPolicy = &decorator{
		NumRetries: int(c.conf.Config.MaxRetries),
		Min:        min,
		Max:        max,
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

func (c *cassandraWriter) writeRow(session *gocql.Session, msg types.Message) error {
	t0 := time.Now()

	values := make([]interface{}, 0, len(c.args))
	for _, arg := range c.args {
		values = append(values, arg.String(0, msg))
	}
	err := session.Query(c.conf.Query, values...).Exec()
	if err != nil {
		return err
	}

	c.mQueryLatency.Timing(time.Since(t0).Nanoseconds())
	return nil
}

func (c *cassandraWriter) writeBatch(session *gocql.Session, msg types.Message) error {
	batch := session.NewBatch(gocql.UnloggedBatch)
	t0 := time.Now()

	msg.Iter(func(i int, p types.Part) error {
		values := make([]interface{}, 0, len(c.args))
		for _, arg := range c.args {
			values = append(values, arg.String(i, msg))
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
	time.Sleep(d.napTime(q.Attempts()))
	return true
}

func getExponentialTime(min time.Duration, max time.Duration, attempts int) time.Duration {
	if min <= 0 {
		min = 100 * time.Millisecond
	}
	if max <= 0 {
		max = 10 * time.Second
	}
	minFloat := float64(min)
	napDuration := minFloat * math.Pow(2, float64(attempts-1))

	// Add some jitter
	napDuration += rand.Float64()*minFloat - (minFloat / 2)
	if napDuration > float64(max) {
		return time.Duration(max)
	}
	return time.Duration(napDuration)
}

func (d *decorator) napTime(attempts int) time.Duration {
	return getExponentialTime(d.Min, d.Max, attempts)
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
