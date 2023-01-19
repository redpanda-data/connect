package cassandra

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/gocql/gocql"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		c, err := newCassandraWriter(conf.Cassandra, nm)
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("cassandra", conf.Cassandra.MaxInFlight, c, nm)
		if err != nil {
			return nil, err
		}
		return batcher.NewFromConfig(conf.Cassandra.Batching, w, nm)
	}), docs.ComponentSpec{
		Name:   "cassandra",
		Status: docs.StatusBeta,
		Summary: `
Runs a query against a Cassandra database for each message in order to insert data.`,
		Description: output.Description(true, true, `
Query arguments can be set using a bloblang array for the fields using the `+"`args_mapping`"+` field.

When populating timestamp columns the value must either be a string in ISO 8601 format (2006-01-02T15:04:05Z07:00), or an integer representing unix time in seconds.`),
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Basic Inserts",
				Summary: "If we were to create a table with some basic columns with `CREATE TABLE foo.bar (id int primary key, content text, created_at timestamp);`, and were processing JSON documents of the form `{\"id\":\"342354354\",\"content\":\"hello world\",\"timestamp\":1605219406}` using logged batches, we could populate our table with the following config:",
				Config: `
output:
  cassandra:
    addresses:
      - localhost:9042
    query: 'INSERT INTO foo.bar (id, content, created_at) VALUES (?, ?, ?)'
    args_mapping: |
      root = [
        this.id,
        this.content,
        this.timestamp
      ]
    batching:
      count: 500
      period: 1s
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
    args_mapping: 'root = [ this ]'
    batching:
      count: 500
      period: 1s
`,
			},
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"addresses",
				"A list of Cassandra nodes to connect to. Multiple comma separated addresses can be specified on a single line.",
				[]string{"localhost:9042"},
				[]string{"foo:9042", "bar:9042"},
				[]string{"foo:9042,bar:9042"},
			).Array(),
			btls.FieldSpec(),
			docs.FieldObject(
				"password_authenticator",
				"An object containing the username and password.",
			).WithChildren(
				docs.FieldBool("enabled", "Whether to use password authentication."),
				docs.FieldString("username", "A username."),
				docs.FieldString("password", "A password.").Secret(),
			).Advanced(),
			docs.FieldBool(
				"disable_initial_host_lookup",
				"If enabled the driver will not attempt to get host info from the system.peers table. This can speed up queries but will mean that data_centre, rack and token information will not be available.",
			).Advanced(),
			docs.FieldString("query", "A query to execute for each message."),
			docs.FieldBloblang(
				"args_mapping",
				"A [Bloblang mapping](/docs/guides/bloblang/about) that can be used to provide arguments to Cassandra queries. The result of the query must be an array containing a matching number of elements to the query arguments.").AtVersion("3.55.0"),
			docs.FieldString(
				"consistency",
				"The consistency level to use.",
			).HasOptions(
				"ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM", "LOCAL_ONE",
			).Advanced(),
			docs.FieldBool(
				"logged_batch",
				"If enabled the driver will perform a logged batch. Disabling this prompts unlogged batches to be used instead, which are less efficient but necessary for alternative storages that do not support logged batches.",
			).Advanced(),
			docs.FieldInt("max_retries", "The maximum number of retries before giving up on a request.").Advanced(),
			docs.FieldObject("backoff", "Control time intervals between retry attempts.").WithChildren(
				docs.FieldString("initial_interval", "The initial period to wait between retry attempts."),
				docs.FieldString("max_interval", "The maximum period to wait between retry attempts."),
				docs.FieldString("max_elapsed_time", "").Deprecated(),
			).Advanced(),
			docs.FieldString("timeout", "The client connection timeout.").AtVersion("3.63.0"),
		).WithChildren(
			docs.FieldInt("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			policy.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewCassandraConfig()),
	})
	if err != nil {
		panic(err)
	}
}

type cassandraWriter struct {
	conf    output.CassandraConfig
	log     log.Modular
	stats   metrics.Type
	tlsConf *tls.Config

	backoffMin time.Duration
	backoffMax time.Duration

	session  *gocql.Session
	connLock sync.RWMutex

	argsMapping *mapping.Executor
	batchType   gocql.BatchType
}

func newCassandraWriter(conf output.CassandraConfig, mgr bundle.NewManagement) (*cassandraWriter, error) {
	c := cassandraWriter{
		log:   mgr.Logger(),
		stats: mgr.Metrics(),
		conf:  conf,
	}

	var err error
	if conf.TLS.Enabled {
		if c.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}
	if c.backoffMin, err = time.ParseDuration(c.conf.Config.Backoff.InitialInterval); err != nil {
		return nil, fmt.Errorf("parsing backoff initial interval: %w", err)
	}
	if c.backoffMax, err = time.ParseDuration(c.conf.Config.Backoff.MaxInterval); err != nil {
		return nil, fmt.Errorf("parsing backoff max interval: %w", err)
	}
	if err = c.parseArgs(mgr); err != nil {
		return nil, fmt.Errorf("parsing args: %w", err)
	}
	c.batchType = gocql.UnloggedBatch
	if c.conf.LoggedBatch {
		c.batchType = gocql.LoggedBatch
	}

	return &c, nil
}

func (c *cassandraWriter) parseArgs(mgr bundle.NewManagement) error {
	if c.conf.ArgsMapping != "" {
		var err error
		if c.argsMapping, err = mgr.BloblEnvironment().NewMapping(c.conf.ArgsMapping); err != nil {
			return fmt.Errorf("parsing args_mapping: %w", err)
		}
	}
	return nil
}

func (c *cassandraWriter) Connect(ctx context.Context) error {
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
	if tout := c.conf.Timeout; len(tout) > 0 {
		var err error
		if conn.Timeout, err = time.ParseDuration(tout); err != nil {
			return fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}
	session, err := conn.CreateSession()
	if err != nil {
		return fmt.Errorf("creating Cassandra session: %w", err)
	}

	c.session = session
	c.log.Infof("Sending messages to Cassandra: %v\n", c.conf.Addresses)
	return nil
}

func (c *cassandraWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	c.connLock.RLock()
	session := c.session
	c.connLock.RUnlock()

	if c.session == nil {
		return component.ErrNotConnected
	}

	if msg.Len() == 1 {
		return c.writeRow(session, msg)
	}
	return c.writeBatch(session, msg)
}

func (c *cassandraWriter) writeRow(session *gocql.Session, msg message.Batch) error {
	values, err := c.mapArgs(msg, 0)
	if err != nil {
		return fmt.Errorf("parsing args: %w", err)
	}
	return session.Query(c.conf.Query, values...).Exec()
}

func (c *cassandraWriter) writeBatch(session *gocql.Session, msg message.Batch) error {
	batch := session.NewBatch(c.batchType)

	if err := msg.Iter(func(i int, p *message.Part) error {
		values, err := c.mapArgs(msg, i)
		if err != nil {
			return fmt.Errorf("parsing args for part: %d: %w", i, err)
		}
		batch.Query(c.conf.Query, values...)
		return nil
	}); err != nil {
		return err
	}

	return session.ExecuteBatch(batch)
}

func (c *cassandraWriter) mapArgs(msg message.Batch, index int) ([]any, error) {
	if c.argsMapping != nil {
		// We've got an "args_mapping" field, extract values from there.
		part, err := c.argsMapping.MapPart(index, msg)
		if err != nil {
			return nil, fmt.Errorf("executing bloblang mapping: %w", err)
		}

		jraw, err := part.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("parsing bloblang mapping result as json: %w", err)
		}

		j, ok := jraw.([]any)
		if !ok {
			return nil, fmt.Errorf("expected bloblang mapping result to be []interface{} but was %T", jraw)
		}

		for i, v := range j {
			j[i] = genericValue{v: v}
		}
		return j, nil
	}
	return nil, nil
}

func (c *cassandraWriter) Close(context.Context) error {
	c.connLock.Lock()
	if c.session != nil {
		c.session.Close()
		c.session = nil
	}
	c.connLock.Unlock()
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
	// write timeout - uncertain whetever write was successful or not
	case *gocql.RequestErrWriteTimeout:
		if t.Received > 0 {
			return gocql.Ignore
		}
		return gocql.Retry
	default:
		return gocql.Rethrow
	}
}

type genericValue struct {
	v any
}

// We get typed values out of mappings. However, gocql performs type checking
// and unfortunately does not like timestamp and some other values as strings:
// https://github.com/gocql/gocql/blob/5913df4d474e0b2492a129d17bbb3c04537a15cd/marshal.go#L1160
// it's also very strict on numerical types, so we need to do some magic here.
func (g genericValue) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	switch info.Type() {
	case gocql.TypeTimestamp:
		t, err := query.IGetTimestamp(g.v)
		if err != nil {
			return nil, err
		}
		return gocql.Marshal(info, t)
	case gocql.TypeDouble:
		f, err := query.IGetNumber(g.v)
		if err != nil {
			return nil, err
		}
		return gocql.Marshal(info, f)
	case gocql.TypeFloat:
		f, err := query.IGetFloat32(g.v)
		if err != nil {
			return nil, err
		}
		return gocql.Marshal(info, f)
	case gocql.TypeVarchar:
		return gocql.Marshal(info, query.IToString(g.v))
	}
	if _, isJSONNum := g.v.(json.Number); isJSONNum {
		i, err := query.IGetInt(g.v)
		if err != nil {
			return nil, err
		}
		return gocql.Marshal(info, i)
	}
	return gocql.Marshal(info, g.v)
}
