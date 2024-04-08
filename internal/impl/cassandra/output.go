package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/gocql/gocql"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/value"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	coFieldQuery       = "query"
	coFieldArgsMapping = "args_mapping"
	coFieldConsistency = "consistency"
	coFieldLoggedBatch = "logged_batch"
	coFieldBatching    = "batching"
)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Summary("Runs a query against a Cassandra database for each message in order to insert data.").
		Description(output.Description(true, true, `
Query arguments can be set using a bloblang array for the fields using the `+"`args_mapping`"+` field.

When populating timestamp columns the value must either be a string in ISO 8601 format (2006-01-02T15:04:05Z07:00), or an integer representing unix time in seconds.`)).
		Example(
			"Basic Inserts",
			"If we were to create a table with some basic columns with `CREATE TABLE foo.bar (id int primary key, content text, created_at timestamp);`, and were processing JSON documents of the form `{\"id\":\"342354354\",\"content\":\"hello world\",\"timestamp\":1605219406}` using logged batches, we could populate our table with the following config:",
			`
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
		).
		Example(
			"Insert JSON Documents",
			"The following example inserts JSON documents into the table `footable` of the keyspace `foospace` using INSERT JSON (https://cassandra.apache.org/doc/latest/cql/json.html#insert-json).",
			`
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
		).
		Fields(clientFields()...).
		Fields(
			service.NewStringField(coFieldQuery).
				Description("A query to execute for each message."),
			service.NewBloblangField(coFieldArgsMapping).
				Description("A [Bloblang mapping](/docs/guides/bloblang/about) that can be used to provide arguments to Cassandra queries. The result of the query must be an array containing a matching number of elements to the query arguments.").
				Version("3.55.0").
				Optional(),
			service.NewStringEnumField(coFieldConsistency,
				"ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM", "LOCAL_ONE").
				Description("The consistency level to use.").
				Advanced().
				Default("QUORUM"),
			service.NewBoolField(coFieldLoggedBatch).
				Description("If enabled the driver will perform a logged batch. Disabling this prompts unlogged batches to be used instead, which are less efficient but necessary for alternative storages that do not support logged batches.").
				Advanced().
				Default(true),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(coFieldBatching),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"cassandra", outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(coFieldBatching); err != nil {
				return
			}
			out, err = newCassandraWriter(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type cassandraWriter struct {
	log *service.Logger

	query       string
	clientConf  clientConf
	argsMapping *bloblang.Executor
	batchType   gocql.BatchType
	consistency gocql.Consistency

	session  *gocql.Session
	connLock sync.RWMutex
}

func newCassandraWriter(conf *service.ParsedConfig, mgr *service.Resources) (c *cassandraWriter, err error) {
	c = &cassandraWriter{
		log: mgr.Logger(),
	}

	if c.query, err = conf.FieldString(coFieldQuery); err != nil {
		return
	}

	if c.clientConf, err = clientConfFromParsed(conf); err != nil {
		return
	}

	if aStr, _ := conf.FieldString(coFieldArgsMapping); aStr != "" {
		if c.argsMapping, err = conf.FieldBloblang(coFieldArgsMapping); err != nil {
			return
		}
	}

	c.batchType = gocql.UnloggedBatch
	if loggedBatch, _ := conf.FieldBool(coFieldLoggedBatch); loggedBatch {
		c.batchType = gocql.LoggedBatch
	}

	var consistencyStr string
	if consistencyStr, err = conf.FieldString(coFieldConsistency); err != nil {
		return
	}
	if c.consistency, err = gocql.ParseConsistencyWrapper(consistencyStr); err != nil {
		return nil, fmt.Errorf("parsing consistency: %w", err)
	}

	return
}

func (c *cassandraWriter) Connect(ctx context.Context) error {
	c.connLock.Lock()
	defer c.connLock.Unlock()
	if c.session != nil {
		return nil
	}

	conn, err := c.clientConf.Create()
	if err != nil {
		return err
	}
	conn.Consistency = c.consistency

	session, err := conn.CreateSession()
	if err != nil {
		return fmt.Errorf("creating Cassandra session: %w", err)
	}

	c.session = session
	return nil
}

func (c *cassandraWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	c.connLock.RLock()
	session := c.session
	c.connLock.RUnlock()

	if c.session == nil {
		return component.ErrNotConnected
	}

	if len(batch) == 1 {
		return c.writeRow(session, batch)
	}
	return c.writeBatch(session, batch)
}

func (c *cassandraWriter) writeRow(session *gocql.Session, b service.MessageBatch) error {
	values, err := c.mapArgs(b, 0)
	if err != nil {
		return fmt.Errorf("parsing args: %w", err)
	}
	return session.Query(c.query, values...).Exec()
}

func (c *cassandraWriter) writeBatch(session *gocql.Session, b service.MessageBatch) error {
	batch := session.NewBatch(c.batchType)

	for i := range b {
		values, err := c.mapArgs(b, i)
		if err != nil {
			return fmt.Errorf("parsing args for part: %d: %w", i, err)
		}
		batch.Query(c.query, values...)
	}

	return session.ExecuteBatch(batch)
}

func (c *cassandraWriter) mapArgs(b service.MessageBatch, index int) ([]any, error) {
	if c.argsMapping != nil {
		// We've got an "args_mapping" field, extract values from there.
		part, err := b.BloblangQuery(index, c.argsMapping)
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
		t, err := value.IGetTimestamp(g.v)
		if err != nil {
			return nil, err
		}
		return gocql.Marshal(info, t)
	case gocql.TypeDouble:
		f, err := value.IGetNumber(g.v)
		if err != nil {
			return nil, err
		}
		return gocql.Marshal(info, f)
	case gocql.TypeFloat:
		f, err := value.IGetFloat32(g.v)
		if err != nil {
			return nil, err
		}
		return gocql.Marshal(info, f)
	case gocql.TypeVarchar:
		return gocql.Marshal(info, value.IToString(g.v))
	}
	if _, isJSONNum := g.v.(json.Number); isJSONNum {
		i, err := value.IGetInt(g.v)
		if err != nil {
			return nil, err
		}
		return gocql.Marshal(info, i)
	}
	return gocql.Marshal(info, g.v)
}
