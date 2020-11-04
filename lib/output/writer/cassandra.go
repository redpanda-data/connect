package writer

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/cenkalti/backoff/v4"
	"github.com/gocql/gocql"
	"golang.org/x/sync/errgroup"
)

// PasswordAuthenticator contains the fields that will be used to authenticate with
// the Cassandra cluster.
type PasswordAuthenticator struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// CassandraConfig contains configuration fields for the Cassandra output type.
type CassandraConfig struct {
	Nodes                 []string              `json:"nodes" yaml:"nodes"`
	AsyncParts            bool                  `json:"async" yaml:"async"`
	PasswordAuthenticator PasswordAuthenticator `json:"password_authenticator" yaml:"password_authenticator"`
	Keyspace              string                `json:"keyspace" yaml:"keyspace"`
	Table                 string                `json:"table" yaml:"table"`
	Consistency           string                `json:"consistency" yaml:"consistency"`
	retries.Config        `json:",inline" yaml:",inline"`
}

// NewCassandraConfig creates a new CassandraConfig with default values.
func NewCassandraConfig() CassandraConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return CassandraConfig{
		Nodes:      []string{"localhost:9042"},
		AsyncParts: true,
		PasswordAuthenticator: PasswordAuthenticator{
			Enabled:  true,
			Username: "cassandra",
			Password: "cassandra",
		},
		Keyspace:    "benthos",
		Table:       "benthos",
		Consistency: gocql.Quorum.String(),
		Config:      rConf,
	}
}

// Cassandra holds the configuration properties for a running instance of the Cassandra sink.
type Cassandra struct {
	conf          CassandraConfig
	log           log.Modular
	stats         metrics.Type
	session       *gocql.Session
	query         string
	boff          backoff.ExponentialBackOff
	mQueryLatency metrics.StatTimer
	connLock      sync.RWMutex
}

// NewCassandra creates a new Cassandra writer type.
func NewCassandra(conf CassandraConfig, log log.Modular, stats metrics.Type) (*Cassandra, error) {
	c := Cassandra{
		log:           log,
		stats:         stats,
		conf:          conf,
		query:         fmt.Sprintf("INSERT INTO %s.%s JSON ?", conf.Keyspace, conf.Table),
		mQueryLatency: stats.GetTimer("query.latency"),
	}
	return &c, nil
}

// Connect establishes a connection to Cassandra by calling ConnectWithContext and passing
// a simple Background context.
func (c *Cassandra) Connect() error {
	return c.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a connection to Cassandra.
func (c *Cassandra) ConnectWithContext(ctx context.Context) error {
	c.connLock.Lock()
	defer c.connLock.Unlock()
	if c.session != nil {
		return nil
	}

	var err error
	conn := gocql.NewCluster(c.conf.Nodes...)
	if c.conf.PasswordAuthenticator.Enabled {
		conn.Authenticator = gocql.PasswordAuthenticator{
			Username: c.conf.PasswordAuthenticator.Username,
			Password: c.conf.PasswordAuthenticator.Password,
		}
	}
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
	c.log.Infof("Sending messages to Cassandra: %v\n", c.conf.Nodes)
	return nil
}

// Connected returns true if the connection to Cassanrda is open.
func (c *Cassandra) Connected() bool {
	c.connLock.RLock()
	session := c.session
	c.connLock.RUnlock()

	return session != nil && !session.Closed()
}

// Write writes a message to Cassandra by calling WriteWithContext with a simple Background context.
func (c *Cassandra) Write(msg types.Message) error {
	return c.WriteWithContext(context.Background(), msg)
}

// WriteWithContext writes a message to Cassandra.
func (c *Cassandra) WriteWithContext(ctx context.Context, msg types.Message) error {
	c.connLock.RLock()
	session := c.session
	c.connLock.RUnlock()

	if c.session == nil {
		return types.ErrNotConnected
	}
	if c.conf.AsyncParts {
		var g errgroup.Group
		msg.Iter(func(i int, p types.Part) error {
			g.Go(func() error {
				return c.writePart(session, p)
			})
			return nil
		})
		return g.Wait()
	}

	return msg.Iter(func(i int, p types.Part) error {
		return c.writePart(session, p)
	})
}

func (c *Cassandra) writePart(session *gocql.Session, p types.Part) error {
	t0 := time.Now()
	err := session.Query(c.query, p.Get()).Exec()
	if err != nil {
		return err
	}
	c.mQueryLatency.Timing(time.Since(t0).Nanoseconds())
	return nil
}

// CloseAsync shuts down the Cassandra output and stops processing messages.
func (c *Cassandra) CloseAsync() {
	c.connLock.Lock()
	if c.session != nil {
		c.session.Close()
		c.session = nil
	}
	c.connLock.Unlock()
}

// WaitForClose blocks until the Cassandra output has closed down.
func (c *Cassandra) WaitForClose(timeout time.Duration) error {
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
