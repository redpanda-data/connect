package crdb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func crdbChangefeedInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Integration").
		Summary("Listens to a CockroachDB Core Changefeed and creates a message for each row received").
		Description("Will continue to listen to the Changefeed until shutdown. If provided, will maintain the previous `timestamp` so that in the event of a restart the chanfeed will resume from where it last processed. This is at-least-once processing, as there is a chance that a timestamp is not successfully stored after being emitted and therefore a row may be sent again on restart.\n\nNote: You must have `SET CLUSTER SETTING kv.rangefeed.enabled = true;` on your CRDB cluster").
		Field(service.NewStringField("dsn").
			Description(`A Data Source Name to identify the target database.`).
			Example("postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable")).
		Field(service.NewStringField("root_ca").
			Description(`A Root CA Certificate`).
			Example(`-----BEGIN CERTIFICATE-----
FEIWJFOIEW...
-----END CERTIFICATE-----`).
			Optional()).
		Field(service.NewStringListField("tables").
			Description("CSV of tables to be included in the changefeed").
			Example([]string{"table1", "table2"})).
		Field(service.NewStringListField("options").
			Description("CSV of options to be included in the changefeed (WITH X, Y...).\n**NOTE: The CURSOR option here will only be applied if a cached cursor value is not found. If the cursor is cached, it will overwrite this option.**").
			Example([]string{"UPDATE", "CURSOR=1536242855577149065.0000000000"}).
			Optional())
}

type crdbChangefeedInput struct {
	dsn        string
	ctx        context.Context
	cancelFunc context.CancelFunc
	pgConfig   *pgxpool.Config
	pgPool     *pgxpool.Pool
	rootCA     string
	statement  string

	rows pgx.Rows

	tables []string
	optins []string

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newCRDBChangefeedInputFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*crdbChangefeedInput, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	c := &crdbChangefeedInput{
		logger:     logger,
		shutSig:    shutdown.NewSignaller(),
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}

	var err error
	c.dsn, err = conf.FieldString("dsn")
	if err != nil {
		return nil, err
	}
	c.pgConfig, err = pgxpool.ParseConfig(c.dsn)
	if err != nil {
		return nil, err
	}

	// Setup the cert if given
	if c.rootCA, err = conf.FieldString("root_ca"); err != nil {
		if c.rootCA != "" {
			certPool := x509.NewCertPool()
			rootCert, err := x509.ParseCertificate([]byte(c.rootCA))
			if err != nil {
				return nil, err
			}
			certPool.AddCert(rootCert)
			c.pgConfig.ConnConfig.TLSConfig = &tls.Config{
				RootCAs: certPool,
			}
		}
	}

	// Setup the query
	tables, err := conf.FieldStringList("tables")
	if err != nil {
		return nil, err
	}

	options, err := conf.FieldStringList("options")
	if err != nil {
		return nil, err
	}

	changeFeedOptions := ""
	if len(options) > 0 {
		changeFeedOptions = fmt.Sprintf(" WITH %s", strings.Join(options, ", "))
	}

	c.statement = fmt.Sprintf("EXPERIMENTAL CHANGEFEED FOR %s%s", strings.Join(tables, ", "), changeFeedOptions)
	logger.Debug("Creating changefeed: " + c.statement)

	return c, nil
}

func init() {
	err := service.RegisterInput(
		"crdb_changefeed", crdbChangefeedInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newCRDBChangefeedInputFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			// return service.AutoRetryNacks(i), nil
			return i, nil
		})

	if err != nil {
		panic(err)
	}
}

func (c *crdbChangefeedInput) Connect(ctx context.Context) (err error) {
	// Connect to the pool
	c.pgPool, err = pgxpool.ConnectConfig(c.ctx, c.pgConfig)
	if err != nil {
		return err
	}
	return err
}

func (c *crdbChangefeedInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if c.pgPool == nil && c.rows == nil {
		return nil, nil, service.ErrNotConnected
	}

	rowChan := make(chan *service.Message)
	readyChan := make(chan struct{})
	errChan := make(chan error)

	if c.rows == nil {
		// Put this in a goroutine because .Query() will block until there is a row available to read
		go func() {
			var err error
			c.logger.Debug(fmt.Sprintf("Running query '%s'", c.statement))
			c.rows, err = c.pgPool.Query(c.ctx, c.statement)
			if err != nil {
				c.logger.Error("Error querying")
				errChan <- err
			}
			// Check if the context was cancelled
			select {
			case <-c.ctx.Done():
				c.logger.Debug("Context was cancelled before query go a first row")
				c.rows.Close() // Need to close here, c.rows is nil in Close()
				c.logger.Debug("Closed rows")
			default:
				// We found a row and can keep going
				readyChan <- struct{}{}
			}
		}()

		c.logger.Debug("Waiting for first row to be available")
		select {
		case <-readyChan:
			// We	got the first line, go read it
			break
		case err := <-errChan:
			return nil, nil, err
		case <-ctx.Done():
			c.logger.Debug("Read context cancelled before first row, ending")
			return nil, nil, service.ErrEndOfInput
		}
	}

	// Put this in a goroutine because .Read() will block until there is a row available to read
	go func() {
		c.logger.Debug("Checking for available rows")
		if c.rows.Next() {
			values, err := c.rows.Values()
			if err != nil {
				c.logger.Error("Error reading row values!")
				errChan <- err
			}
			// Construct the new JSON
			var jsonBytes []byte
			jsonBytes, err = json.Marshal(map[string]string{
				"table":       values[0].(string),
				"primary_key": string(values[1].([]byte)), // Stringified JSON (Array)
				"row":         string(values[2].([]byte)), // Stringified JSON (Object)
			})
			// TODO: Store the current time for the CURSOR offset to cache
			if err != nil {
				c.logger.Error("Failed to marshal JSON")
				errChan <- err
			}
			msg := service.NewMessage(jsonBytes)
			rowChan <- msg
		}
	}()

	select {
	case msg := <-rowChan:
		return msg, func(ctx context.Context, err error) error {
			// No acking in core changefeeds
			return nil
		}, nil
	case err := <-errChan:
		return nil, nil, err
	case <-ctx.Done():
		c.logger.Debug("Read context cancelled, ending")
		return nil, nil, service.ErrEndOfInput
	}
}

func (c *crdbChangefeedInput) Close(ctx context.Context) error {
	c.logger.Debug("Got close signal")
	c.cancelFunc()
	c.pgPool.Close()
	c.shutSig.ShutdownComplete()
	select {
	case <-c.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
