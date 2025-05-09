package driver

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	protonDriver "github.com/timeplus-io/proton-go-driver/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type driver struct {
	logger      *service.Logger
	conn        *sql.DB
	rows        *sql.Rows
	columnTypes []*sql.ColumnType

	ctx    context.Context
	cancel context.CancelFunc
}

var (
	codeRe = *regexp.MustCompile(`code: (.+[0-9])`)
	msgRe  = *regexp.MustCompile(`message: (.*)`)
)

// NewDriver creates a new proton driver.
func NewDriver(logger *service.Logger, addr, username, password string) *driver {
	conn := protonDriver.OpenDB(&protonDriver.Options{
		Addr: []string{addr},
		Auth: protonDriver.Auth{
			Username: username,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})

	return &driver{
		logger: logger,
		conn:   conn,
	}
}

// Run starts a query.
func (d *driver) Run(sql string) error {
	d.ctx, d.cancel = context.WithCancel(context.Background())
	ckCtx := protonDriver.Context(d.ctx)

	rows, err := d.conn.QueryContext(ckCtx, sql)
	if err != nil {
		return err
	}

	if err := rows.Err(); err != nil {
		return err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	d.rows = rows
	d.columnTypes = columnTypes

	return nil
}

// Read reads one row.
func (d *driver) Read(ctx context.Context) (map[string]any, error) {
	for { // retry loop
		if d.rows.Next() {
			count := len(d.columnTypes)

			values := make([]any, count)
			valuePtrs := make([]any, count)

			for i := range d.columnTypes {
				valuePtrs[i] = &values[i]
			}

			if err := d.rows.Scan(valuePtrs...); err != nil {
				return nil, err
			}

			event := make(map[string]any)
			for i, col := range d.columnTypes {
				event[col.Name()] = values[i]
			}

			return event, nil
		}

		if err := d.rows.Err(); err != nil {
			if isQueryCancelErr(err) {
				// Most likely timeplusd got restarted. Since we are going to re-connect to timeplusd once it recovered, we do not log it as error for now.
				d.logger.With("reason", err).Info("query cancelled")
				return nil, io.EOF
			}
			if errors.Is(err, context.Canceled) {
				return nil, err
			}

			d.logger.With("error", err).Errorf("query failed: %s", err.Error())
			// this happens when the SQL is updated, i.e. a new MV is created, the previous checkpoint is on longer available.
			if strings.Contains(err.Error(), "code: 2003") {
				continue // retry
			}
			return nil, err
		}

		return nil, io.EOF
	}
}

// Close terminates the running query.
func (d *driver) Close(context.Context) error {
	d.cancel()

	if err := d.rows.Close(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	if err := d.rows.Err(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	return d.conn.Close()
}

func isQueryCancelErr(err error) bool {
	code, msg := parse(err)
	return code == 394 && strings.Contains(msg, "Query was cancelled")
}

func parse(err error) (int, string) {
	var code int
	var msg string

	errStr := err.Error()
	codeMatches := codeRe.FindStringSubmatch(errStr)
	if len(codeMatches) == 2 {
		code, _ = strconv.Atoi(codeMatches[1])
	}

	msgMatches := msgRe.FindStringSubmatch(errStr)
	if len(msgMatches) == 2 {
		msg = msgMatches[1]
	}

	return code, msg
}
