// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package questdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func questdbOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Pushes messages to a QuestDB table").
		Description("Important: We recommend that the dedupe feature is enabled on the QuestDB server. "+
			"Please visit https://questdb.io/docs/ for more information about deploying, configuring, and using QuestDB."+
			service.OutputPerformanceDocs(true, true)).
		Categories("Services").
		Fields(
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField("batching"),
			service.NewTLSToggledField("tls"),
			service.NewStringField("address").
				Description("Address of the QuestDB server's HTTP port (excluding protocol)").
				Example("localhost:9000"),
			service.NewStringField("username").
				Description("Username for HTTP basic auth").
				Optional().
				Secret(),
			service.NewStringField("password").
				Description("Password for HTTP basic auth").
				Optional().
				Secret(),
			service.NewStringField("token").
				Description("Bearer token for HTTP auth (takes precedence over basic auth username & password)").
				Optional().
				Secret(),
			service.NewDurationField("retry_timeout").
				Description("The time to continue retrying after a failed HTTP request. The interval between retries is an exponential "+
					"backoff starting at 10ms and doubling after each failed attempt up to a maximum of 1 second.").
				Optional().
				Advanced(),
			service.NewDurationField("request_timeout").
				Description("The time to wait for a response from the server. This is in addition to the calculation "+
					"derived from the request_min_throughput parameter.").
				Optional().
				Advanced(),
			service.NewIntField("request_min_throughput").
				Description("Minimum expected throughput in bytes per second for HTTP requests. If the throughput is lower than this value, "+
					"the connection will time out. This is used to calculate an additional timeout on top of request_timeout. This is useful for large requests. "+
					"You can set this value to 0 to disable this logic.").
				Optional().
				Advanced(),
			service.NewStringField("table").
				Description("Destination table").
				Example("trades"),
			service.NewStringField("designated_timestamp_field").
				Description("Name of the designated timestamp field").
				Optional(),
			service.NewStringField("designated_timestamp_unit").
				Description("Designated timestamp field units").
				Default("auto").
				LintRule(`root = if ["nanos","micros","millis","seconds","auto"].contains(this) != true { [ "valid options are \"nanos\", \"micros\", \"millis\", \"seconds\", \"auto\"" ] }`).
				Optional(),
			service.NewStringListField("timestamp_string_fields").
				Description("String fields with textual timestamps").
				Optional(),
			service.NewStringField("timestamp_string_format").
				Description("Timestamp format, used when parsing timestamp string fields. Specified in golang's time.Parse layout").
				Default(time.StampMicro+"Z0700").
				Optional(),
			service.NewStringListField("symbols").
				Description("Columns that should be the SYMBOL type (string values default to STRING)").
				Optional(),
			service.NewStringListField("doubles").
				Description("Columns that should be double type, (int is default)").
				Optional(),
			service.NewBoolField("error_on_empty_messages").
				Description("Mark a message as errored if it is empty after field validation").
				Optional().
				Default(false),
		)
}

type questdbWriter struct {
	log *service.Logger

	pool      *qdb.LineSenderPool
	transport *http.Transport

	address                  string
	symbols                  map[string]bool
	doubles                  map[string]bool
	table                    string
	designatedTimestampField string
	designatedTimestampUnit  timestampUnit
	timestampStringFormat    string
	timestampStringFields    map[string]bool
	errorOnEmptyMessages     bool
}

func fromConf(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
	if batchPol, err = conf.FieldBatchPolicy("batching"); err != nil {
		return
	}

	if mif, err = conf.FieldMaxInFlight(); err != nil {
		return
	}

	// We force the use of HTTP connections (instead of TCP) and
	// disable the QuestDB LineSender[s] auto flush to force the client
	// to send data over the wire only once, when a MessageBatch has been
	// completely processed.
	opts := []qdb.LineSenderOption{
		qdb.WithHttp(),
		qdb.WithAutoFlushDisabled(),
	}

	// Now, we process options for and construct the LineSenderPool
	// which is used to send data to QuestDB using Influx Line Protocol

	var addr string
	if addr, err = conf.FieldString("address"); err != nil {
		return
	}
	opts = append(opts, qdb.WithAddress(addr))

	if conf.Contains("retry_timeout") {
		var retryTimeout time.Duration
		if retryTimeout, err = conf.FieldDuration("retry_timeout"); err != nil {
			return
		}
		opts = append(opts, qdb.WithRetryTimeout(retryTimeout))
	}

	if conf.Contains("request_timeout") {
		var requestTimeout time.Duration
		if requestTimeout, err = conf.FieldDuration("request_timeout"); err != nil {
			return
		}
		opts = append(opts, qdb.WithRequestTimeout(requestTimeout))
	}

	if conf.Contains("request_min_throughput") {
		var requestMinThroughput int
		if requestMinThroughput, err = conf.FieldInt("request_min_throughput"); err != nil {
			return
		}
		opts = append(opts, qdb.WithMinThroughput(requestMinThroughput))
	}

	if conf.Contains("token") {
		var token string
		if token, err = conf.FieldString("token"); err != nil {
			return
		}
		opts = append(opts, qdb.WithBearerToken(token))
	}

	if conf.Contains("username") && conf.Contains("password") {
		var username, password string
		if username, err = conf.FieldString("username"); err != nil {
			return
		}
		if password, err = conf.FieldString("password"); err != nil {
			return
		}
		opts = append(opts, qdb.WithBasicAuth(username, password))

	}

	// Use a common http transport with user-defined TLS config
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxConnsPerHost:     0,
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     120 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return
	}

	if tlsEnabled {
		opts = append(opts, qdb.WithTls())
		transport.TLSClientConfig = tlsConf
	}

	opts = append(opts, qdb.WithHttpTransport(transport))

	// Allocate the QuestDBWriter which wraps the LineSenderPool
	w := &questdbWriter{
		address:               addr,
		log:                   mgr.Logger(),
		symbols:               map[string]bool{},
		doubles:               map[string]bool{},
		timestampStringFields: map[string]bool{},
		transport:             transport,
	}
	out = w
	w.pool, err = qdb.PoolFromOptions(opts...)
	if err != nil {
		return
	}

	// Apply pool-level options
	// todo: is this the correct interpretation of max-in-flight?
	qdb.WithMaxSenders(mif)(w.pool)

	// Configure the questdbWriter with additional options

	if w.table, err = conf.FieldString("table"); err != nil {
		return
	}

	// Symbols, doubles, and timestampStringFields are stored in maps
	// for fast lookup.
	var symbols []string
	if conf.Contains("symbols") {
		if symbols, err = conf.FieldStringList("symbols"); err != nil {
			return
		}
		for _, s := range symbols {
			w.symbols[s] = true
		}
	}

	var doubles []string
	if conf.Contains("doubles") {
		if doubles, err = conf.FieldStringList("doubles"); err != nil {
			return
		}
		for _, d := range doubles {
			w.doubles[d] = true
		}
	}

	var timestampStringFields []string
	if conf.Contains("timestamp_string_fields") {
		if timestampStringFields, err = conf.FieldStringList("timestamp_string_fields"); err != nil {
			return
		}
		for _, f := range timestampStringFields {
			w.timestampStringFields[f] = true
		}
	}

	if conf.Contains("designated_timestamp_field") {
		if w.designatedTimestampField, err = conf.FieldString("designated_timestamp_field"); err != nil {
			return
		}
	}

	var designatedTimestampUnit string
	if conf.Contains("designated_timestamp_unit") {
		if designatedTimestampUnit, err = conf.FieldString("designated_timestamp_unit"); err != nil {
			return
		}

		// perform validation on timestamp units here in case the user doesn't lint the config
		w.designatedTimestampUnit = timestampUnit(designatedTimestampUnit)
		if !w.designatedTimestampUnit.IsValid() {
			err = fmt.Errorf("%v is not a valid timestamp unit", designatedTimestampUnit)
			return
		}
	}

	if conf.Contains("timestamp_string_format") {
		if w.timestampStringFormat, err = conf.FieldString("timestamp_string_format"); err != nil {
			return
		}
	}

	if w.errorOnEmptyMessages, err = conf.FieldBool("error_on_empty_messages"); err != nil {
		return
	}

	return
}

func (q *questdbWriter) Connect(ctx context.Context) error {
	// No connections are required to initialize a LineSenderPool,
	// so nothing to do here. Each LineSender has its own http client
	// that will use the network only when flushing messages to the server.
	return nil
}

func (q *questdbWriter) parseTimestamp(v any) (time.Time, error) {
	switch val := v.(type) {
	case string:
		t, err := time.Parse(q.timestampStringFormat, val)
		if err != nil {
			q.log.Errorf("could not parse timestamp field %v", err)
		}
		return t, err
	case json.Number:
		intVal, err := val.Int64()
		if err != nil {
			q.log.Errorf("numerical timestamps must be int64: %v", err)
		}
		return q.designatedTimestampUnit.From(intVal), err
	default:
		err := fmt.Errorf("unsupported type %T for designated timestamp: %v", v, v)
		q.log.Error(err.Error())
		return time.Time{}, err
	}
}

func (q *questdbWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) (err error) {
	sender, err := q.pool.Sender(ctx)
	if err != nil {
		return err
	}

	err = batch.WalkWithBatchedErrors(func(i int, m *service.Message) (err error) {
		// QuestDB's LineSender constructs ILP messages using a buffer, so message
		// components must be written in the correct order, otherwise the sender will
		// return an error. This order is:
		// 1. Table Name
		// 2. Symbols (key/value pairs)
		// 3. Columns (key/value pairs)
		// 4. Timestamp [optional]
		//
		// Before writing any column, we call Table(), which is guaranteed to run once.
		// hasTable flag is used for that.
		var hasTable bool

		q.log.Tracef("Writing message %v", i)

		jVal, err := m.AsStructured()
		if err != nil {
			err = fmt.Errorf("unable to parse JSON: %v", err)
			m.SetError(err)
			return err
		}
		jObj, ok := jVal.(map[string]any)
		if !ok {
			err = fmt.Errorf("expected JSON object, found '%T'", jVal)
			m.SetError(err)
			return err
		}

		// Stage 1: Handle all symbols, which must be written to the buffer first
		for s := range q.symbols {
			v, found := jObj[s]
			if found {
				if !hasTable {
					sender.Table(q.table)
					hasTable = true
				}
				switch val := v.(type) {
				case string:
					sender.Symbol(s, val)
				default:
					sender.Symbol(s, fmt.Sprintf("%v", val))
				}
			}
		}

		// Stage 2: Handle columns
		for k, v := range jObj {
			// Skip designated timestamp field (will process this in the 3rd stage)
			if q.designatedTimestampField == k {
				continue
			}

			// Skip symbols (already processed in 1st stage)
			if _, isSymbol := q.symbols[k]; isSymbol {
				continue
			}

			// For all non-timestamp fields, process values by JSON types since we are working
			// with structured messages
			switch val := v.(type) {
			case string:
				// Check if the field is a timestamp and process accordingly
				if _, isTimestampField := q.timestampStringFields[k]; isTimestampField {
					timestamp, err := q.parseTimestamp(v)
					if err == nil {
						if !hasTable {
							sender.Table(q.table)
							hasTable = true
						}
						sender.TimestampColumn(k, timestamp)
					} else {
						q.log.Errorf("%v", err)
					}
					continue
				}

				if !hasTable {
					sender.Table(q.table)
					hasTable = true
				}
				sender.StringColumn(k, val)
			case bool:
				if !hasTable {
					sender.Table(q.table)
					hasTable = true
				}
				sender.BoolColumn(k, val)
			case json.Number:
				// For json numbers, assume int unless column is explicitly marked as a double
				if _, isDouble := q.doubles[k]; isDouble {
					floatVal, err := val.Float64()
					if err != nil {
						q.log.Errorf("could not parse %v into a double: %v", val, err)
					}

					if !hasTable {
						sender.Table(q.table)
						hasTable = true
					}
					sender.Float64Column(k, floatVal)
				} else {
					intVal, err := val.Int64()
					if err != nil {
						q.log.Errorf("could not parse %v into an integer: %v", val, err)
					}

					if !hasTable {
						sender.Table(q.table)
						hasTable = true
					}
					sender.Int64Column(k, intVal)
				}
			case float64:
				// float64 is only needed if BENTHOS_USE_NUMBER=false
				if !hasTable {
					sender.Table(q.table)
					hasTable = true
				}
				sender.Float64Column(k, float64(val))
			default:
				q.log.Errorf("unsupported type %T for field %v", v, k)
			}
		}

		// Stage 3: Handle designated timestamp and finalize the buffered message
		var designatedTimestamp time.Time
		if q.designatedTimestampField != "" {
			val, found := jObj[q.designatedTimestampField]
			if found {
				designatedTimestamp, err = q.parseTimestamp(val)
				if err != nil {
					q.log.Errorf("unable to parse designated timestamp: %v", val)
				}
			}
		}

		if !hasTable {
			if q.errorOnEmptyMessages {
				err = errors.New("empty message, skipping send to QuestDB")
				m.SetError(err)
				return err
			}
			q.log.Warn("empty message, skipping send to QuestDB")
			return nil
		}

		if !designatedTimestamp.IsZero() {
			err = sender.At(ctx, designatedTimestamp)
		} else {
			err = sender.AtNow(ctx)
		}

		if err != nil {
			m.SetError(err)
		}
		return err
	})

	// This will flush the sender, no need to call sender.Flush at the end of the method
	releaseErr := sender.Close(ctx)
	if releaseErr != nil {
		if err != nil {
			err = fmt.Errorf("%v %w", err, releaseErr)
		} else {
			err = releaseErr
		}
	}

	return err
}

func (q *questdbWriter) Close(ctx context.Context) error {
	return q.pool.Close(ctx)
}

func init() {
	service.MustRegisterBatchOutput(
		"questdb",
		questdbOutputConfig(),
		fromConf,
	)
}
