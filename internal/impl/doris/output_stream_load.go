// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package doris

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/rs/xid"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/httpclient"
)

const (
	dsFieldURL              = "url"
	dsFieldFEURLs           = "fe_urls"
	dsFieldDatabase         = "database"
	dsFieldTable            = "table"
	dsFieldUsername         = "username"
	dsFieldPassword         = "password"
	dsFieldQueryPort        = "query_port"
	dsFieldFormat           = "format"
	dsFieldReadJSONByLine   = "read_json_by_line"
	dsFieldStripOuterArray  = "strip_outer_array"
	dsFieldJSONPaths        = "jsonpaths"
	dsFieldJSONRoot         = "json_root"
	dsFieldColumns          = "columns"
	dsFieldWhere            = "where"
	dsFieldColumnSeparator  = "column_separator"
	dsFieldLineDelimiter    = "line_delimiter"
	dsFieldGroupCommit      = "group_commit"
	dsFieldMaxFilterRatio   = "max_filter_ratio"
	dsFieldPartitions       = "partitions"
	dsFieldTempPartitions   = "temporary_partitions"
	dsFieldSkipLines        = "skip_lines"
	dsFieldEmptyFieldAsNull = "empty_field_as_null"
	dsFieldTrimDoubleQuotes = "trim_double_quotes"
	dsFieldNumAsString      = "num_as_string"
	dsFieldFuzzyParse       = "fuzzy_parse"
	dsFieldLabelPrefix      = "label_prefix"
	dsFieldHeaders          = "headers"
	dsFieldStrictMode       = "strict_mode"
	dsFieldTimeout          = "timeout"
	dsFieldBatching         = "batching"
	dsMetaLabel             = "doris_stream_load_label"
	dsStatusSuccess         = "Success"
	dsStatusPublishTimeout  = "Publish Timeout"
	dsStatusLabelExists     = "Label Already Exists"
	dsExistingJobFinished   = "FINISHED"
	dsDefaultLabelPrefix    = "redpanda_connect"
	dsDefaultMaxInFlight    = 8
	dsDefaultTimeout        = 30 * time.Second
	dsDefaultColumnSep      = ","
	dsDefaultLineDelimiter  = "\n"
	dsDefaultQueryPort      = 9030
	dsHeaderLocation        = "Location"
	dsHeaderLabel           = "label"
	dsHeaderFormat          = "format"
	dsHeaderColumns         = "columns"
	dsHeaderWhere           = "where"
	dsHeaderColumnSeparator = "column_separator"
	dsHeaderLineDelimiter   = "line_delimiter"
	dsHeaderGroupCommit     = "group_commit"
	dsHeaderMaxFilterRatio  = "max_filter_ratio"
	dsHeaderPartitions      = "partitions"
	dsHeaderTempPartitions  = "temporary_partitions"
	dsHeaderSkipLines       = "skip_lines"
	dsHeaderEmptyAsNull     = "empty_field_as_null"
	dsHeaderTrimQuotes      = "trim_double_quotes"
	dsHeaderReadJSONByLine  = "read_json_by_line"
	dsHeaderStripOuterArray = "strip_outer_array"
	dsHeaderJSONPaths       = "jsonpaths"
	dsHeaderJSONRoot        = "json_root"
	dsHeaderNumAsString     = "num_as_string"
	dsHeaderFuzzyParse      = "fuzzy_parse"
	dsHeaderStrictMode      = "strict_mode"
	dsHeaderTimeout         = "timeout"
)

var dsLabelSanitizer = regexp.MustCompile(`[^-_A-Za-z0-9:]`)
var dsHeaderValueEscaper = strings.NewReplacer(
	"\n", "\\n",
	"\r", "\\r",
	"\t", "\\t",
)

type dorisStreamLoadConfig struct {
	URL              string
	FEURLs           []string
	Database         string
	Table            string
	Username         string
	Password         string
	QueryPort        int
	Format           string
	ReadJSONByLine   bool
	StripOuterArray  bool
	JSONPaths        string
	JSONRoot         string
	Columns          []string
	Where            string
	ColumnSeparator  string
	LineDelimiter    string
	GroupCommit      string
	MaxFilterRatio   *float64
	Partitions       []string
	TempPartitions   []string
	SkipLines        *int
	EmptyAsNull      bool
	TrimDoubleQuotes bool
	NumAsString      bool
	FuzzyParse       bool
	LabelPrefix      string
	Headers          map[string]string
	StrictMode       bool
	Timeout          time.Duration
}

type dorisStreamLoadOutput struct {
	conf      dorisStreamLoadConfig
	client    *http.Client
	endpoints []*url.URL
	nextFE    atomic.Uint64
	log       *service.Logger

	connectionCheckFn func(context.Context, string, int) error
}

type dorisStreamLoadResponse struct {
	Status             string `json:"Status"`
	Message            string `json:"Message"`
	Msg                string `json:"msg"`
	FirstErrorMsg      string `json:"FirstErrorMsg"`
	ExistingJobStatus  string `json:"ExistingJobStatus"`
	NumberTotalRows    int64  `json:"NumberTotalRows"`
	NumberLoadedRows   int64  `json:"NumberLoadedRows"`
	NumberFilteredRows int64  `json:"NumberFilteredRows"`
	ErrorURL           string `json:"ErrorURL"`
}

func dorisStreamLoadSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.86.0").
		Summary("Writes batches of messages into Apache Doris using Stream Load.").
		Description(`Each output batch is encoded into a single Doris Stream Load request. The sink first contacts FE and follows the Stream Load redirect to BE before uploading the batch body. The batch is only acknowledged when Doris reports success.`).
		Field(service.NewStringField(dsFieldURL).
			Description("Backward-compatible single Doris FE HTTP URL, for example http://fe_host:8030. When fe_urls is provided it takes precedence.").
			Example("http://127.0.0.1:8030").
			Optional().
			Advanced()).
		Field(service.NewStringListField(dsFieldFEURLs).
			Description("A list of Doris FE HTTP URLs. The sink will try these FE endpoints in order with per-request failover, and the starting FE is rotated across requests.").
			Example([]string{"http://fe1:8030", "http://fe2:8030"}).
			Default([]any{}).
			Optional()).
		Field(service.NewStringField(dsFieldDatabase).
			Description("Target Doris database.").
			Example("test_db")).
		Field(service.NewStringField(dsFieldTable).
			Description("Target Doris table.").
			Example("events")).
		Field(service.NewStringField(dsFieldUsername).
			Description("Doris username.").
			Example("root")).
		Field(service.NewStringField(dsFieldPassword).
			Description("Doris password.").
			Secret()).
		Field(service.NewIntField(dsFieldQueryPort).
			Description("Doris FE MySQL query port, used by ConnectionTest to verify that the target database and table exist. Set to 0 to disable query-port checks.").
			Default(dsDefaultQueryPort).
			Advanced()).
		Field(service.NewStringEnumField(dsFieldFormat, "json", "csv").
			Description("Body format sent to Doris Stream Load.").
			Default("json")).
		Field(service.NewBoolField(dsFieldReadJSONByLine).
			Description("Encode a JSON batch as newline-delimited JSON and set the Doris header read_json_by_line=true.").
			Default(true)).
		Field(service.NewBoolField(dsFieldStripOuterArray).
			Description("Encode a JSON batch as a single JSON array and set the Doris header strip_outer_array=true.").
			Default(false)).
		Field(service.NewStringField(dsFieldJSONPaths).
			Description("Optional Doris jsonpaths header value.").
			Default("").
			Optional().
			Advanced()).
		Field(service.NewStringField(dsFieldJSONRoot).
			Description("Optional Doris json_root header value.").
			Default("").
			Optional().
			Advanced()).
		Field(service.NewStringListField(dsFieldColumns).
			Description("Optional Doris columns header. When set the values are joined with commas.").
			Default([]any{}).
			Optional()).
		Field(service.NewStringField(dsFieldWhere).
			Description("Optional Doris where header for filtering imported rows.").
			Default("").
			Optional().
			Advanced()).
		Field(service.NewStringField(dsFieldColumnSeparator).
			Description("Column separator used for csv format.").
			Default(dsDefaultColumnSep).
			Advanced()).
		Field(service.NewStringField(dsFieldLineDelimiter).
			Description("Line delimiter used for csv batches.").
			Default(dsDefaultLineDelimiter).
			Advanced()).
		Field(service.NewStringField(dsFieldGroupCommit).
			Description("Optional Doris group_commit mode. Valid values are sync_mode, async_mode, and off_mode. When omitted Doris uses the server default behavior.").
			Default("").
			Optional().
			Advanced()).
		Field(service.NewFloatField(dsFieldMaxFilterRatio).
			Description("Optional Doris max_filter_ratio header value.").
			Default(0.0).
			Optional().
			Advanced()).
		Field(service.NewStringListField(dsFieldPartitions).
			Description("Optional Doris partitions header. Values are joined with commas.").
			Default([]any{}).
			Optional().
			Advanced()).
		Field(service.NewStringListField(dsFieldTempPartitions).
			Description("Optional Doris temporary_partitions header. Values are joined with commas.").
			Default([]any{}).
			Optional().
			Advanced()).
		Field(service.NewIntField(dsFieldSkipLines).
			Description("Optional Doris skip_lines header.").
			Default(0).
			Optional().
			Advanced()).
		Field(service.NewBoolField(dsFieldEmptyFieldAsNull).
			Description("Whether Doris should treat empty input fields as NULL.").
			Default(false).
			Advanced()).
		Field(service.NewBoolField(dsFieldTrimDoubleQuotes).
			Description("Whether Doris should trim double quotes in CSV/text imports.").
			Default(false).
			Advanced()).
		Field(service.NewBoolField(dsFieldNumAsString).
			Description("Whether Doris should treat JSON numbers as strings.").
			Default(false).
			Advanced()).
		Field(service.NewBoolField(dsFieldFuzzyParse).
			Description("Whether Doris should enable fuzzy_parse for JSON inputs.").
			Default(false).
			Advanced()).
		Field(service.NewStringField(dsFieldLabelPrefix).
			Description("Prefix used when generating Doris labels.").
			Default(dsDefaultLabelPrefix)).
		Field(service.NewStringMapField(dsFieldHeaders).
			Description("Optional additional static Stream Load headers. Reserved Doris headers configured by this component take precedence.").
			Default(map[string]any{}).
			Advanced()).
		Field(service.NewBoolField(dsFieldStrictMode).
			Description("Whether to set Doris strict_mode=true.").
			Default(false).
			Advanced()).
		Field(service.NewDurationField(dsFieldTimeout).
			Description("Timeout for each Doris HTTP request.").
			Default(dsDefaultTimeout.String())).
		Field(service.NewOutputMaxInFlightField().
			Description("Maximum number of parallel in-flight Doris Stream Load requests.").
			Default(dsDefaultMaxInFlight)).
		Field(service.NewBatchPolicyField(dsFieldBatching)).
		Example("JSON lines from stdin", "Read newline-delimited JSON messages and write them into Doris using one Stream Load request per batch.", `
input:
  stdin:
    scanner:
      lines: {}

output:
  doris_stream_load:
    url: http://127.0.0.1:8030
    database: test_db
    table: events
    username: root
    password: secret
    format: json
    read_json_by_line: true
    columns: [id, name, ts]
`).
		LintRule(`
root = []
if this.format == "json" {
  if this.read_json_by_line == true && this.strip_outer_array == true {
    root = root.append("read_json_by_line and strip_outer_array cannot both be true")
  }
  if this.read_json_by_line == false && this.strip_outer_array == false {
    root = root.append("json format requires either read_json_by_line or strip_outer_array to be true")
  }
}
if this.format == "csv" {
  if this.column_separator == "" {
    root = root.append("column_separator cannot be empty when format=csv")
  }
  if this.line_delimiter == "" {
    root = root.append("line_delimiter cannot be empty when format=csv")
  }
}
if (this.url.or("").string() == "" && this.fe_urls.length() == 0) {
  root = root.append("either url or fe_urls must be configured")
}
if this.partitions.length() > 0 && this.temporary_partitions.length() > 0 {
  root = root.append("partitions and temporary_partitions cannot both be set")
}
if this.max_filter_ratio < 0 || this.max_filter_ratio > 1 {
  root = root.append("max_filter_ratio must be between 0 and 1")
}
if this.group_commit != "" && this.group_commit != "sync_mode" && this.group_commit != "async_mode" && this.group_commit != "off_mode" {
  root = root.append("group_commit must be one of sync_mode, async_mode, off_mode")
}
`).
		Description(service.OutputPerformanceDocs(true, true))
}

func init() {
	service.MustRegisterBatchOutput(
		"doris_stream_load",
		dorisStreamLoadSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			var parsed dorisStreamLoadConfig
			if parsed, err = dorisStreamLoadConfigFromParsed(conf); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(dsFieldBatching); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newDorisStreamLoadOutput(parsed, mgr)
			return
		},
	)
}

func dorisStreamLoadConfigFromParsed(conf *service.ParsedConfig) (c dorisStreamLoadConfig, err error) {
	if conf.Contains(dsFieldURL) {
		if c.URL, err = conf.FieldString(dsFieldURL); err != nil {
			return
		}
	}
	if c.FEURLs, err = conf.FieldStringList(dsFieldFEURLs); err != nil {
		return
	}
	if c.Database, err = conf.FieldString(dsFieldDatabase); err != nil {
		return
	}
	if c.Table, err = conf.FieldString(dsFieldTable); err != nil {
		return
	}
	if c.Username, err = conf.FieldString(dsFieldUsername); err != nil {
		return
	}
	if c.Password, err = conf.FieldString(dsFieldPassword); err != nil {
		return
	}
	if c.QueryPort, err = conf.FieldInt(dsFieldQueryPort); err != nil {
		return
	}
	if c.Format, err = conf.FieldString(dsFieldFormat); err != nil {
		return
	}
	if c.ReadJSONByLine, err = conf.FieldBool(dsFieldReadJSONByLine); err != nil {
		return
	}
	if c.StripOuterArray, err = conf.FieldBool(dsFieldStripOuterArray); err != nil {
		return
	}
	if c.JSONPaths, err = conf.FieldString(dsFieldJSONPaths); err != nil {
		return
	}
	if c.JSONRoot, err = conf.FieldString(dsFieldJSONRoot); err != nil {
		return
	}
	if c.Columns, err = conf.FieldStringList(dsFieldColumns); err != nil {
		return
	}
	if c.Where, err = conf.FieldString(dsFieldWhere); err != nil {
		return
	}
	if c.ColumnSeparator, err = conf.FieldString(dsFieldColumnSeparator); err != nil {
		return
	}
	if c.LineDelimiter, err = conf.FieldString(dsFieldLineDelimiter); err != nil {
		return
	}
	if c.LabelPrefix, err = conf.FieldString(dsFieldLabelPrefix); err != nil {
		return
	}
	if c.GroupCommit, err = conf.FieldString(dsFieldGroupCommit); err != nil {
		return
	}
	if c.Headers, err = conf.FieldStringMap(dsFieldHeaders); err != nil {
		return
	}
	if conf.Contains(dsFieldMaxFilterRatio) {
		var tmp float64
		if tmp, err = conf.FieldFloat(dsFieldMaxFilterRatio); err != nil {
			return
		}
		c.MaxFilterRatio = &tmp
	}
	if c.Partitions, err = conf.FieldStringList(dsFieldPartitions); err != nil {
		return
	}
	if c.TempPartitions, err = conf.FieldStringList(dsFieldTempPartitions); err != nil {
		return
	}
	if conf.Contains(dsFieldSkipLines) {
		var tmp int
		if tmp, err = conf.FieldInt(dsFieldSkipLines); err != nil {
			return
		}
		c.SkipLines = &tmp
	}
	if c.EmptyAsNull, err = conf.FieldBool(dsFieldEmptyFieldAsNull); err != nil {
		return
	}
	if c.TrimDoubleQuotes, err = conf.FieldBool(dsFieldTrimDoubleQuotes); err != nil {
		return
	}
	if c.NumAsString, err = conf.FieldBool(dsFieldNumAsString); err != nil {
		return
	}
	if c.FuzzyParse, err = conf.FieldBool(dsFieldFuzzyParse); err != nil {
		return
	}
	if c.StrictMode, err = conf.FieldBool(dsFieldStrictMode); err != nil {
		return
	}
	if c.Timeout, err = conf.FieldDuration(dsFieldTimeout); err != nil {
		return
	}

	if len(c.FEURLs) == 0 {
		if c.URL == "" {
			err = errors.New("either url or fe_urls must be configured")
			return
		}
		c.FEURLs = []string{c.URL}
	} else if c.URL != "" {
		// Keep backwards compatibility but prefer the explicit FE list.
		c.FEURLs = append([]string(nil), c.FEURLs...)
	}
	for i, feURL := range c.FEURLs {
		var parsedURL *url.URL
		if parsedURL, err = url.Parse(feURL); err != nil {
			err = fmt.Errorf("parsing fe_urls[%d]: %w", i, err)
			return
		}
		if parsedURL.Scheme == "" || parsedURL.Host == "" {
			err = fmt.Errorf("fe_urls[%d] must include scheme and host", i)
			return
		}
	}
	if c.Database == "" {
		err = errors.New("database must not be empty")
		return
	}
	if c.Table == "" {
		err = errors.New("table must not be empty")
		return
	}
	if c.Username == "" {
		err = errors.New("username must not be empty")
		return
	}
	if c.LabelPrefix == "" {
		c.LabelPrefix = dsDefaultLabelPrefix
	}
	if c.Format == "csv" {
		if c.ColumnSeparator == "" {
			err = errors.New("column_separator must not be empty when format=csv")
			return
		}
		if c.LineDelimiter == "" {
			err = errors.New("line_delimiter must not be empty when format=csv")
			return
		}
	}
	if len(c.Partitions) > 0 && len(c.TempPartitions) > 0 {
		err = errors.New("partitions and temporary_partitions cannot both be configured")
		return
	}
	if c.MaxFilterRatio != nil && (*c.MaxFilterRatio < 0 || *c.MaxFilterRatio > 1) {
		err = errors.New("max_filter_ratio must be between 0 and 1")
		return
	}
	return
}

func newDorisStreamLoadOutput(conf dorisStreamLoadConfig, mgr *service.Resources) (*dorisStreamLoadOutput, error) {
	if len(conf.FEURLs) == 0 && conf.URL != "" {
		conf.FEURLs = []string{conf.URL}
	}
	if len(conf.FEURLs) == 0 {
		return nil, errors.New("no Doris FE endpoints configured")
	}

	cfg := httpclient.Config{
		BaseURL:                conf.FEURLs[0],
		Timeout:                conf.Timeout,
		Transport:              httpclient.DefaultTransportConfig(),
		BackoffInitialInterval: 500 * time.Millisecond,
		BackoffMaxInterval:     30 * time.Second,
		BackoffMaxRetries:      3,
		Retry:                  httpclient.DefaultRetryConfig(),
		MetricPrefix:           "doris_stream_load_http",
	}
	cfg.AuthSigner = httpclient.BasicAuthSigner(conf.Username, conf.Password)

	if strings.HasPrefix(conf.FEURLs[0], "https://") {
		cfg.TLSEnabled = true
	}

	client, err := httpclient.NewClient(cfg, mgr)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client: %w", err)
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	endpoints, err := streamLoadURLs(conf.FEURLs, conf.Database, conf.Table)
	if err != nil {
		return nil, err
	}

	return &dorisStreamLoadOutput{
		conf:      conf,
		client:    client,
		endpoints: endpoints,
		log:       mgr.Logger(),
	}, nil
}

func (d *dorisStreamLoadOutput) Connect(context.Context) error {
	return nil
}

func (d *dorisStreamLoadOutput) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	var lastErr error
	for _, feURL := range d.conf.FEURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, feURL, nil)
		if err != nil {
			lastErr = fmt.Errorf("creating FE reachability request: %w", err)
			continue
		}
		resp, err := d.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("connecting to Doris FE %s: %w", feURL, err)
			continue
		}
		resp.Body.Close()
		if d.conf.QueryPort > 0 {
			if err := d.connectionCheck(ctx, feURL, d.conf.QueryPort); err != nil {
				lastErr = err
				continue
			}
		}
		return service.ConnectionTestSucceeded().AsList()
	}
	if lastErr == nil {
		lastErr = errors.New("no Doris FE endpoints configured")
	}
	return service.ConnectionTestFailed(lastErr).AsList()
}

func (d *dorisStreamLoadOutput) connectionCheck(ctx context.Context, feURL string, queryPort int) error {
	if d.connectionCheckFn != nil {
		return d.connectionCheckFn(ctx, feURL, queryPort)
	}

	parsed, err := url.Parse(feURL)
	if err != nil {
		return fmt.Errorf("parsing Doris FE URL %q for query_port checks: %w", feURL, err)
	}
	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("unable to determine host from Doris FE URL %q for query_port checks", feURL)
	}

	cfg := mysqlDriver.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(host, strconv.Itoa(queryPort))
	cfg.User = d.conf.Username
	cfg.Passwd = d.conf.Password
	cfg.DBName = "information_schema"
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("opening Doris query_port connection to %s:%d: %w", host, queryPort, err)
	}
	defer db.Close()

	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(qctx); err != nil {
		return fmt.Errorf("pinging Doris query_port %s:%d: %w", host, queryPort, err)
	}

	var dbCount int
	if err := db.QueryRowContext(
		qctx,
		"SELECT COUNT(*) FROM schemata WHERE schema_name = ?",
		d.conf.Database,
	).Scan(&dbCount); err != nil {
		return fmt.Errorf("checking Doris database %s via %s:%d: %w", d.conf.Database, host, queryPort, err)
	}
	if dbCount == 0 {
		return fmt.Errorf("Doris database %s was not found via query_port %s:%d", d.conf.Database, host, queryPort)
	}

	var tableCount int
	if err := db.QueryRowContext(
		qctx,
		"SELECT COUNT(*) FROM tables WHERE table_schema = ? AND table_name = ?",
		d.conf.Database,
		d.conf.Table,
	).Scan(&tableCount); err != nil {
		return fmt.Errorf("checking Doris table %s.%s via %s:%d: %w", d.conf.Database, d.conf.Table, host, queryPort, err)
	}
	if tableCount == 0 {
		return fmt.Errorf("Doris table %s.%s was not found via query_port %s:%d", d.conf.Database, d.conf.Table, host, queryPort)
	}

	return nil
}

func (d *dorisStreamLoadOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	label := d.getOrCreateLabel(batch)
	body, err := d.encodeBatch(batch)
	if err != nil {
		return err
	}

	resp, err := d.sendViaFE(ctx, label, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	loadResp, rawBody, err := parseDorisStreamLoadResponse(resp)
	if err != nil {
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("Doris returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(rawBody)))
		}
		return fmt.Errorf("parsing Doris response: %w", err)
	}
	if err := classifyDorisStreamLoadResponse(resp.StatusCode, loadResp, rawBody); err != nil {
		return err
	}

	d.log.Infof(
		"Doris stream load success label=%s table=%s.%s status=%s loaded_rows=%d filtered_rows=%d total_rows=%d",
		label, d.conf.Database, d.conf.Table, loadResp.Status, loadResp.NumberLoadedRows, loadResp.NumberFilteredRows, loadResp.NumberTotalRows,
	)
	return nil
}

func (d *dorisStreamLoadOutput) Close(context.Context) error {
	return nil
}

func (d *dorisStreamLoadOutput) encodeBatch(batch service.MessageBatch) ([]byte, error) {
	switch d.conf.Format {
	case "json":
		return encodeBatchAsJSON(batch, d.conf.ReadJSONByLine, d.conf.StripOuterArray)
	case "csv":
		return encodeBatchAsCSV(batch, d.conf.LineDelimiter)
	default:
		return nil, fmt.Errorf("unsupported format: %s", d.conf.Format)
	}
}

func encodeBatchAsJSON(batch service.MessageBatch, readByLine, stripOuterArray bool) ([]byte, error) {
	if readByLine && stripOuterArray {
		return nil, errors.New("read_json_by_line and strip_outer_array cannot both be true")
	}
	if !readByLine && !stripOuterArray {
		return nil, errors.New("json batches require either read_json_by_line or strip_outer_array to be true")
	}

	docs := make([][]byte, 0, len(batch))
	for i, msg := range batch {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("reading message %d bytes: %w", i, err)
		}
		if !json.Valid(b) {
			return nil, fmt.Errorf("message %d is not valid JSON", i)
		}
		docs = append(docs, b)
	}

	if readByLine {
		return bytes.Join(docs, []byte("\n")), nil
	}

	items := make([]json.RawMessage, 0, len(docs))
	for _, doc := range docs {
		items = append(items, json.RawMessage(doc))
	}
	body, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("marshaling JSON array body: %w", err)
	}
	return body, nil
}

func encodeBatchAsCSV(batch service.MessageBatch, lineDelimiter string) ([]byte, error) {
	if lineDelimiter == "" {
		return nil, errors.New("line_delimiter cannot be empty")
	}
	rows := make([][]byte, 0, len(batch))
	for i, msg := range batch {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("reading message %d bytes: %w", i, err)
		}
		rows = append(rows, b)
	}
	return bytes.Join(rows, []byte(lineDelimiter)), nil
}

func (d *dorisStreamLoadOutput) sendViaFE(ctx context.Context, label string, body []byte) (*http.Response, error) {
	var lastErr error
	if len(d.endpoints) == 0 {
		return nil, errors.New("no Doris FE endpoints configured")
	}

	start := int(d.nextFE.Add(1)-1) % len(d.endpoints)
	for i := 0; i < len(d.endpoints); i++ {
		feEndpoint := d.endpoints[(start+i)%len(d.endpoints)]
		feReq, err := d.newRequest(ctx, feEndpoint.String(), label, body)
		if err != nil {
			lastErr = err
			continue
		}

		feResp, err := d.client.Do(feReq)
		if err != nil {
			lastErr = fmt.Errorf("requesting Doris FE %s redirect: %w", feEndpoint.String(), err)
			continue
		}
		if feResp.StatusCode != http.StatusTemporaryRedirect {
			return feResp, nil
		}

		location := feResp.Header.Get(dsHeaderLocation)
		feResp.Body.Close()
		if location == "" {
			lastErr = fmt.Errorf("Doris FE %s redirect response missing Location header", feEndpoint.String())
			continue
		}

		targetURL, err := d.resolveRedirectURL(feEndpoint, location)
		if err != nil {
			lastErr = err
			continue
		}

		beReq, err := d.newRequest(ctx, targetURL, label, body)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := d.client.Do(beReq)
		if err != nil {
			lastErr = fmt.Errorf("sending Doris Stream Load body via FE %s to BE: %w", feEndpoint.String(), err)
			continue
		}
		return resp, nil
	}

	if lastErr == nil {
		lastErr = errors.New("failed to send Doris Stream Load request")
	}
	return nil, lastErr
}

func (d *dorisStreamLoadOutput) newRequest(ctx context.Context, targetURL, label string, body []byte) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, reader)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	for k, v := range d.conf.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Expect", "100-continue")
	}
	req.Header.Set(dsHeaderLabel, label)
	req.Header.Set(dsHeaderFormat, d.conf.Format)
	req.Header.Set(dsHeaderTimeout, strconv.Itoa(int(d.conf.Timeout.Seconds())))
	if len(d.conf.Columns) > 0 {
		req.Header.Set(dsHeaderColumns, strings.Join(d.conf.Columns, ","))
	}
	if d.conf.Where != "" {
		req.Header.Set(dsHeaderWhere, d.conf.Where)
	}
	if d.conf.GroupCommit != "" {
		req.Header.Set(dsHeaderGroupCommit, d.conf.GroupCommit)
	}
	if d.conf.MaxFilterRatio != nil {
		req.Header.Set(dsHeaderMaxFilterRatio, strconv.FormatFloat(*d.conf.MaxFilterRatio, 'f', -1, 64))
	}
	if len(d.conf.Partitions) > 0 {
		req.Header.Set(dsHeaderPartitions, strings.Join(d.conf.Partitions, ","))
	}
	if len(d.conf.TempPartitions) > 0 {
		req.Header.Set(dsHeaderTempPartitions, strings.Join(d.conf.TempPartitions, ","))
	}
	if d.conf.SkipLines != nil {
		req.Header.Set(dsHeaderSkipLines, strconv.Itoa(*d.conf.SkipLines))
	}
	if d.conf.EmptyAsNull {
		req.Header.Set(dsHeaderEmptyAsNull, "true")
	}
	if d.conf.TrimDoubleQuotes {
		req.Header.Set(dsHeaderTrimQuotes, "true")
	}
	if d.conf.NumAsString {
		req.Header.Set(dsHeaderNumAsString, "true")
	}
	if d.conf.FuzzyParse {
		req.Header.Set(dsHeaderFuzzyParse, "true")
	}
	if d.conf.StrictMode {
		req.Header.Set(dsHeaderStrictMode, "true")
	}

	switch d.conf.Format {
	case "json":
		req.Header.Set("Content-Type", "application/json")
		if d.conf.ReadJSONByLine {
			req.Header.Set(dsHeaderReadJSONByLine, "true")
		}
		if d.conf.StripOuterArray {
			req.Header.Set(dsHeaderStripOuterArray, "true")
		}
		if d.conf.JSONPaths != "" {
			req.Header.Set(dsHeaderJSONPaths, d.conf.JSONPaths)
		}
		if d.conf.JSONRoot != "" {
			req.Header.Set(dsHeaderJSONRoot, d.conf.JSONRoot)
		}
	case "csv":
		req.Header.Set("Content-Type", "text/csv")
		req.Header.Set(dsHeaderColumnSeparator, escapeDorisHeaderValue(d.conf.ColumnSeparator))
		req.Header.Set(dsHeaderLineDelimiter, escapeDorisHeaderValue(d.conf.LineDelimiter))
	}

	return req, nil
}

func (d *dorisStreamLoadOutput) resolveRedirectURL(feEndpoint *url.URL, location string) (string, error) {
	loc, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("parsing Doris redirect URL: %w", err)
	}
	if loc.IsAbs() {
		return loc.String(), nil
	}
	return feEndpoint.ResolveReference(loc).String(), nil
}

func streamLoadURLs(baseURLs []string, database, table string) ([]*url.URL, error) {
	endpoints := make([]*url.URL, 0, len(baseURLs))
	for _, baseURL := range baseURLs {
		u, err := url.Parse(baseURL)
		if err != nil {
			return nil, fmt.Errorf("parsing base url %q: %w", baseURL, err)
		}
		u.Path = path.Join(u.Path, "api", database, table, "_stream_load")
		endpoints = append(endpoints, u)
	}
	return endpoints, nil
}

func parseDorisStreamLoadResponse(resp *http.Response) (dorisStreamLoadResponse, []byte, error) {
	var loadResp dorisStreamLoadResponse
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return loadResp, nil, fmt.Errorf("reading response body: %w", err)
	}
	if len(body) == 0 {
		return loadResp, body, errors.New("empty response body")
	}
	if err := json.Unmarshal(body, &loadResp); err != nil {
		return loadResp, body, fmt.Errorf("unmarshaling JSON body %q: %w", string(body), err)
	}
	return loadResp, body, nil
}

func classifyDorisStreamLoadResponse(httpStatus int, resp dorisStreamLoadResponse, rawBody []byte) error {
	if httpStatus < 200 || httpStatus >= 300 {
		return fmt.Errorf("Doris returned HTTP %d: %s", httpStatus, strings.TrimSpace(string(rawBody)))
	}

	message := resp.errorMessage()

	switch resp.Status {
	case dsStatusSuccess:
		return nil
	case dsStatusLabelExists:
		if resp.ExistingJobStatus == dsExistingJobFinished {
			return nil
		}
		return fmt.Errorf(
			"Doris label already exists and existing job is %q: %s; raw response: %s",
			resp.ExistingJobStatus, message, strings.TrimSpace(string(rawBody)),
		)
	case dsStatusPublishTimeout:
		return fmt.Errorf("Doris stream load publish timeout: %s; raw response: %s", message, strings.TrimSpace(string(rawBody)))
	case "":
		return fmt.Errorf("Doris response missing Status: %s", strings.TrimSpace(string(rawBody)))
	default:
		return fmt.Errorf("Doris stream load failed with status %q: %s; raw response: %s", resp.Status, message, strings.TrimSpace(string(rawBody)))
	}
}

func (r dorisStreamLoadResponse) errorMessage() string {
	if r.Message != "" {
		if r.FirstErrorMsg != "" {
			return r.Message + "; first_error=" + r.FirstErrorMsg
		}
		return r.Message
	}
	if r.Msg != "" {
		if r.FirstErrorMsg != "" {
			return r.Msg + "; first_error=" + r.FirstErrorMsg
		}
		return r.Msg
	}
	if r.FirstErrorMsg != "" {
		return r.FirstErrorMsg
	}
	return ""
}

func (d *dorisStreamLoadOutput) getOrCreateLabel(batch service.MessageBatch) string {
	if label, ok := batch[0].MetaGet(dsMetaLabel); ok && label != "" {
		return label
	}

	label := dorisLabel(d.conf.LabelPrefix)
	for _, msg := range batch {
		msg.MetaSetMut(dsMetaLabel, label)
	}
	return label
}

func dorisLabel(prefix string) string {
	prefix = dsLabelSanitizer.ReplaceAllString(prefix, "_")
	prefix = strings.Trim(prefix, "_")
	if prefix == "" {
		prefix = dsDefaultLabelPrefix
	}
	stamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	suffix := xid.New().String()

	maxPrefixLen := 128 - len(stamp) - len(suffix) - 2
	if maxPrefixLen < 1 {
		maxPrefixLen = 1
	}
	if len(prefix) > maxPrefixLen {
		prefix = prefix[:maxPrefixLen]
	}
	return prefix + "_" + stamp + "_" + suffix
}

func escapeDorisHeaderValue(v string) string {
	return dsHeaderValueEscaper.Replace(v)
}
