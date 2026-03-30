// Copyright 2024 Redpanda Data, Inc.
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

package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func sqlRawOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Executes an arbitrary SQL query for each message.").
		Description(`When multiple queries are configured, all messages within a batch are executed in a single database transaction. Single-query configurations execute each message individually without a transaction, preserving per-message error granularity. When consuming from Kafka, messages are automatically ordered by partition within the transaction, allowing `+"`max_in_flight` > 1"+` to parallelize across partitions while preserving consume order within each partition. Messages without `+"`kafka_partition`"+` metadata default to partition 0.`).
		Field(driverField).
		Field(dsnField).
		Field(rawQueryField().
			Example("INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);").Optional()).
		Field(service.NewBoolField("unsafe_dynamic_query").
			Description("Whether to enable xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions] in the query. Great care should be made to ensure your queries are defended against injection attacks.").
			Advanced().
			Default(false)).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional xref:guides:bloblang/about.adoc[Bloblang mapping] which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `query`.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`).
			Optional()).
		Field(service.NewObjectListField(
			"queries",
			rawQueryField(),
			rawQueryArgsMappingField(),
			rawQueryWhenField(),
		).
			Description("A list of query statements. When a `when` condition is specified on entries, the first query whose condition evaluates to `true` (or that has no condition) is executed for each message. When no `when` conditions are present, all queries execute for each message within a transaction. When specifying multiple statements without conditions, they are all executed within a transaction.").
			Optional()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of batches to be sending in parallel at any given time.").
			Default(64)).
		Fields(connFields()...).
		Field(service.NewBatchPolicyField("batching")).
		Version("3.65.0").
		Example("Table Insert (MySQL)",
			`
Here we insert rows into a database by populating the columns id, name and topic with values extracted from messages and metadata:`,
			`
output:
  sql_raw:
    driver: mysql
    dsn: foouser:foopassword@tcp(localhost:3306)/foodb
    query: "INSERT INTO footable (id, name, topic) VALUES (?, ?, ?);"
    args_mapping: |
      root = [
        this.user.id,
        this.user.name,
        meta("kafka_topic"),
      ]
`,
		).
		Example(
			"Dynamically Creating Tables (PostgreSQL)",
			`Here we dynamically create output tables transactionally with inserting a record into the newly created table.`,
			`
output:
  processors:
    - mapping: |
        root = this
        # Prevent SQL injection when using unsafe_dynamic_query
        meta table_name = "\"" + metadata("table_name").replace_all("\"", "\"\"") + "\""
  sql_raw:
    driver: postgres
    dsn: postgres://localhost/postgres
    unsafe_dynamic_query: true
    queries:
      - query: |
          CREATE TABLE IF NOT EXISTS ${!metadata("table_name")} (id varchar primary key, document jsonb);
      - query: |
          INSERT INTO ${!metadata("table_name")} (id, document) VALUES ($1, $2)
          ON CONFLICT (id) DO UPDATE SET document = EXCLUDED.document;
        args_mapping: |
          root = [ this.id, this.document.string() ]

`,
		).
		Example(
			"Conditional CDC Queries (PostgreSQL)",
			`Route messages to different SQL operations based on message metadata. Tombstone messages trigger a DELETE, while all other messages perform an upsert. All operations within a batch execute in a single transaction, ordered by Kafka partition.`,
			`
output:
  sql_raw:
    driver: postgres
    dsn: postgres://localhost/postgres
    max_in_flight: 8
    batching:
      count: 100
      period: 100ms
    queries:
      - when: 'root = meta("kafka_tombstone_message") == "true"'
        query: 'DELETE FROM users WHERE id = $1'
        args_mapping: 'root = [this.id]'
      - query: |
          INSERT INTO users (id, name, updated_at)
          VALUES ($1, $2, $3)
          ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            updated_at = EXCLUDED.updated_at
        args_mapping: 'root = [this.id, this.name, this.updated_at]'
`,
		).
		LintRule(`root = match {
        !this.exists("queries") && !this.exists("query") => [ "either ` + "`query`" + ` or ` + "`queries`" + ` is required" ],
    }`).
		LintRule(`root = if this.exists("queries") && this.queries.length() > 1 && this.queries.any(q -> q.exists("when")) {
        this.queries.map_each(q -> if q.exists("when") { "has_when" } else { "no_when" }).fold({"idx": 0, "errs": []}, item -> {
            "idx": item.tally.idx + 1,
            "errs": if item.value == "no_when" && item.tally.idx < this.queries.length() - 1 {
                item.tally.errs.append("query at index %d has no ` + "`when`" + ` condition and will always match, making subsequent queries unreachable".format(item.tally.idx))
            } else {
                item.tally.errs
            },
        }).errs
    } else { [] }`)
}

func init() {
	service.MustRegisterBatchOutput(
		"sql_raw", sqlRawOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			out, err = newSQLRawOutputFromConfig(conf, mgr)
			return
		})
}

//------------------------------------------------------------------------------

type sqlRawOutput struct {
	driver string
	dsn    string
	db     *sql.DB
	dbMut  sync.RWMutex

	queries           []rawQueryStatement
	hasWhenConditions bool

	argsConverter argsConverter

	connSettings *connSettings

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newSQLRawOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlRawOutput, error) {
	driverStr, err := conf.FieldString("driver")
	if err != nil {
		return nil, err
	}

	dsnStr, err := conf.FieldString("dsn")
	if err != nil {
		return nil, err
	}

	unsafeDyn, err := conf.FieldBool("unsafe_dynamic_query")
	if err != nil {
		return nil, err
	}

	queriesConf := []*service.ParsedConfig{}
	if conf.Contains("query") {
		queriesConf = append(queriesConf, conf)
	}
	if conf.Contains("queries") {
		qc, err := conf.FieldObjectList("queries")
		if err != nil {
			return nil, err
		}
		queriesConf = append(queriesConf, qc...)
	}

	if len(queriesConf) == 0 {
		return nil, errors.New("either field 'query' or field 'queries' is required")
	}

	var queries []rawQueryStatement
	var hasWhenConditions bool
	for _, qc := range queriesConf {
		var statement rawQueryStatement
		if unsafeDyn {
			statement.dynamic, err = qc.FieldInterpolatedString("query")
			if err != nil {
				return nil, err
			}
		} else {
			statement.static, err = qc.FieldString("query")
			if err != nil {
				return nil, err
			}
		}

		if qc.Contains("args_mapping") {
			if statement.argsMapping, err = qc.FieldBloblang("args_mapping"); err != nil {
				return nil, err
			}
		}

		if qc.Contains("when") {
			if statement.whenMapping, err = qc.FieldBloblang("when"); err != nil {
				return nil, err
			}
			hasWhenConditions = true
		}
		queries = append(queries, statement)
	}

	connSettings, err := connSettingsFromParsed(conf, mgr)
	if err != nil {
		return nil, err
	}

	var argsConverter argsConverter
	if driverStr == "postgres" {
		argsConverter = bloblValuesToPgSQLValues
	} else {
		argsConverter = func(v []any) []any { return v }
	}

	return newSQLRawOutput(mgr.Logger(), driverStr, dsnStr, queries, hasWhenConditions, argsConverter, connSettings), nil
}

func newSQLRawOutput(
	logger *service.Logger,
	driverStr, dsnStr string,
	queries []rawQueryStatement,
	hasWhenConditions bool,
	argsConverter argsConverter,
	connSettings *connSettings,
) *sqlRawOutput {
	return &sqlRawOutput{
		logger:            logger,
		shutSig:           shutdown.NewSignaller(),
		driver:            driverStr,
		dsn:               dsnStr,
		queries:           queries,
		hasWhenConditions: hasWhenConditions,
		argsConverter:     argsConverter,
		connSettings:      connSettings,
	}
}

func (s *sqlRawOutput) Connect(ctx context.Context) error {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db != nil {
		return nil
	}

	var err error
	if s.db, err = sqlOpenWithReworks(s.logger, s.driver, s.dsn); err != nil {
		return err
	}

	s.connSettings.apply(ctx, s.db, s.logger)

	go func() {
		<-s.shutSig.HardStopChan()

		s.dbMut.Lock()
		_ = s.db.Close()
		s.dbMut.Unlock()

		s.shutSig.TriggerHasStopped()
	}()
	return nil
}

func (s *sqlRawOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	// Build batch-level executors for each query's mappings.
	argsExec := make([]*service.MessageBatchBloblangExecutor, len(s.queries))
	for i, q := range s.queries {
		if q.argsMapping != nil {
			argsExec[i] = batch.BloblangExecutor(q.argsMapping)
		}
	}
	dynQueries := make([]*service.MessageBatchInterpolationExecutor, len(s.queries))
	for i, q := range s.queries {
		if q.dynamic != nil {
			dynQueries[i] = batch.InterpolationExecutor(q.dynamic)
		}
	}

	whenExec := make([]*service.MessageBatchBloblangExecutor, len(s.queries))
	for i, q := range s.queries {
		if q.whenMapping != nil {
			whenExec[i] = batch.BloblangExecutor(q.whenMapping)
		}
	}

	// Single-query configs use non-transactional per-message execution for
	// backward compatibility. This preserves per-message error granularity so
	// the framework can retry only failed messages.
	if len(s.queries) == 1 {
		return s.writeBatchPerMessage(ctx, batch, argsExec, dynQueries, whenExec)
	}

	// Group message indices by kafka_partition so that messages from the same
	// partition execute in consume order within the transaction.
	ordered := s.partitionOrderedIndices(batch)

	// Execute the entire batch within a single transaction.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() {
		if err != nil && !errors.Is(err, context.Canceled) {
			if rerr := tx.Rollback(); rerr != nil {
				s.logger.Warnf("Failed to rollback transaction: %v", rerr)
			}
		}
	}()

	for _, i := range ordered {
		if s.hasWhenConditions {
			err = s.execConditionalQuery(ctx, tx, batch, i, argsExec, dynQueries, whenExec)
		} else {
			err = s.execAllQueries(ctx, tx, batch, i, argsExec, dynQueries)
		}
		if err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

// writeBatchPerMessage executes a single query per message without a
// transaction, preserving per-message error granularity so that only failed
// messages are retried by the framework.
func (s *sqlRawOutput) writeBatchPerMessage(
	ctx context.Context,
	batch service.MessageBatch,
	argsExec []*service.MessageBatchBloblangExecutor,
	dynQueries []*service.MessageBatchInterpolationExecutor,
	whenExec []*service.MessageBatchBloblangExecutor,
) error {
	query := s.queries[0]
	return batch.WalkWithBatchedErrors(func(i int, _ *service.Message) error {
		// Evaluate the when condition if present; skip message if false.
		if whenExec[0] != nil {
			resMsg, err := whenExec[0].Query(i)
			if err != nil {
				return fmt.Errorf("when condition evaluation failed: %w", err)
			}
			val, err := resMsg.AsStructured()
			if err != nil {
				return fmt.Errorf("when condition returned non-structured result: %w", err)
			}
			b, ok := val.(bool)
			if !ok {
				return fmt.Errorf("when condition returned non-boolean value: %T", val)
			}
			if !b {
				return nil
			}
		}

		var args []any
		if argsExec[0] != nil {
			resMsg, err := argsExec[0].Query(i)
			if err != nil {
				return fmt.Errorf("arguments mapping failed: %w", err)
			}

			iargs, err := resMsg.AsStructured()
			if err != nil {
				return fmt.Errorf("mapping returned non-structured result: %w", err)
			}

			var ok bool
			if args, ok = iargs.([]any); !ok {
				return fmt.Errorf("mapping returned non-array result: %T", iargs)
			}
			args = s.argsConverter(args)
		}

		queryStr := query.static
		if query.dynamic != nil {
			var err error
			if queryStr, err = dynQueries[0].TryString(i); err != nil {
				return fmt.Errorf("query interpolation error: %w", err)
			}
		}

		if _, err := s.db.ExecContext(ctx, queryStr, args...); err != nil {
			return fmt.Errorf("running query: %w", err)
		}
		return nil
	})
}

// partitionOrderedIndices returns message indices grouped by kafka_partition
// metadata, with partitions sorted numerically. Messages within the same
// partition retain their original batch order.
func (s *sqlRawOutput) partitionOrderedIndices(batch service.MessageBatch) []int {
	if len(batch) <= 1 {
		if len(batch) == 1 {
			return []int{0}
		}
		return nil
	}

	type partitionGroup struct {
		partition int
		indices   []int
	}
	groups := map[int]*partitionGroup{}
	for i, msg := range batch {
		partition := 0
		if pStr, ok := msg.MetaGet("kafka_partition"); ok && pStr != "" {
			if p, convErr := strconv.Atoi(pStr); convErr != nil {
				s.logger.Warnf("Failed to parse kafka_partition metadata %q as integer, defaulting to partition 0: %v", pStr, convErr)
			} else {
				partition = p
			}
		}
		g, exists := groups[partition]
		if !exists {
			g = &partitionGroup{partition: partition}
			groups[partition] = g
		}
		g.indices = append(g.indices, i)
	}

	if len(groups) == 1 {
		// All messages in the same partition (or no partition metadata);
		// preserve original order.
		ordered := make([]int, len(batch))
		for i := range ordered {
			ordered[i] = i
		}
		return ordered
	}

	keys := make([]int, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	ordered := make([]int, 0, len(batch))
	for _, k := range keys {
		ordered = append(ordered, groups[k].indices...)
	}
	return ordered
}

// execConditionalQuery evaluates when conditions and executes the first
// matching query for message i.
func (s *sqlRawOutput) execConditionalQuery(
	ctx context.Context,
	tx *sql.Tx,
	batch service.MessageBatch,
	i int,
	argsExec []*service.MessageBatchBloblangExecutor,
	dynQueries []*service.MessageBatchInterpolationExecutor,
	whenExec []*service.MessageBatchBloblangExecutor,
) error {
	for j, query := range s.queries {
		if whenExec[j] != nil {
			resMsg, err := whenExec[j].Query(i)
			if err != nil {
				return fmt.Errorf("when condition evaluation failed for query %d: %w", j, err)
			}
			val, err := resMsg.AsStructured()
			if err != nil {
				return fmt.Errorf("when condition returned non-structured result for query %d: %w", j, err)
			}
			b, ok := val.(bool)
			if !ok {
				return fmt.Errorf("when condition for query %d returned non-boolean value: %T", j, val)
			}
			if !b {
				continue
			}
		}
		return s.execQuery(ctx, tx, batch, i, j, query, argsExec, dynQueries)
	}
	// No query matched — this is not an error; the message is skipped.
	return nil
}

// execAllQueries executes all configured queries for message i (the original
// multi-statement transaction behavior).
func (s *sqlRawOutput) execAllQueries(
	ctx context.Context,
	tx *sql.Tx,
	batch service.MessageBatch,
	i int,
	argsExec []*service.MessageBatchBloblangExecutor,
	dynQueries []*service.MessageBatchInterpolationExecutor,
) error {
	for j, query := range s.queries {
		if err := s.execQuery(ctx, tx, batch, i, j, query, argsExec, dynQueries); err != nil {
			return err
		}
	}
	return nil
}

// execQuery resolves args and dynamic query strings, then executes a single
// query within the provided transaction.
func (s *sqlRawOutput) execQuery(
	ctx context.Context,
	tx *sql.Tx,
	_ service.MessageBatch,
	msgIdx, queryIdx int,
	query rawQueryStatement,
	argsExec []*service.MessageBatchBloblangExecutor,
	dynQueries []*service.MessageBatchInterpolationExecutor,
) error {
	var args []any
	if argsExec[queryIdx] != nil {
		resMsg, err := argsExec[queryIdx].Query(msgIdx)
		if err != nil {
			return fmt.Errorf("arguments mapping failed: %w", err)
		}

		iargs, err := resMsg.AsStructured()
		if err != nil {
			return fmt.Errorf("mapping returned non-structured result: %w", err)
		}

		var ok bool
		if args, ok = iargs.([]any); !ok {
			return fmt.Errorf("mapping returned non-array result: %T", iargs)
		}
		args = s.argsConverter(args)
	}

	queryStr := query.static
	if query.dynamic != nil {
		var err error
		if queryStr, err = dynQueries[queryIdx].TryString(msgIdx); err != nil {
			return fmt.Errorf("query interpolation error: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, queryStr, args...); err != nil {
		return fmt.Errorf("running query: %w", err)
	}
	return nil
}

func (s *sqlRawOutput) Close(ctx context.Context) error {
	s.shutSig.TriggerHardStop()
	s.dbMut.RLock()
	isNil := s.db == nil
	s.dbMut.RUnlock()
	if isNil {
		return nil
	}
	select {
	case <-s.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
