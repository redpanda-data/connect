// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package metadata

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
)

// State represents the current status of a partition in the change stream.
type State string

// Possible states for a partition in the change stream.
const (
	StateCreated   State = "CREATED"
	StateScheduled State = "SCHEDULED"
	StateRunning   State = "RUNNING"
	StateFinished  State = "FINISHED"
)

// PartitionMetadata contains information about a change stream partition.
//
// To support reading change stream records in near  real-time as database
// writes scale, the Spanner API is designed for a change stream to be queried
// concurrently using change stream partitions. Change stream partitions
// map to change stream data splits that contain the change stream records.
// A change stream's partitions change dynamically over time and are correlated
// to how Spanner dynamically splits and merges the database data.
//
// A change stream partition contains records for an immutable key range for
// a specific time range. Any change stream partition can split into one or more
// change stream partitions, or be merged with other change stream partitions.
// When these split or merge events happen, child partitions are created to
// capture the changes for their respective immutable key ranges for the next
// time range. In addition to data change records, a change stream query returns
// child partition records to notify readers of new change stream partitions
// that need to be queried, as well as heartbeat records to indicate forward
// progress when no writes have occurred recently.
//
// The StartTimestamp is taken from ChildPartitionsRecord.StartTimestamp,
// and represents the earliest DataChangeRecord.CommitTimestamp in this
// partition or in the sibling partitions.
//
// The Watermark is set to the last processed DataChangeRecord.CommitTimestamp
// in this partition.
//
// The order of timestamps monotonically increases, starting with:
//   - StartTimestamp,
//   - Watermark,
//   - CreatedAt,
//   - ScheduledAt,
//   - RunningAt,
//   - FinishedAt.
//
// The last four timestamps are set to the Spanner commit timestamp when
// the PartitionMetadata record is created, scheduled, started, or finished.
type PartitionMetadata struct {
	PartitionToken  string     `spanner:"PartitionToken" json:"partition_token"`
	ParentTokens    []string   `spanner:"ParentTokens" json:"parent_tokens"`
	StartTimestamp  time.Time  `spanner:"StartTimestamp" json:"start_timestamp"`
	EndTimestamp    time.Time  `spanner:"EndTimestamp" json:"end_timestamp"`
	HeartbeatMillis int64      `spanner:"HeartbeatMillis" json:"heartbeat_millis"`
	State           State      `spanner:"State" json:"state"`
	Watermark       time.Time  `spanner:"Watermark" json:"watermark"`
	CreatedAt       time.Time  `spanner:"CreatedAt" json:"created_at"`
	ScheduledAt     *time.Time `spanner:"ScheduledAt" json:"scheduled_at,omitempty"`
	RunningAt       *time.Time `spanner:"RunningAt" json:"running_at,omitempty"`
	FinishedAt      *time.Time `spanner:"FinishedAt" json:"finished_at,omitempty"`
}

// Column names for the partition metadata table
const (
	columnPartitionToken  = "PartitionToken"
	columnParentTokens    = "ParentTokens"
	columnStartTimestamp  = "StartTimestamp"
	columnEndTimestamp    = "EndTimestamp"
	columnHeartbeatMillis = "HeartbeatMillis"
	columnState           = "State"
	columnWatermark       = "Watermark"
	columnCreatedAt       = "CreatedAt"
	columnScheduledAt     = "ScheduledAt"
	columnRunningAt       = "RunningAt"
	columnFinishedAt      = "FinishedAt"
)

// StoreConfig contains configuration for the metadata store.
type StoreConfig struct {
	ProjectID  string
	InstanceID string
	DatabaseID string
	Dialect    databasepb.DatabaseDialect
	TableNames
}

func (c StoreConfig) fullDatabaseName() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", c.ProjectID, c.InstanceID, c.DatabaseID)
}

func (c StoreConfig) isPostgres() bool {
	return c.Dialect == databasepb.DatabaseDialect_POSTGRESQL
}

// CreatePartitionMetadataTableWithDatabaseAdminClient creates a table for
// storing partition metadata if it doesn't exist.
func CreatePartitionMetadataTableWithDatabaseAdminClient(
	ctx context.Context,
	conf StoreConfig,
	adm *adminapi.DatabaseAdminClient,
) error {
	const TTLAfterPartitionFinishedDays = 1

	var ddl []string

	if conf.isPostgres() {
		// PostgreSQL requires quotes around identifiers to preserve casing
		ddl = append(ddl, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"("%s" text NOT NULL,"%s" text[] NOT NULL,"%s" timestamptz NOT NULL,"%s" timestamptz NOT NULL,"%s" BIGINT NOT NULL,"%s" text NOT NULL,"%s" timestamptz NOT NULL,"%s" SPANNER.COMMIT_TIMESTAMP NOT NULL,"%s" SPANNER.COMMIT_TIMESTAMP,"%s" SPANNER.COMMIT_TIMESTAMP,"%s" SPANNER.COMMIT_TIMESTAMP, PRIMARY KEY ("%s")) TTL INTERVAL '%d days' ON "%s"`,
			conf.TableName,
			columnPartitionToken,
			columnParentTokens,
			columnStartTimestamp,
			columnEndTimestamp,
			columnHeartbeatMillis,
			columnState,
			columnWatermark,
			columnCreatedAt,
			columnScheduledAt,
			columnRunningAt,
			columnFinishedAt,
			columnPartitionToken,
			TTLAfterPartitionFinishedDays,
			columnFinishedAt))

		ddl = append(ddl, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s" on "%s" ("%s") INCLUDE ("%s")`,
			conf.WatermarkIndexName,
			conf.TableName,
			columnWatermark,
			columnState))

		ddl = append(ddl, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s" ON "%s" ("%s","%s")`,
			conf.CreatedAtIndexName,
			conf.TableName,
			columnCreatedAt,
			columnStartTimestamp))
	} else {
		ddl = append(ddl, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s STRING(MAX) NOT NULL,%s ARRAY<STRING(MAX)> NOT NULL,%s TIMESTAMP NOT NULL,%s TIMESTAMP NOT NULL,%s INT64 NOT NULL,%s STRING(MAX) NOT NULL,%s TIMESTAMP NOT NULL,%s TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),%s TIMESTAMP OPTIONS (allow_commit_timestamp=true),%s TIMESTAMP OPTIONS (allow_commit_timestamp=true),%s TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (%s), ROW DELETION POLICY (OLDER_THAN(%s, INTERVAL %d DAY))`,
			conf.TableName,
			columnPartitionToken,
			columnParentTokens,
			columnStartTimestamp,
			columnEndTimestamp,
			columnHeartbeatMillis,
			columnState,
			columnWatermark,
			columnCreatedAt,
			columnScheduledAt,
			columnRunningAt,
			columnFinishedAt,
			columnPartitionToken,
			columnFinishedAt,
			TTLAfterPartitionFinishedDays))

		ddl = append(ddl, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s on %s (%s) STORING (%s)`,
			conf.WatermarkIndexName,
			conf.TableName,
			columnWatermark,
			columnState))

		ddl = append(ddl, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (%s,%s)`,
			conf.CreatedAtIndexName,
			conf.TableName,
			columnCreatedAt,
			columnStartTimestamp))
	}

	op, err := adm.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   conf.fullDatabaseName(),
		Statements: ddl,
	})
	if err != nil {
		return fmt.Errorf("create partition metadata table: %w", err)
	}

	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("wait for partition metadata table creation: %w", err)
	}

	return nil
}

// DeletePartitionMetadataTableWithDatabaseAdminClient deletes the partition
// metadata table.
func DeletePartitionMetadataTableWithDatabaseAdminClient(
	ctx context.Context,
	conf StoreConfig,
	adm *adminapi.DatabaseAdminClient,
) error {
	var ddl []string

	if conf.isPostgres() {
		for _, index := range []string{conf.WatermarkIndexName, conf.CreatedAtIndexName} {
			ddl = append(ddl, fmt.Sprintf(`DROP INDEX "%s"`, index))
		}
		ddl = append(ddl, fmt.Sprintf(`DROP TABLE "%s"`, conf.TableName))
	} else {
		for _, index := range []string{conf.WatermarkIndexName, conf.CreatedAtIndexName} {
			ddl = append(ddl, fmt.Sprintf(`DROP INDEX %s`, index))
		}
		ddl = append(ddl, fmt.Sprintf(`DROP TABLE %s`, conf.TableName))
	}

	op, err := adm.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   conf.fullDatabaseName(),
		Statements: ddl,
	})
	if err != nil {
		return fmt.Errorf("delete partition metadata table: %w", err)
	}

	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("wait for partition metadata table deletion: %w", err)
	}

	return nil
}

// Store manages the persistence of partition metadata.
type Store struct {
	conf   StoreConfig
	client *spanner.Client
}

// NewStore returns a Store instance with the given configuration and Spanner
// client. The client must be connected to the same database as the configuration.
func NewStore(conf StoreConfig, client *spanner.Client) *Store {
	return &Store{
		conf:   conf,
		client: client,
	}
}

// GetPartition fetches the partition metadata row data for the given partition token.
func (s *Store) GetPartition(ctx context.Context, partitionToken string) (PartitionMetadata, error) {
	var stmt spanner.Statement
	if s.conf.isPostgres() {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT * FROM "%s" WHERE "%s" = $1`,
				s.conf.TableName, columnPartitionToken),
			Params: map[string]any{"p1": partitionToken},
		}
	} else {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT * FROM %s WHERE %s = @partition`,
				s.conf.TableName, columnPartitionToken),
			Params: map[string]any{"partition": partitionToken},
		}
	}

	iter := s.client.Single().QueryWithOptions(ctx, stmt, queryTag("GetPartition"))
	defer iter.Stop()

	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return PartitionMetadata{}, nil
	}
	if err != nil {
		return PartitionMetadata{}, fmt.Errorf("get partition: %w", err)
	}

	var pm PartitionMetadata
	if err := row.ToStruct(&pm); err != nil {
		return PartitionMetadata{}, fmt.Errorf("parse partition: %w", err)
	}

	return pm, nil
}

// GetUnfinishedMinWatermark fetches the earliest partition watermark from
// the partition metadata table that is not in a FINISHED state.
func (s *Store) GetUnfinishedMinWatermark(ctx context.Context) (time.Time, error) {
	var stmt spanner.Statement
	if s.conf.isPostgres() {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT "%s" FROM "%s" WHERE "%s" != $1 ORDER BY "%s" ASC LIMIT 1`,
				columnWatermark, s.conf.TableName, columnState, columnWatermark),
			Params: map[string]any{"p1": StateFinished},
		}
	} else {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT %s FROM %s WHERE %s != @state ORDER BY %s ASC LIMIT 1`,
				columnWatermark, s.conf.TableName, columnState, columnWatermark),
			Params: map[string]any{"state": StateFinished},
		}
	}

	iter := s.client.Single().QueryWithOptions(ctx, stmt, queryTag("GetUnfinishedMinWatermark"))
	defer iter.Stop()

	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("get unfinished min watermark: %w", err)
	}

	var watermark time.Time
	if err := row.Columns(&watermark); err != nil {
		return time.Time{}, fmt.Errorf("parse watermark: %w", err)
	}

	return watermark, nil
}

// GetPartitionsCreatedAfter fetches all partitions created after the
// specified timestamp that are in the CREATED state. Results are ordered by
// creation time and start timestamp in ascending order.
func (s *Store) GetPartitionsCreatedAfter(ctx context.Context, timestamp time.Time) ([]PartitionMetadata, error) {
	var stmt spanner.Statement
	if s.conf.isPostgres() {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT * FROM "%s" WHERE "%s" > $1 AND "%s" = $2 ORDER BY "%s" ASC, "%s" ASC`,
				s.conf.TableName, columnCreatedAt, columnState, columnCreatedAt, columnStartTimestamp),
			Params: map[string]any{
				"p1": timestamp,
				"p2": StateCreated,
			},
		}
	} else {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT * FROM %s WHERE %s > @timestamp AND %s = @state ORDER BY %s ASC, %s ASC`,
				s.conf.TableName, columnCreatedAt, columnState, columnCreatedAt, columnStartTimestamp),
			Params: map[string]any{
				"timestamp": timestamp,
				"state":     StateCreated,
			},
		}
	}

	iter := s.client.Single().QueryWithOptions(ctx, stmt, queryTag("GetPartitionsCreatedAfter"))
	defer iter.Stop()

	var pms []PartitionMetadata
	if err := iter.Do(func(row *spanner.Row) error {
		var p PartitionMetadata
		if err := row.ToStruct(&p); err != nil {
			return err
		}
		pms = append(pms, p)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("get all partitions created after: %w", err)
	}

	return pms, nil
}

// Create creates a new partition metadata row in state CREATED.
func (s *Store) Create(ctx context.Context, pms []PartitionMetadata) error {
	ms := make([]*spanner.Mutation, len(pms))

	for i, p := range pms {
		ms[i] = spanner.Insert(s.conf.TableName,
			[]string{
				columnPartitionToken,
				columnParentTokens,
				columnStartTimestamp,
				columnEndTimestamp,
				columnHeartbeatMillis,
				columnState,
				columnWatermark,
				columnCreatedAt,
			},
			[]any{
				p.PartitionToken,
				p.ParentTokens,
				p.StartTimestamp,
				p.EndTimestamp,
				p.HeartbeatMillis,
				StateCreated,
				p.Watermark,
				spanner.CommitTimestamp,
			})
	}

	return s.applyWithTag(ctx, "Create", ms...)
}

func (s *Store) insert(ctx context.Context, partitions []PartitionMetadata) error {
	ms := make([]*spanner.Mutation, len(partitions))

	var err error
	for i := range partitions {
		ms[i], err = spanner.InsertStruct(s.conf.TableName, &partitions[i])
		if err != nil {
			return err
		}
	}

	return s.applyWithTag(ctx, "Insert", ms...)
}

// UpdateToScheduled updates multiple partition rows to SCHEDULED state. It only
// updates partitions that are currently in CREATED state. Returns the commit
// timestamp of the transaction.
func (s *Store) UpdateToScheduled(ctx context.Context, partitionTokens []string) (time.Time, error) {
	return s.updatePartitionStatus(ctx, partitionTokens, StateCreated, StateScheduled, columnScheduledAt)
}

// UpdateToRunning updates partition row to RUNNING state. It only updates
// partitions that are currently in SCHEDULED state. Returns the commit
// timestamp of the transaction.
func (s *Store) UpdateToRunning(ctx context.Context, partitionToken string) (time.Time, error) {
	return s.updatePartitionStatus(ctx, []string{partitionToken}, StateScheduled, StateRunning, columnRunningAt)
}

// UpdateToFinished updates partition row to FINISHED state. It only updates
// partitions that are currently in RUNNING state. Returns the commit
// timestamp of the transaction.
func (s *Store) UpdateToFinished(ctx context.Context, partitionToken string) (time.Time, error) {
	return s.updatePartitionStatus(ctx, []string{partitionToken}, StateRunning, StateFinished, columnFinishedAt)
}

// updatePartitionStatus updates partition rows from fromState to toState and
// sets the specified timestamp column to the commit timestamp. It only updates
// partitions that are currently in fromState. Returns the commit timestamp
// of the transaction.
func (s *Store) updatePartitionStatus(
	ctx context.Context,
	partitionTokens []string,
	fromState State,
	toState State,
	timestampColumn string,
) (time.Time, error) {
	resp, err := s.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		matchingTokens, err := s.getPartitionsMatchingStateInTransaction(ctx, txn, partitionTokens, fromState)
		if err != nil {
			return fmt.Errorf("get partitions matching state: %w", err)
		}

		var ms []*spanner.Mutation
		for _, token := range matchingTokens {
			m := spanner.Update(
				s.conf.TableName,
				[]string{
					columnPartitionToken,
					columnState,
					timestampColumn,
				},
				[]any{
					token,
					toState,
					spanner.CommitTimestamp,
				})
			ms = append(ms, m)
		}
		return txn.BufferWrite(ms)
	}, spanner.TransactionOptions{TransactionTag: "UpdateTo" + strings.ToTitle(string(toState))})

	return resp.CommitTs.UTC(), err
}

// CheckParentPartitionsFinished checks if all parent tokens in the given list
// are in FINISHED state.
func (s *Store) CheckParentPartitionsFinished(ctx context.Context, partitionTokens []string) (bool, error) {
	if len(partitionTokens) == 0 {
		return true, nil
	}

	var ok bool

	if _, err := s.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		matchingTokens, err := s.getPartitionsMatchingStateInTransaction(ctx, txn, partitionTokens, StateFinished)
		if err != nil {
			return fmt.Errorf("get partitions matching state: %w", err)
		}
		ok = len(partitionTokens) == len(matchingTokens)

		return nil
	}, spanner.TransactionOptions{TransactionTag: "CheckParentPartitionsFinished"}); err != nil {
		return false, err
	}

	return ok, nil
}

func (s *Store) getPartitionsMatchingStateInTransaction(
	ctx context.Context,
	txn *spanner.ReadWriteTransaction,
	partitionTokens []string,
	state State,
) ([]string, error) {
	var stmt spanner.Statement
	if s.conf.isPostgres() {
		var sb strings.Builder
		for i, tok := range partitionTokens {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('\'')
			sb.WriteString(tok)
			sb.WriteByte('\'')
		}

		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT "%s" FROM "%s" WHERE "%s" = ANY (Array[%s]) AND "%s" = '%s'`,
				columnPartitionToken,
				s.conf.TableName,
				columnPartitionToken,
				sb.String(),
				columnState,
				state),
		}
	} else {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT %s FROM %s WHERE %s IN UNNEST(@partitionTokens) AND %s = @state`,
				columnPartitionToken,
				s.conf.TableName,
				columnPartitionToken,
				columnState),
			Params: map[string]any{
				"partitionTokens": partitionTokens,
				"state":           state,
			},
		}
	}

	iter := txn.QueryWithOptions(ctx, stmt, queryTag(fmt.Sprintf("getPartitionsMatchingState=%s", state)))
	defer iter.Stop()

	var matchingTokens []string
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("query partitions: %w", err)
		}

		var token string
		if err := row.Column(0, &token); err != nil {
			return nil, fmt.Errorf("get partition token: %w", err)
		}
		matchingTokens = append(matchingTokens, token)
	}

	return matchingTokens, nil
}

// UpdateWatermark updates the partition watermark to the given timestamp.
func (s *Store) UpdateWatermark(ctx context.Context, partitionToken string, watermark time.Time) error {
	m := spanner.Update(
		s.conf.TableName,
		[]string{
			columnPartitionToken,
			columnWatermark,
		},
		[]any{
			partitionToken,
			watermark,
		},
	)

	return s.applyWithTag(ctx, "updateWatermark", m)
}

func queryTag(tag string) spanner.QueryOptions {
	return spanner.QueryOptions{RequestTag: "query=" + tag}
}

func (s *Store) applyWithTag(ctx context.Context, tag string, ms ...*spanner.Mutation) error {
	_, err := s.client.Apply(ctx, ms, spanner.TransactionTag(tag))
	if err != nil {
		return fmt.Errorf("%s: %w", tag, err)
	}

	return nil
}
