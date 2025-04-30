// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreams

import (
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
)

// ChangeRecord is the single unit of the records from the change stream.
// See https://cloud.google.com/spanner/docs/change-streams/details#change_streams_record_format
type ChangeRecord struct {
	DataChangeRecords      []*DataChangeRecord      `spanner:"data_change_record" json:"data_change_record"`
	HeartbeatRecords       []*HeartbeatRecord       `spanner:"heartbeat_record" json:"heartbeat_record"`
	ChildPartitionsRecords []*ChildPartitionsRecord `spanner:"child_partitions_record" json:"child_partitions_record"`
}

func (cr *ChangeRecord) String() string {
	var (
		b strings.Builder
		c = false
	)
	b.WriteString("ChangeRecord{")
	if len(cr.DataChangeRecords) > 0 {
		b.WriteString("DataChangeRecords: ")
		b.WriteString(fmt.Sprintf("%+v", cr.DataChangeRecords))
		c = true
	}
	if len(cr.HeartbeatRecords) > 0 {
		if c {
			b.WriteString(", ")
		}
		b.WriteString("HeartbeatRecords: ")
		b.WriteString(fmt.Sprintf("%+v", cr.HeartbeatRecords))
		c = true
	}
	if len(cr.ChildPartitionsRecords) > 0 {
		if c {
			b.WriteString(", ")
		}
		b.WriteString("ChildPartitionsRecords: ")
		b.WriteString(fmt.Sprintf("%+v", cr.ChildPartitionsRecords))
	}
	b.WriteString("}")
	return b.String()
}

// DataChangeRecord contains a set of changes to the table with the same
// modification type (insert, update, or delete) committed at the same
// CommitTimestamp in one change stream partition for the same transaction.
// Multiple data change records can be returned for the same transaction across
// multiple change stream partitions.
//
// All data change records have CommitTimestamp, ServerTransactionID,
// and RecordSequence fields, which together determine the order in the change
// stream for a stream record. These three fields are sufficient to derive
// the ordering of changes and provide external consistency.
//
// Note that multiple transactions can have the same commit timestamp
// if they touch non-overlapping data. The ServerTransactionID field offers
// the ability to distinguish which set of changes (potentially across change
// stream partitions) were issued within the same transaction. Pairing it with
// the RecordSequence and NumberOfRecordsInTransaction fields allows you to
// buffer and order all the records from a particular transaction, as well.
//
// See https://cloud.google.com/spanner/docs/change-streams/details#data-change-records
type DataChangeRecord struct {
	CommitTimestamp                      time.Time     `spanner:"commit_timestamp" json:"commit_timestamp"`
	RecordSequence                       string        `spanner:"record_sequence" json:"record_sequence"`
	ServerTransactionID                  string        `spanner:"server_transaction_id" json:"server_transaction_id"`
	IsLastRecordInTransactionInPartition bool          `spanner:"is_last_record_in_transaction_in_partition" json:"is_last_record_in_transaction_in_partition"`
	TableName                            string        `spanner:"table_name" json:"table_name"`
	ColumnTypes                          []*ColumnType `spanner:"column_types" json:"column_types"`
	Mods                                 []*Mod        `spanner:"mods" json:"mods"`
	ModType                              string        `spanner:"mod_type" json:"mod_type"`
	ValueCaptureType                     string        `spanner:"value_capture_type" json:"value_capture_type"`
	NumberOfRecordsInTransaction         int64         `spanner:"number_of_records_in_transaction" json:"number_of_records_in_transaction"`
	NumberOfPartitionsInTransaction      int64         `spanner:"number_of_partitions_in_transaction" json:"number_of_partitions_in_transaction"`
	TransactionTag                       string        `spanner:"transaction_tag" json:"transaction_tag"`
	IsSystemTransaction                  bool          `spanner:"is_system_transaction" json:"is_system_transaction"`
}

// String implements the fmt.Stringer interface for DataChangeRecord.
func (dcr *DataChangeRecord) String() string {
	return fmt.Sprintf("DataChangeRecord{CommitTimestamp: %v, RecordSequence: %s, ServerTransactionID: %s, "+
		"IsLastRecordInTransactionInPartition: %v, TableName: %s, ColumnTypes: %+v, Mods: %+v, ModType: %s, "+
		"ValueCaptureType: %s, NumberOfRecordsInTransaction: %d, NumberOfPartitionsInTransaction: %d, "+
		"TransactionTag: %s, IsSystemTransaction: %v}",
		dcr.CommitTimestamp, dcr.RecordSequence, dcr.ServerTransactionID,
		dcr.IsLastRecordInTransactionInPartition, dcr.TableName, dcr.ColumnTypes, dcr.Mods, dcr.ModType,
		dcr.ValueCaptureType, dcr.NumberOfRecordsInTransaction, dcr.NumberOfPartitionsInTransaction,
		dcr.TransactionTag, dcr.IsSystemTransaction)
}

// ColumnType is the metadata of the column.
type ColumnType struct {
	Name            string           `spanner:"name" json:"name"`
	Type            spanner.NullJSON `spanner:"type" json:"type"`
	IsPrimaryKey    bool             `spanner:"is_primary_key" json:"is_primary_key"`
	OrdinalPosition int64            `spanner:"ordinal_position" json:"ordinal_position"`
}

// String implements the fmt.Stringer interface for ColumnType.
func (ct *ColumnType) String() string {
	return fmt.Sprintf("ColumnType{Name: %s, Type: %+v, IsPrimaryKey: %v, OrdinalPosition: %d}",
		ct.Name, ct.Type, ct.IsPrimaryKey, ct.OrdinalPosition)
}

// Mod is the changes that were made on the table.
// See https://cloud.google.com/spanner/docs/change-streams/details#heartbeat-records
type Mod struct {
	Keys      spanner.NullJSON `spanner:"keys" json:"keys"`
	NewValues spanner.NullJSON `spanner:"new_values" json:"new_values"`
	OldValues spanner.NullJSON `spanner:"old_values" json:"old_values"`
}

// String implements the fmt.Stringer interface for Mod.
func (m *Mod) String() string {
	return fmt.Sprintf("Mod{Keys: %+v, NewValues: %+v, OldValues: %+v}",
		m.Keys, m.NewValues, m.OldValues)
}

// HeartbeatRecord is the heartbeat record returned from Cloud Spanner.
//
// When a heartbeat record is returned, it indicates that all changes with
// CommitTimestamp less than or equal to the heartbeat record's Timestamp have
// been returned, and future data records in this partition must have higher
// commit timestamps than that returned by the heartbeat record.
//
// Heartbeat records are returned when there are no data changes written to
// a partition. When there are data changes written to the partition,
// DataChangeRecord.CommitTimestamp can be used instead of
// HeartbeatRecord.Timestamp to tell that the reader is making forward
// progress in reading the partition.
//
// You can use heartbeat records returned on partitions to synchronize readers
// across all partitions. Once all readers have received either a heartbeat
// greater than or equal to some timestamp A or have received data or child
// partition records greater than or equal to timestamp A, the readers know they
// have received all records committed at or before that timestamp A and can
// start processing the buffered records—for example, sorting the
// cross-partition records by timestamp and grouping them by ServerTransactionID.
//
// See https://cloud.google.com/spanner/docs/change-streams/details#heartbeat-records
type HeartbeatRecord struct {
	Timestamp time.Time `spanner:"timestamp" json:"timestamp"`
}

// String implements the fmt.Stringer interface for HeartbeatRecord.
func (hr *HeartbeatRecord) String() string {
	return fmt.Sprintf("HeartbeatRecord{Timestamp: %v}", hr.Timestamp)
}

// ChildPartitionsRecord contains information about child partitions:
// their partition tokens, the tokens of their parent partitions,
// and the StartTimestamp that represents the earliest timestamp that the child
// partitions contain change records for. Records whose commit timestamps are
// immediately prior to the StartTimestamp are returned in the current partition.
//
// See https://cloud.google.com/spanner/docs/change-streams/details#child-partitions-records
type ChildPartitionsRecord struct {
	StartTimestamp  time.Time         `spanner:"start_timestamp" json:"start_timestamp"`
	RecordSequence  string            `spanner:"record_sequence" json:"record_sequence"`
	ChildPartitions []*ChildPartition `spanner:"child_partitions" json:"child_partitions"`
}

func (cpr *ChildPartitionsRecord) String() string {
	return fmt.Sprintf("ChildPartitionsRecord{StartTimestamp: %v, RecordSequence: %s, ChildPartitions: %+v}",
		cpr.StartTimestamp, cpr.RecordSequence, cpr.ChildPartitions)
}

// ChildPartition contains the child partition token.
type ChildPartition struct {
	Token                 string   `spanner:"token" json:"token"`
	ParentPartitionTokens []string `spanner:"parent_partition_tokens" json:"parent_partition_tokens"`
}

func (cp *ChildPartition) String() string {
	return fmt.Sprintf("ChildPartition{Token: %s, ParentPartitionTokens: %+v}",
		cp.Token, cp.ParentPartitionTokens)
}

// TODO(mmt): add splits to metrics
func (cp *ChildPartition) isSplit() bool {
	return len(cp.ParentPartitionTokens) == 1
}
