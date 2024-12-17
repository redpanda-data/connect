package spannercdc

import (
	"time"

	"cloud.google.com/go/spanner"
)

// ChangeRecord is the change record of the table.
type ChangeRecord struct {
	DataChangeRecords      []*dataChangeRecord      `spanner:"data_change_record" json:"data_change_record"`
	HeartbeatRecords       []*HeartbeatRecord       `spanner:"heartbeat_record" json:"heartbeat_record"`
	ChildPartitionsRecords []*ChildPartitionsRecord `spanner:"child_partitions_record" json:"child_partitions_record"`
}

type dataChangeRecord struct {
	CommitTimestamp                      time.Time     `spanner:"commit_timestamp" json:"commit_timestamp"`
	RecordSequence                       string        `spanner:"record_sequence" json:"record_sequence"`
	ServerTransactionID                  string        `spanner:"server_transaction_id" json:"server_transaction_id"`
	IsLastRecordInTransactionInPartition bool          `spanner:"is_last_record_in_transaction_in_partition" json:"is_last_record_in_transaction_in_partition"`
	TableName                            string        `spanner:"table_name" json:"table_name"`
	ColumnTypes                          []*columnType `spanner:"column_types" json:"column_types"`
	Mods                                 []*mod        `spanner:"mods" json:"mods"`
	ModType                              string        `spanner:"mod_type" json:"mod_type"`
	ValueCaptureType                     string        `spanner:"value_capture_type" json:"value_capture_type"`
	NumberOfRecordsInTransaction         int64         `spanner:"number_of_records_in_transaction" json:"number_of_records_in_transaction"`
	NumberOfPartitionsInTransaction      int64         `spanner:"number_of_partitions_in_transaction" json:"number_of_partitions_in_transaction"`
	TransactionTag                       string        `spanner:"transaction_tag" json:"transaction_tag"`
	IsSystemTransaction                  bool          `spanner:"is_system_transaction" json:"is_system_transaction"`
}

type columnType struct {
	Name            string           `spanner:"name" json:"name"`
	Type            spanner.NullJSON `spanner:"type" json:"type"`
	IsPrimaryKey    bool             `spanner:"is_primary_key" json:"is_primary_key"`
	OrdinalPosition int64            `spanner:"ordinal_position" json:"ordinal_position"`
}

type mod struct {
	Keys      spanner.NullJSON `spanner:"keys" json:"keys"`
	NewValues spanner.NullJSON `spanner:"new_values" json:"new_values"`
	OldValues spanner.NullJSON `spanner:"old_values" json:"old_values"`
}

// DataChangeRecord is the change set of the table.
type DataChangeRecord struct {
	CommitTimestamp                      time.Time     `json:"commit_timestamp"`
	RecordSequence                       string        `json:"record_sequence"`
	ServerTransactionID                  string        `json:"server_transaction_id"`
	IsLastRecordInTransactionInPartition bool          `json:"is_last_record_in_transaction_in_partition"`
	TableName                            string        `json:"table_name"`
	ColumnTypes                          []*ColumnType `json:"column_types"`
	Mods                                 []*Mod        `json:"mods"`
	ModType                              ModType       `json:"mod_type"`
	ValueCaptureType                     string        `json:"value_capture_type"`
	NumberOfRecordsInTransaction         int64         `json:"number_of_records_in_transaction"`
	NumberOfPartitionsInTransaction      int64         `json:"number_of_partitions_in_transaction"`
	TransactionTag                       string        `json:"transaction_tag"`
	IsSystemTransaction                  bool          `json:"is_system_transaction"`
}

// ColumnType is the metadata of the column.
type ColumnType struct {
	Name            string `json:"name"`
	Type            Type   `json:"type"`
	IsPrimaryKey    bool   `json:"is_primary_key,omitempty"`
	OrdinalPosition int64  `json:"ordinal_position"`
}

// Type is the type of the column.
type Type struct {
	Code             TypeCode `json:"code"`
	ArrayElementType TypeCode `json:"array_element_type,omitempty"`
}

// TypeCode defines the type of the column.
type TypeCode string

// TypeCode constants.
const (
	TypeCodeNONE      TypeCode = ""
	TypeCodeBOOL      TypeCode = "BOOL"
	TypeCodeINT64     TypeCode = "INT64"
	TypeCodeFLOAT64   TypeCode = "FLOAT64"
	TypeCodeTIMESTAMP TypeCode = "TIMESTAMP"
	TypeCodeDATE      TypeCode = "DATE"
	TypeCodeSTRING    TypeCode = "STRING"
	TypeCodeBYTES     TypeCode = "BYTES"
	TypeCodeNUMERIC   TypeCode = "NUMERIC"
	TypeCodeJSON      TypeCode = "JSON"
	TypeCodeARRAY     TypeCode = "ARRAY"
)

// Mod contains the keys and the values of the changed records.
type Mod struct {
	Keys      map[string]interface{} `json:"keys,omitempty"`
	NewValues map[string]interface{} `json:"new_values,omitempty"`
	OldValues map[string]interface{} `json:"old_values,omitempty"`
}

// ModType is the type of the modification.
type ModType string

// ModTypes available for use.
const (
	ModTypeINSERT = "INSERT"
	ModTypeUPDATE = "UPDATE"
	ModTypeDELETE = "DELETE"
)

// AllModTypes contains all the available ModTypes.
var AllModTypes = []ModType{ModTypeINSERT, ModTypeUPDATE, ModTypeDELETE}

// HeartbeatRecord stores the timestamp of the heartbeat.
type HeartbeatRecord struct {
	Timestamp time.Time `spanner:"timestamp" json:"timestamp"`
}

// ChildPartitionsRecord stores the child partitions.
type ChildPartitionsRecord struct {
	StartTimestamp  time.Time         `spanner:"start_timestamp" json:"start_timestamp"`
	RecordSequence  string            `spanner:"record_sequence" json:"record_sequence"`
	ChildPartitions []*ChildPartition `spanner:"child_partitions" json:"child_partitions"`
}

// ChildPartition stores the child partition.
type ChildPartition struct {
	Token                 string   `spanner:"token" json:"token"`
	ParentPartitionTokens []string `spanner:"parent_partition_tokens" json:"parent_partition_tokens"`
}

// DecodeToNonSpannerType decodes the data change record to the non-spanner type.
func (r *dataChangeRecord) DecodeToNonSpannerType() *DataChangeRecord {
	columnTypes := []*ColumnType{}
	for _, t := range r.ColumnTypes {
		columnTypes = append(columnTypes, &ColumnType{
			Name:            t.Name,
			Type:            decodeColumnTypeJSONToType(t.Type),
			IsPrimaryKey:    t.IsPrimaryKey,
			OrdinalPosition: t.OrdinalPosition,
		})
	}

	mods := make([]*Mod, 0, len(r.Mods))
	for _, m := range r.Mods {
		mods = append(mods, &Mod{
			Keys:      decodeNullJSONToMap(m.Keys),
			NewValues: decodeNullJSONToMap(m.NewValues),
			OldValues: decodeNullJSONToMap(m.OldValues),
		})
	}

	return &DataChangeRecord{
		CommitTimestamp:                      r.CommitTimestamp,
		RecordSequence:                       r.RecordSequence,
		ServerTransactionID:                  r.ServerTransactionID,
		IsLastRecordInTransactionInPartition: r.IsLastRecordInTransactionInPartition,
		TableName:                            r.TableName,
		ColumnTypes:                          columnTypes,
		Mods:                                 mods,
		ModType:                              ModType(r.ModType),
		ValueCaptureType:                     r.ValueCaptureType,
		NumberOfRecordsInTransaction:         r.NumberOfRecordsInTransaction,
		NumberOfPartitionsInTransaction:      r.NumberOfPartitionsInTransaction,
		TransactionTag:                       r.TransactionTag,
		IsSystemTransaction:                  r.IsSystemTransaction,
	}
}

func decodeColumnTypeJSONToType(columnType spanner.NullJSON) Type {
	m := columnType.Value.(map[string]interface{})
	code := TypeCode(m["code"].(string))

	if aet, ok := m["array_element_type"].(map[string]interface{}); ok {
		arrayElementType := TypeCode(aet["code"].(string))
		return Type{
			Code:             code,
			ArrayElementType: arrayElementType,
		}
	}

	return Type{Code: code}
}

func decodeNullJSONToMap(j spanner.NullJSON) map[string]interface{} {
	if j.IsNull() {
		return nil
	}
	return j.Value.(map[string]interface{})
}
