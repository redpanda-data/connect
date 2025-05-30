// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
//
// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package changestreams

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodePostgresRow(t *testing.T) {
	tests := []struct {
		desc             string
		changeRecordJSON string
		want             ChangeRecord
	}{
		{
			desc: "child partitions record",
			changeRecordJSON: `
{
  "child_partitions_record": {
    "start_timestamp": "2023-02-24T01:06:48.000000-08:00",
    "record_sequence": "00000001",
    "child_partitions": [
      {
        "token": "__8BAYEG0qQD8AABgsBDg3BsYXllcnNzdHJlYW0AAYSBAIKAgwjDZAAAAAAAbYQEHbHBFIVnMF8wAAH__4X_BfVuWWHW8ob_BfVu9ITI-Yf_BfVuWWHW8sBkAQH__w",
        "parent_partition_tokens": []
      }
    ]
  }
}`,
			want: ChangeRecord{
				ChildPartitionsRecords: []*ChildPartitionsRecord{
					{
						StartTimestamp: mustParseTime("2023-02-24T01:06:48.000000-08:00"),
						RecordSequence: "00000001",
						ChildPartitions: []*ChildPartition{
							{
								Token:                 "__8BAYEG0qQD8AABgsBDg3BsYXllcnNzdHJlYW0AAYSBAIKAgwjDZAAAAAAAbYQEHbHBFIVnMF8wAAH__4X_BfVuWWHW8ob_BfVu9ITI-Yf_BfVuWWHW8sBkAQH__w",
								ParentPartitionTokens: []string{},
							},
						},
					},
				},
			},
		},
		{
			desc: "data change record",
			changeRecordJSON: `
{
  "data_change_record": {
    "column_types": [
      {
        "is_primary_key": true,
        "name": "playerid",
        "ordinal_position": 1,
        "type": {
          "code": "INT64"
        }
      },
      {
        "is_primary_key": false,
        "name": "playername",
        "ordinal_position": 2,
        "type": {
          "code": "STRING"
        }
      }
    ],
    "commit_timestamp": "2023-02-24T17:17:00.678847-08:00",
    "is_last_record_in_transaction_in_partition": true,
    "is_system_transaction": false,
    "mod_type": "INSERT",
    "mods": [
      {
        "keys": {
          "playerid": "3"
        },
        "new_values": {
          "playername": "b"
        },
        "old_values": {}
      }
    ],
    "number_of_partitions_in_transaction": 1,
    "number_of_records_in_transaction": 1,
    "record_sequence": "00000000",
    "server_transaction_id": "NTQ5MTAxNjk2MzM2OTMxOTM5NQ==",
    "table_name": "players",
    "transaction_tag": "",
    "value_capture_type": "OLD_AND_NEW_VALUES"
  }
}`,
			want: ChangeRecord{
				DataChangeRecords: []*DataChangeRecord{
					{
						CommitTimestamp:                      mustParseTime("2023-02-24T17:17:00.678847-08:00"),
						IsLastRecordInTransactionInPartition: true,
						IsSystemTransaction:                  false,
						ModType:                              "INSERT",
						NumberOfRecordsInTransaction:         1,
						NumberOfPartitionsInTransaction:      1,
						RecordSequence:                       "00000000",
						ServerTransactionID:                  "NTQ5MTAxNjk2MzM2OTMxOTM5NQ==",
						TableName:                            "players",
						TransactionTag:                       "",
						ValueCaptureType:                     "OLD_AND_NEW_VALUES",
						ColumnTypes: []*ColumnType{
							{
								Name: "playerid",
								Type: spanner.NullJSON{
									Value: map[string]interface{}{"code": "INT64"},
									Valid: true,
								},
								IsPrimaryKey:    true,
								OrdinalPosition: 1,
							},
							{
								Name: "playername",
								Type: spanner.NullJSON{
									Value: map[string]interface{}{"code": "STRING"},
									Valid: true,
								},
								IsPrimaryKey:    false,
								OrdinalPosition: 2,
							},
						},
						Mods: []*Mod{
							{
								Keys: spanner.NullJSON{
									Value: map[string]interface{}{"playerid": "3"},
									Valid: true,
								},
								NewValues: spanner.NullJSON{
									Value: map[string]interface{}{"playername": "b"},
									Valid: true,
								},
								OldValues: spanner.NullJSON{
									Value: map[string]interface{}{},
									Valid: true,
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "heartbeat record",
			changeRecordJSON: `
{
  "heartbeat_record": {
    "timestamp": "2023-02-24T17:16:43.811345-08:00"
  }
}`,
			want: ChangeRecord{
				HeartbeatRecords: []*HeartbeatRecord{
					{
						Timestamp: mustParseTime("2023-02-24T17:16:43.811345-08:00"),
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			var jsonVal interface{}
			require.NoError(t, json.Unmarshal([]byte(test.changeRecordJSON), &jsonVal))

			row, err := spanner.NewRow([]string{"read_json_playersstream"}, []interface{}{spanner.NullJSON{
				Valid: true,
				Value: jsonVal,
			}})
			require.NoError(t, err)

			got, err := decodePostgresRow(row)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func mustParseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(fmt.Sprintf("invalid time %q: %v", value, err))
	}
	return t
}
