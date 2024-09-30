// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

type ChangeFilter struct {
	tablesWhiteList map[string]bool
	schemaWhiteList string
}

type Filtered func(change Wal2JsonChanges)

func NewChangeFilter(tableSchemas []string, schema string) ChangeFilter {
	tablesMap := map[string]bool{}
	for _, table := range tableSchemas {
		tablesMap[table] = true
	}

	return ChangeFilter{
		tablesWhiteList: tablesMap,
		schemaWhiteList: schema,
	}
}

func (c ChangeFilter) FilterChange(lsn string, changes WallMessage, OnFiltered Filtered) {
	if len(changes.Change) == 0 {
		return
	}

	for _, ch := range changes.Change {
		var filteredChanges = Wal2JsonChanges{
			Lsn:     &lsn,
			Changes: []Wal2JsonChange{},
		}
		if ch.Schema != c.schemaWhiteList {
			continue
		}

		var (
			tableExist bool
		)

		if _, tableExist = c.tablesWhiteList[ch.Table]; !tableExist {
			continue
		}

		if ch.Kind == "delete" {
			ch.Columnvalues = make([]interface{}, len(ch.Oldkeys.Keyvalues))
			for i, changedValue := range ch.Oldkeys.Keyvalues {
				if len(ch.Columnvalues) == 0 {
					break
				}
				ch.Columnvalues[i] = changedValue
			}
		}

		filteredChanges.Changes = append(filteredChanges.Changes, Wal2JsonChange{
			Kind:         ch.Kind,
			Schema:       ch.Schema,
			Table:        ch.Table,
			ColumnNames:  ch.Columnnames,
			ColumnTypes:  ch.Columntypes,
			ColumnValues: ch.Columnvalues,
		})

		OnFiltered(filteredChanges)
	}
}
