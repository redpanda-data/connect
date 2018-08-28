// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package input

import (
	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMySQL] = TypeSpec{
		constructor: NewMySQL,
		description: `
Streams a MySQL binary replication log as a series of JSON events. Each event
includes top level fields containing the binlog file name, event offset, schema
name, table name, event timestamp, event type, map of primary keys, and an
optional row summary. A sample event is shown below:

` + "``` json" + `
{
	"file": "binlog.000003",
	"keys": {
		"id": 1
	},
	"pos": 670,
	"row": {
		"after": {
			"created_at": "2018-08-23T21:32:05.839348Z",
			"id": 1,
			"title": "foo"
		},
		"before": {
			"created_at": "2018-08-23T21:32:05.839348Z",
			"id": 1,
			"title": "bar"
		}
	},
	"schema": "test",
	"table": "foo",
	"timestamp": "2018-08-23T15:32:05Z",
	"type":"update"
}
` + "```" + `

This input type requires a durable cache for periodically checkpointing
its position in the replication stream. The checkpointing frequency can
be tuned using the ` + "`sync_interval`" + ` config field. It is also 
recommended to disable any cache TTL functionality.

This input supports both single (default) and batch message types. To
enable batch mode, set the ` + "`batch_size`" + ` config field to a 
value that is greater than 1. When operating in batch mode, the buffering
window can be adjusted by tuning the ` + "`buffer_timeout`" + ` config
field (default value is ` + "`1s`" + `).

### Row Summary
This input supports the following row summary view types that can be set
using the ` + "`view`" + ` config field.
- **KEYS_ONLY** - No row summary, primary key map only.
- **NEW_AND_OLD_IMAGES** - Includes an image of the record both before 
	and after it was modified.
- **NEW_IMAGE** - Includes an image of the record after it was modified.
- **OLD_IMAGE** - Includes an image of the record before it was modified.

### Starting Position & Content filters

This input supports the following starting positions:
- **last synchronized**
	- The default starting position uses the last synchronized checkpoint. 
	This position will take priority over all others. To disable, clear the 
	cache for this consumer.
- **dump**
	- This method begins by dumping the current db(s) using ` + "`mysqldump`" + `.
	This requires the ` + "`mysqldump`" + ` executable to be available on the
	host machine. To use this starting position, the ` + "`mysqldump_path`" + `
	config field must contain the path/to/mysqldump,
- **latest**
	- This method will begin streaming at the latest binlog position. Enable 
	this position using the ` + "`latest`" + ` config field.

A list of databases to subscribe to can be specified using the ` + "`databases`" + `
config field. An optional table filter can be specified using the ` + "`tables`" + `
config field as long as only a single database is specified.

### Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- mysql_event_keys
- mysql_event_schema
- mysql_event_table
- mysql_log_position
- mysql_event_type
- mysql_file
- mysql_next_pos
- mysql_server_id
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).
	`,
	}
}

//------------------------------------------------------------------------------

// NewMySQL creates a new NSQ input type.
func NewMySQL(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	cache, err := mgr.GetCache(conf.MySQL.Cache)
	if err != nil {
		return nil, types.ErrCacheNotFound
	}

	m, err := reader.NewMySQL(conf.MySQL, cache, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader(TypeMySQL, m, log, stats)
}

//------------------------------------------------------------------------------
