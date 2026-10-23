package parquet

import (
	"fmt"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// wideCommonSchema builds a schema of ~1,200 leaves: groups of mixed leaves,
// optional timestamps, and an array of objects per group.
func wideCommonSchema() (schema.Common, func(int) map[string]any) {
	const groups, leaves = 40, 25
	root := schema.Common{Name: "root", Type: schema.Object}
	for g := range groups {
		obj := schema.Common{Name: fmt.Sprintf("group_%02d", g), Type: schema.Object, Optional: true}
		for l := range leaves {
			name := fmt.Sprintf("f_%02d", l)
			switch l % 3 {
			case 0:
				obj.Children = append(obj.Children, schema.Common{Name: name, Type: schema.String, Optional: true})
			case 1:
				obj.Children = append(obj.Children, schema.Common{Name: name, Type: schema.Int64, Optional: true})
			default:
				obj.Children = append(obj.Children, schema.Common{Name: name, Type: schema.Timestamp, Optional: true})
			}
		}
		obj.Children = append(obj.Children, schema.Common{
			Name: "items", Type: schema.Array, Optional: true,
			Children: []schema.Common{{Type: schema.Object, Children: []schema.Common{
				{Name: "k", Type: schema.String, Optional: true},
				{Name: "v", Type: schema.Int64, Optional: true},
			}}},
		})
		root.Children = append(root.Children, obj)
	}
	ts := time.Unix(1_700_000_000, 0).UTC()
	row := func(i int) map[string]any {
		r := map[string]any{}
		for g := range groups {
			obj := map[string]any{}
			for l := range leaves {
				name := fmt.Sprintf("f_%02d", l)
				switch l % 3 {
				case 0:
					obj[name] = fmt.Sprintf("v%d", i)
				case 1:
					obj[name] = int64(i + l)
				default:
					obj[name] = ts
				}
			}
			obj["items"] = []any{map[string]any{"k": "a", "v": int64(1)}, map[string]any{"k": "b", "v": int64(2)}}
			r[fmt.Sprintf("group_%02d", g)] = obj
		}
		return r
	}
	return root, row
}

func BenchmarkParquetEncodeWideSchema(b *testing.B) {
	const rows = 2500
	common, mkRow := wideCommonSchema()
	meta := common.ToAny()
	batch := make(service.MessageBatch, rows)
	for i := range batch {
		batch[i] = service.NewMessage(nil)
		batch[i].SetStructuredMut(mkRow(i))
		batch[i].MetaSetMut("schema", meta)
	}
	proc, err := newParquetEncodeProcessor(nil, nil, "schema", &parquet.Zstd, parquet.Microsecond)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := proc.ProcessBatch(b.Context(), batch); err != nil {
			b.Fatal(err)
		}
	}
}
