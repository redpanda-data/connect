package parquet

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestParquetProcessorConfigLinting(t *testing.T) {
	configTests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "missing operator",
			config: `
parquet:
  schema: '{}'
`,
			errContains: `field operator is required`,
		},
		{
			name: "invalid operator",
			config: `
parquet:
  operator: not_real
  schema: no
`,
			errContains: `value not_real is not a valid`,
		},
	}

	env := service.NewEnvironment()
	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			strm := env.NewStreamBuilder()
			err := strm.AddProcessorYAML(test.config)
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func TestParquetProcessorConfigParse(t *testing.T) {
	tmpSchemaFile, err := os.CreateTemp("", "benthos_parquet_test")
	require.NoError(t, err)

	_, err = tmpSchemaFile.WriteString(`{
  "Tag": "name=root, repetitiontype=REQUIRED",
  "Fields": [
    {"Tag": "name=name, inname=NameIn, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
    {"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"},
    {"Tag": "name=id, inname=Id, type=INT64, repetitiontype=REQUIRED"}
  ]
}`)
	require.NoError(t, err)

	configTests := []struct {
		name        string
		config      string
		schema      string
		errContains string
	}{
		{
			name: "no schema or schema file",
			config: `
operator: to_json
`,
			errContains: "either a raw `schema` or a non-empty `schema_file` must be specified",
		},
		{
			name: "raw schema",
			config: `
operator: to_json
schema: |
  {
    "Tag": "name=root, repetitiontype=REQUIRED",
    "Fields": [
      {"Tag": "name=name, inname=NameIn, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
      {"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"},
      {"Tag": "name=id, inname=Id, type=INT64, repetitiontype=REQUIRED"}
    ]
  }
`,
			schema: `{
  "Tag": "name=root, repetitiontype=REQUIRED",
  "Fields": [
    {"Tag": "name=name, inname=NameIn, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
    {"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"},
    {"Tag": "name=id, inname=Id, type=INT64, repetitiontype=REQUIRED"}
  ]
}
`,
		},
		{
			name: "schema file",
			config: fmt.Sprintf(`
operator: to_json
schema_file: %v
`, tmpSchemaFile.Name()),
			schema: `{
  "Tag": "name=root, repetitiontype=REQUIRED",
  "Fields": [
    {"Tag": "name=name, inname=NameIn, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
    {"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"},
    {"Tag": "name=id, inname=Id, type=INT64, repetitiontype=REQUIRED"}
  ]
}`,
		},
	}

	confSpec := parquetProcessorConfig()
	env := service.NewEnvironment()

	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := confSpec.ParseYAML(test.config, env)
			require.NoError(t, err)

			proc, err := newParquetProcessorFromConfig(pConf, nil)
			if test.errContains == "" {
				require.NoError(t, err)
				assert.Equal(t, test.schema, proc.schema)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func TestParquetJSONSchemaRoundTrip(t *testing.T) {
	schema := `{
  "Tag": "name=root, repetitiontype=REQUIRED",
  "Fields": [
    {"Tag": "name=name, inname=NameIn, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
    {"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"},
    {"Tag": "name=id, inname=Id, type=INT64, repetitiontype=REQUIRED"},
    {"Tag": "name=weight, inname=Weight, type=FLOAT, repetitiontype=REQUIRED"},
    {
      "Tag": "name=favPokemon, inname=FavPokemon, type=LIST, repetitiontype=OPTIONAL",
      "Fields": [
        { "Tag": "name=element, repetitiontype=REQUIRED", "Fields": [
          { "Tag": "name=name, inname=PokeName, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED" },
          { "Tag": "name=coolness, inname=Coolness, type=FLOAT, repetitiontype=REQUIRED" }
        ] }
      ]
    }
  ]
}`

	inputDocs := []string{
		`{"NameIn":"fooer first","age":21,"id":1,"weight":60.1}`,
		`{"NameIn":"fooer second","age":22,"id":2,"weight":60.2}`,
		`{"NameIn":"fooer third","age":23,"id":3,"weight":60.3,"favPokemon":[{"PokeName":"bulbasaur","Coolness":99}]}`,
		`{"NameIn":"fooer fourth","age":24,"id":4,"weight":60.4}`,
		`{"NameIn":"fooer fifth","age":25,"id":5,"weight":60.5}`,
		`{"NameIn":"fooer sixth","age":26,"id":6,"weight":60.6}`,
	}

	// Test every compression codec
	for _, c := range []string{
		"uncompressed", "snappy", "gzip", "lz4", "zstd",
		// "lzo", "brotli", "lz4_raw",
	} {
		t.Run(fmt.Sprintf("with %v codec", c), func(t *testing.T) {
			writer, err := newParquetProcessor("from_json", c, schema, nil)
			require.NoError(t, err)

			reader, err := newParquetProcessor("to_json", "", schema, nil)
			require.NoError(t, err)

			var inputBatch service.MessageBatch
			for _, d := range inputDocs {
				inputBatch = append(inputBatch, service.NewMessage([]byte(d)))
			}

			writerResBatches, err := writer.ProcessBatch(context.Background(), inputBatch)
			require.NoError(t, err)
			require.Len(t, writerResBatches, 1)
			require.Len(t, writerResBatches[0], 1)

			readerResBatches, err := reader.ProcessBatch(context.Background(), writerResBatches[0])
			require.NoError(t, err)
			require.Len(t, writerResBatches, 1)

			var readerResStrs []string
			for _, m := range readerResBatches[0] {
				mBytes, err := m.AsBytes()
				require.NoError(t, err)
				readerResStrs = append(readerResStrs, string(mBytes))
			}

			assert.Equal(t, []string{
				`{"NameIn":"fooer first","Age":21,"Id":1,"Weight":60.1,"FavPokemon":null}`,
				`{"NameIn":"fooer second","Age":22,"Id":2,"Weight":60.2,"FavPokemon":null}`,
				`{"NameIn":"fooer third","Age":23,"Id":3,"Weight":60.3,"FavPokemon":[{"PokeName":"bulbasaur","Coolness":99}]}`,
				`{"NameIn":"fooer fourth","Age":24,"Id":4,"Weight":60.4,"FavPokemon":null}`,
				`{"NameIn":"fooer fifth","Age":25,"Id":5,"Weight":60.5,"FavPokemon":null}`,
				`{"NameIn":"fooer sixth","Age":26,"Id":6,"Weight":60.6,"FavPokemon":null}`,
			}, readerResStrs)
		})
	}
}
