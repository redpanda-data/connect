package pure

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("compress",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryEncoding).
			Description(`Compresses a string or byte array value according to a specified algorithm.`).
			Param(bloblang.NewStringParam("algorithm").Description("One of `flate`, `gzip`, `pgzip`, `lz4`, `snappy`, `zlib`, `zstd`.")).
			Param(bloblang.NewInt64Param("level").Description("The level of compression to use. May not be applicable to all algorithms.").Default(-1)).
			Example("", `let long_content = range(0, 1000).map_each(content()).join(" ")
root.a_len = $long_content.length()
root.b_len = $long_content.compress("gzip").length()
`,
				[2]string{
					`hello world this is some content`,
					`{"a_len":32999,"b_len":161}`,
				},
			).
			Example("", `root.compressed = content().compress("lz4").encode("base64")`,
				[2]string{
					`hello world I love space`,
					`{"compressed":"BCJNGGRwuRgAAIBoZWxsbyB3b3JsZCBJIGxvdmUgc3BhY2UAAAAAGoETLg=="}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			level, err := args.GetInt64("level")
			if err != nil {
				return nil, err
			}
			algStr, err := args.GetString("algorithm")
			if err != nil {
				return nil, err
			}
			algFn, err := strToCompressFunc(algStr)
			if err != nil {
				return nil, err
			}
			return bloblang.BytesMethod(func(data []byte) (any, error) {
				return algFn(int(level), data)
			}), nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("decompress",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryEncoding).
			Description(`Decompresses a string or byte array value according to a specified algorithm. The result of decompression `).
			Param(bloblang.NewStringParam("algorithm").Description("One of `gzip`, `pgzip`, `zlib`, `bzip2`, `flate`, `snappy`, `lz4`, `zstd`.")).
			Example("", `root = this.compressed.decode("base64").decompress("lz4")`,
				[2]string{
					`{"compressed":"BCJNGGRwuRgAAIBoZWxsbyB3b3JsZCBJIGxvdmUgc3BhY2UAAAAAGoETLg=="}`,
					`hello world I love space`,
				},
			).
			Example(
				"Use the `.string()` method in order to coerce the result into a string, this makes it possible to place the data within a JSON document without automatic base64 encoding.",
				`root.result = this.compressed.decode("base64").decompress("lz4").string()`,
				[2]string{
					`{"compressed":"BCJNGGRwuRgAAIBoZWxsbyB3b3JsZCBJIGxvdmUgc3BhY2UAAAAAGoETLg=="}`,
					`{"result":"hello world I love space"}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			algStr, err := args.GetString("algorithm")
			if err != nil {
				return nil, err
			}
			algFn, err := strToDecompressFunc(algStr)
			if err != nil {
				return nil, err
			}
			return bloblang.BytesMethod(func(data []byte) (any, error) {
				return algFn(data)
			}), nil
		}); err != nil {
		panic(err)
	}
}
