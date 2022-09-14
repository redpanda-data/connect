package msgpack

import (
	"github.com/vmihailenco/msgpack/v5"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	// Note: The examples are run and tested from within
	// ./internal/bloblang/query/parsed_test.go

	msgpackParseSpec := bloblang.NewPluginSpec().
		Category("Parsing").
		Description("Parses a [MessagePack](https://msgpack.org/) message into a structured document.").
		Example("",
			`root = content().decode("hex").parse_msgpack()`,
			[2]string{
				`81a3666f6fa3626172`,
				`{"foo":"bar"}`,
			}).
		Example("",
			`root = this.encoded.decode("base64").parse_msgpack()`,
			[2]string{
				`{"encoded":"gaNmb2+jYmFy"}`,
				`{"foo":"bar"}`,
			})

	if err := bloblang.RegisterMethodV2(
		"parse_msgpack", msgpackParseSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(v any) (any, error) {
				b, err := query.IGetBytes(v)
				if err != nil {
					return nil, err
				}
				var jObj any
				if err := msgpack.Unmarshal(b, &jObj); err != nil {
					return nil, err
				}
				return jObj, nil
			}, nil
		},
	); err != nil {
		panic(err)
	}

	msgpackFormatSpec := bloblang.NewPluginSpec().
		Category("Parsing").
		Description("Formats data as a [MessagePack](https://msgpack.org/) message in bytes format.").
		Example("",
			`root = this.format_msgpack().encode("hex")`,
			[2]string{
				`{"foo":"bar"}`,
				`81a3666f6fa3626172`,
			}).
		Example("",
			`root.encoded = this.format_msgpack().encode("base64")`,
			[2]string{
				`{"foo":"bar"}`,
				`{"encoded":"gaNmb2+jYmFy"}`,
			})

	if err := bloblang.RegisterMethodV2(
		"format_msgpack", msgpackFormatSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(v any) (any, error) {
				return msgpack.Marshal(v)
			}, nil
		},
	); err != nil {
		panic(err)
	}
}
