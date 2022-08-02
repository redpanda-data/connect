package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/benthosdev/benthos/v4/internal/impl/lang"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func BenchmarkStreamPipelines(b *testing.B) {
	for _, test := range []struct {
		name   string
		confFn func(iterations, batchSize int) string
	}{
		{
			name: "basic pipeline",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "pipeline processors chained",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()
      root.name = fake("name")
      root.mobile = fake("phone_number")
      root.site = fake("url")

pipeline:
  processors:
    - jq:
        query: '{id: .id, name: .name, mobile: .mobile, site: .site}'
    - jq:
        query: '{id: .id, name: .name, mobile: .mobile, site: .site}'
    - jq:
        query: '{id: .id, name: .name, mobile: .mobile, site: .site}'

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "basic mapping",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()
      root.name = fake("name")
      root.mobile = fake("phone_number")
      root.site = fake("url")
      root.email = fake("email")
      root.friends = range(0, (random_int() %% 10) + 1).map_each(fake("name"))

pipeline:
  processors:
    - mapping: |
        root = this
        root.loud_name = this.name.uppercase()
        root.good_friends = this.friends.filter(f -> f.lowercase().contains("a"))

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "basic mapping inline",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()
      root.name = fake("name")
      root.mobile = fake("phone_number")
      root.site = fake("url")
      root.email = fake("email")
      root.friends = range(0, (random_int() %% 10) + 1).map_each(fake("name"))

pipeline:
  processors:
    - mutation: |
        root.loud_name = this.name.uppercase()
        root.good_friends = this.friends.filter(f -> f.lowercase().contains("a"))

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "basic mapping as input proc",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()
      root.name = fake("name")
      root.mobile = fake("phone_number")
      root.site = fake("url")
      root.email = fake("email")
      root.friends = range(0, (random_int() %% 10) + 1).map_each(fake("name"))
  processors:
    - mapping: |
        root = this
        root.loud_name = this.name.uppercase()
        root.good_friends = this.friends.filter(f -> f.lowercase().contains("a"))

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "basic mapping inline split with input proc",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()
      root.name = fake("name")
      root.mobile = fake("phone_number")
      root.site = fake("url")
      root.email = fake("email")
      root.friends = range(0, (random_int() %% 10) + 1).map_each(fake("name"))
  processors:
    - mutation: |
        root.loud_name = this.name.uppercase()

pipeline:
  processors:
    - mutation: |
        root.good_friends = this.friends.filter(f -> f.lowercase().contains("a"))

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "basic mapping as branch",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()
      root.name = fake("name")
      root.mobile = fake("phone_number")
      root.site = fake("url")
      root.email = fake("email")
      root.friends = range(0, (random_int() %% 10) + 1).map_each(fake("name"))

pipeline:
  processors:
    - branch:
        processors: [ noop: {} ]
        result_map: |
          root.loud_name = this.name.uppercase()
          root.good_friends = this.friends.filter(f -> f.lowercase().contains("a"))

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "basic multiplexing",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()

output:
  switch:
    cases:
      - check: this.id.contains("a")
        output:
          drop: {}
      - check: this.id.contains("b")
        output:
          drop: {}
      - check: this.id.contains("c")
        output:
          drop: {}
      - output:
          drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "basic switch processor",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()

pipeline:
  processors:
    - switch:
        - check: this.id.contains("a")
          processors:
            - mapping: 'root = content().uppercase()'
        - check: this.id.contains("b")
          processors:
            - mapping: 'root = content().uppercase()'
        - check: this.id.contains("c")
          processors:
            - mapping: 'root = content().uppercase()'

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "convoluted data generation",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()
      root.name = fake("name")
      root.mobile = fake("phone_number")
      root.site = fake("url")
      root.email = fake("email")
      root.friends = range(0, (random_int() %% 10) + 1).map_each(fake("name"))
      root.meows = range(0, (random_int() %% 10) + 1).fold({}, item -> item.tally.merge({
        nanoid(): fake("name")
      }))

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "large data mapping",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":"bar value"}
      root.id = uuid_v4()
      root.name = fake("name")
      root.mobile = fake("phone_number")
      root.site = fake("url")
      root.email = fake("email")
      root.friends = range(0, (random_int() %% 10) + 1).map_each(fake("name"))
      root.meows = {
        nanoid(): fake("name"),
        nanoid(): fake("name"),
        nanoid(): fake("name"),
        nanoid(): fake("name"),
        nanoid(): fake("name"),
      }

pipeline:
  processors:
    - mapping: |
        root = this
        root.loud_name = this.name.uppercase()
        root.good_friends = this.friends.filter(f -> f.lowercase().contains("a"))
        root.meows = this.meows.map_each_key(key -> key.uppercase())

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "basic branch processors",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":(random_int()%%10).string()}
      root.id = uuid_v4()
      root.name = fake("name")
      root.email = fake("email")

pipeline:
  processors:
    - branch:
        request_map: |
          root.foo = meta("foo")
          root.email = this.email
        processors:
          - mapping: root = content().uppercase()
        result_map: |
          root.foo_stuff = content().string()
    - branch:
        request_map: |
          root.bar = meta("bar")
          root.name = this.name
        processors:
          - mapping: root = content().uppercase()
        result_map: |
          root.bar_stuff = content().string()

output:
  drop: {}
`, iterations, batchSize)
			},
		},
		{
			name: "basic workflow processors",
			confFn: func(iterations, batchSize int) string {
				return fmt.Sprintf(`
input:
  generate:
    count: %v
    batch_size: %v
    interval: ""
    mapping: |
      meta = {"foo":"foo value","bar":(random_int()%%10).string()}
      root.id = uuid_v4()
      root.name = fake("name")
      root.email = fake("email")

pipeline:
  processors:
    - workflow:
        branches:
          foo_stuff:
            request_map: |
              root.foo = meta("foo")
              root.email = this.email
            processors:
              - mapping: root = content().uppercase()
            result_map: |
              root.foo_stuff = content().string()
          bar_stuff:
            request_map: |
              root.bar = meta("bar")
              root.name = this.name
            processors:
              - mapping: root = content().uppercase()
            result_map: |
              root.bar_stuff = content().string()

output:
  drop: {}
`, iterations, batchSize)
			},
		},
	} {
		test := test
		for _, batchSize := range []int{1, 10, 50} {
			batchSize := batchSize
			b.Run(fmt.Sprintf("%v/%v", test.name, batchSize), func(b *testing.B) {
				iterations := b.N / batchSize
				if iterations < 1 {
					iterations = 1
				}

				builder := service.NewStreamBuilder()
				require.NoError(b, builder.SetYAML(test.confFn(iterations, batchSize)))
				require.NoError(b, builder.SetLoggerYAML(`level: none`))

				strm, err := builder.Build()
				require.NoError(b, err)

				ctx, done := context.WithTimeout(context.Background(), time.Second*30)
				defer done()

				b.ReportAllocs()
				b.ResetTimer()

				require.NoError(b, strm.Run(ctx))
			})
		}
	}
}
