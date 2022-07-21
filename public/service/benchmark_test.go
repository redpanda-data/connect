package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/require"

	_ "github.com/benthosdev/benthos/v4/internal/impl/lang"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func BenchmarkStreamPipelines(b *testing.B) {
	for name, confFn := range map[string]func(int) string{
		"basic pipeline": func(iterations int) string {
			return fmt.Sprintf(`
input:
  generate:
    count: %v
    interval: ""
    mapping: |
      root.id = uuid_v4()

output:
  drop: {}
`, iterations)
		},
		"basic pipeline batched": func(iterations int) string {
			batchCount := 20
			messageCount := iterations / batchCount
			if messageCount <= 0 {
				messageCount = 1
			}
			return fmt.Sprintf(`
input:
  generate:
    count: %v
    interval: ""
    mapping: |
      root = range(0, %v).map_each({"id": uuid_v4()})
  processors:
   - unarchive:
       format: json_array

output:
  drop: {}
`, messageCount, batchCount)
		},
		"basic mapping": func(iterations int) string {
			return fmt.Sprintf(`
input:
  generate:
    count: %v
    interval: ""
    mapping: |
      root.id = uuid_v4()
      root.name = fake("name")
      root.mobile = fake("phone_number")
      root.site = fake("url")
      root.email = fake("email")
      root.friends = range(0, (random_int() %% 10) + 1).map_each(fake("name"))

pipeline:
  processors:
    - bloblang: |
        root = this
        root.loud_name = this.name.uppercase()
        root.good_friends = this.friends.filter(f -> f.lowercase().contains("a"))

output:
  drop: {}
`, iterations)
		},
		"basic multiplexing": func(iterations int) string {
			return fmt.Sprintf(`
input:
  generate:
    count: %v
    interval: ""
    mapping: |
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
`, iterations)
		},
		"basic switch processor": func(iterations int) string {
			return fmt.Sprintf(`
input:
  generate:
    count: %v
    interval: ""
    mapping: |
      root.id = uuid_v4()

pipeline:
  processors:
    - switch:
        - check: this.id.contains("a")
          processors:
            - bloblang: 'root = content().uppercase()'
        - check: this.id.contains("b")
          processors:
            - bloblang: 'root = content().uppercase()'
        - check: this.id.contains("c")
          processors:
            - bloblang: 'root = content().uppercase()'

output:
  drop: {}
`, iterations)
		},
		"convoluted data generation": func(iterations int) string {
			return fmt.Sprintf(`
input:
  generate:
    count: %v
    interval: ""
    mapping: |
      root.id = uuid_v4()
      root.name = fake("name")
      root.mobile = fake("phone_number")
      root.site = fake("url")
      root.email = fake("email")
      root.friends = range(0, (random_int() %% 10) + 1).map_each(fake("name"))
      root.meows = range(0, (random_int() %% 10) + 1).map_each({
        nanoid(): fake("name")
      }).fold({}, item -> item.tally.merge(item.value))

output:
  drop: {}
`, iterations)
		},
		"large data mapping": func(iterations int) string {
			return fmt.Sprintf(`
input:
  generate:
    count: %v
    interval: ""
    mapping: |
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
    - bloblang: |
        root = this
        root.loud_name = this.name.uppercase()
        root.good_friends = this.friends.filter(f -> f.lowercase().contains("a"))
        root.meows = this.meows.map_each_key(key -> key.uppercase())

output:
  drop: {}
`, iterations)
		},
	} {
		confFn := confFn
		b.Run(name, func(b *testing.B) {
			builder := service.NewStreamBuilder()
			require.NoError(b, builder.SetYAML(confFn(b.N)))
			require.NoError(b, builder.SetLoggerYAML(`level: none`))

			strm, err := builder.Build()
			require.NoError(b, err)

			ctx, done := context.WithTimeout(context.Background(), time.Second*30)
			defer done()

			require.NoError(b, strm.Run(ctx))
		})
	}
}
