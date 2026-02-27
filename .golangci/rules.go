package gorules

import "github.com/quasilyte/go-ruleguard/dsl"

// failedToError flags "failed to X" error messages and suggests gerund form ("Xing").
//
// Go convention: wrap errors with present participle, e.g. "opening file: ..."
// not "failed to open file: ...". See https://go.dev/wiki/CodeReviewComments#error-strings
//
// Autofix: go run ./cmd/tools/failed_to_lint
func failedToError(m dsl.Matcher) {
	m.Match(`fmt.Errorf($msg)`, `fmt.Errorf($msg, $*_)`).
		Where(m["msg"].Text.Matches(`.*failed to .*`)).
		Report(`use gerund error wrapping ("opening file") not "failed to" ("failed to open file"); autofix: go run ./cmd/tools/failed_to_lint`)

	m.Match(`errors.New($msg)`).
		Where(m["msg"].Text.Matches(`.*failed to .*`)).
		Report(`use gerund error wrapping ("opening file") not "failed to" ("failed to open file"); autofix: go run ./cmd/tools/failed_to_lint`)
}
