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

// nestedMutexLock flags Lock/RLock/Unlock/RUnlock calls on chained selectors
// (e.g. x.y.mu.Lock()). Mutex operations should only be called on a direct
// field (x.mu.Lock()) or local variable (mu.Lock()), never by reaching into
// another struct's internals. sync.Cond.L is excluded as a legitimate stdlib
// pattern.
func nestedMutexLock(m dsl.Matcher) {
	m.Match(`$x.Lock()`, `$x.Unlock()`, `$x.RLock()`, `$x.RUnlock()`).
		Where(m["x"].Text.Matches(`\w+\.\w+\.\w+`) && !m["x"].Text.Matches(`\.cond\.L$`)).
		Report(`do not lock a mutex through a chained selector ($x); mutex operations should only be called on direct fields`)
}
