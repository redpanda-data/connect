package field

import (
	"strconv"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

//------------------------------------------------------------------------------

// Resolver is an interface for resolving a string containing Bloblang function
// interpolations into either a string or bytes.
type Resolver interface {
	ResolveString(index int, msg Message, escaped, legacy bool) string
	ResolveBytes(index int, msg Message, escaped, legacy bool) []byte
}

//------------------------------------------------------------------------------

// StaticResolver is a Resolver implementation that simply returns a static
// string
type StaticResolver string

// ResolveString returns a string.
func (s StaticResolver) ResolveString(index int, msg Message, escaped, legacy bool) string {
	return string(s)
}

// ResolveBytes returns a byte slice.
func (s StaticResolver) ResolveBytes(index int, msg Message, escaped, legacy bool) []byte {
	return []byte(s)
}

//------------------------------------------------------------------------------

// QueryResolver executes a query and returns a string representation of the
// result.
type QueryResolver struct {
	fn query.Function
}

// NewQueryResolver creates a field query resolver that returns the result of a
// query function.
func NewQueryResolver(fn query.Function) *QueryResolver {
	return &QueryResolver{fn}
}

// ResolveString returns a string.
func (q QueryResolver) ResolveString(index int, msg Message, escaped, legacy bool) string {
	return query.ExecToString(q.fn, query.FunctionContext{
		Index:    index,
		MsgBatch: msg,
		Legacy:   legacy,
	})
}

// ResolveBytes returns a byte slice.
func (q QueryResolver) ResolveBytes(index int, msg Message, escaped, legacy bool) []byte {
	bs := query.ExecToBytes(q.fn, query.FunctionContext{
		Index:    index,
		MsgBatch: msg,
		Legacy:   legacy,
	})
	if escaped {
		bs = escapeBytes(bs)
	}
	return bs
}

func escapeBytes(in []byte) []byte {
	quoted := strconv.Quote(string(in))
	if len(quoted) < 3 {
		return in
	}
	return []byte(quoted[1 : len(quoted)-1])
}

//------------------------------------------------------------------------------
