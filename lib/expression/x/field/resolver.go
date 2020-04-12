package field

import (
	"strconv"

	"github.com/Jeffail/benthos/v3/lib/expression/x/query"
)

//------------------------------------------------------------------------------

type resolver interface {
	ResolveString(index int, msg Message, escaped, legacy bool) string
	ResolveBytes(index int, msg Message, escaped, legacy bool) []byte
}

//------------------------------------------------------------------------------

type staticResolver string

func (s staticResolver) ResolveString(index int, msg Message, escaped, legacy bool) string {
	return string(s)
}

func (s staticResolver) ResolveBytes(index int, msg Message, escaped, legacy bool) []byte {
	return []byte(s)
}

//------------------------------------------------------------------------------

type queryResolver struct {
	fn query.Function
}

func (q queryResolver) ResolveString(index int, msg Message, escaped, legacy bool) string {
	return q.fn.ToString(index, msg, legacy)
}

func (q queryResolver) ResolveBytes(index int, msg Message, escaped, legacy bool) []byte {
	bs := q.fn.ToBytes(index, msg, legacy)
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
