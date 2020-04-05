package expression

import "strconv"

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

type dynamicResolverFunc func(index int, msg Message, escaped, legacy bool) []byte

type dynamicResolver dynamicResolverFunc

func (f dynamicResolver) ResolveString(index int, msg Message, escaped, legacy bool) string {
	return string(f.ResolveBytes(index, msg, escaped, legacy))
}

func (f dynamicResolver) ResolveBytes(index int, msg Message, escaped, legacy bool) []byte {
	if escaped {
		return escapeBytes(f(index, msg, escaped, legacy))
	}
	return f(index, msg, escaped, legacy)
}

func escapeBytes(in []byte) []byte {
	quoted := strconv.Quote(string(in))
	if len(quoted) < 3 {
		return in
	}
	return []byte(quoted[1 : len(quoted)-1])
}

//------------------------------------------------------------------------------
