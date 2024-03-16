package parser

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

func rootLevelIfExpressionParser(pCtx Context) Func[mapping.Statement] {
	return func(input []rune) Result[mapping.Statement] {
		ifParser := Sequence(
			FuncAsAny(Expect(Term("if"), "assignment")),
			FuncAsAny(SpacesAndTabs),
			FuncAsAny(MustBe(queryParser(pCtx))),
			FuncAsAny(DiscardedWhitespaceNewlineComments),
			FuncAsAny(DelimitedPattern(
				Sequence(
					charSquigOpen,
					DiscardedWhitespaceNewlineComments,
				),
				mappingStatement(pCtx, true, nil),
				Sequence(
					Discard(SpacesAndTabs),
					NewlineAllowComment,
					DiscardedWhitespaceNewlineComments,
				),
				Sequence(
					DiscardedWhitespaceNewlineComments,
					charSquigClose,
				),
			)),
		)

		elseIfParser := Optional(Sequence(
			FuncAsAny(DiscardedWhitespaceNewlineComments),
			FuncAsAny(Term("else if")),
			FuncAsAny(SpacesAndTabs),
			FuncAsAny(MustBe(queryParser(pCtx))),
			FuncAsAny(DiscardedWhitespaceNewlineComments),
			FuncAsAny(DelimitedPattern(
				Sequence(
					charSquigOpen,
					DiscardedWhitespaceNewlineComments,
				),
				mappingStatement(pCtx, true, nil),
				Sequence(
					Discard(SpacesAndTabs),
					NewlineAllowComment,
					DiscardedWhitespaceNewlineComments,
				),
				Sequence(
					DiscardedWhitespaceNewlineComments,
					charSquigClose,
				),
			)),
		))

		elseParser := Optional(Sequence(
			FuncAsAny(DiscardedWhitespaceNewlineComments),
			FuncAsAny(Term("else")),
			FuncAsAny(DiscardedWhitespaceNewlineComments),
			FuncAsAny(DelimitedPattern(
				Sequence(
					charSquigOpen,
					DiscardedWhitespaceNewlineComments,
				),
				mappingStatement(pCtx, true, nil),
				Sequence(
					Discard(SpacesAndTabs),
					NewlineAllowComment,
					DiscardedWhitespaceNewlineComments,
				),
				Sequence(
					DiscardedWhitespaceNewlineComments,
					charSquigClose,
				),
			)),
		))

		res := ifParser(input)
		if res.Err != nil {
			return Fail[mapping.Statement](res.Err, input)
		}

		seqSlice := res.Payload
		stmt := mapping.NewRootLevelIfStatement(input)
		stmt.Add(seqSlice[2].(query.Function), seqSlice[4].([]mapping.Statement)...)

		for {
			res = elseIfParser(res.Remaining)
			if res.Err != nil {
				return Fail[mapping.Statement](res.Err, input)
			}
			if res.Payload == nil {
				break
			}
			seqSlice = res.Payload
			stmt.Add(seqSlice[3].(query.Function), seqSlice[5].([]mapping.Statement)...)
		}

		res = elseParser(res.Remaining)
		if res.Err != nil {
			return Fail[mapping.Statement](res.Err, input)
		}
		if seqSlice = res.Payload; seqSlice != nil {
			stmt.Add(nil, seqSlice[3].([]mapping.Statement)...)
		}
		return Success[mapping.Statement](stmt, res.Remaining)
	}
}
