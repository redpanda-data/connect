package mapping

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/value"
)

type Statement interface {
	QueryTargets(ctx query.TargetsContext) (query.TargetsContext, []query.TargetPath)
	AssignmentTargets() []TargetPath
	Input() []rune
	Execute(fnContext query.FunctionContext, asContext AssignmentContext) error
}

//------------------------------------------------------------------------------

type SingleStatement struct {
	input      []rune
	assignment Assignment
	query      query.Function
}

func NewSingleStatement(input []rune, assignment Assignment, query query.Function) *SingleStatement {
	return &SingleStatement{
		input:      input,
		assignment: assignment,
		query:      query,
	}
}

func (s *SingleStatement) QueryTargets(ctx query.TargetsContext) (query.TargetsContext, []query.TargetPath) {
	return s.query.QueryTargets(ctx)
}

func (s *SingleStatement) AssignmentTargets() []TargetPath {
	return []TargetPath{s.assignment.Target()}
}

func (s *SingleStatement) Input() []rune {
	return s.input
}

func (s *SingleStatement) Execute(fnContext query.FunctionContext, asContext AssignmentContext) error {
	res, err := s.query.Exec(fnContext)
	if err != nil {
		return err
	}
	if _, isNothing := res.(value.Nothing); isNothing {
		// Skip assignment entirely
		return nil
	}
	return s.assignment.Apply(res, asContext)
}

//------------------------------------------------------------------------------

type rootLevelIfStatementPair struct {
	query      query.Function
	statements []Statement
}

type RootLevelIfStatement struct {
	input []rune
	pairs []rootLevelIfStatementPair
}

func NewRootLevelIfStatement(input []rune) *RootLevelIfStatement {
	return &RootLevelIfStatement{
		input: input,
	}
}

func (r *RootLevelIfStatement) Add(query query.Function, statements ...Statement) *RootLevelIfStatement {
	r.pairs = append(r.pairs, rootLevelIfStatementPair{query: query, statements: statements})
	return r
}

func (r *RootLevelIfStatement) QueryTargets(ctx query.TargetsContext) (query.TargetsContext, []query.TargetPath) {
	var paths []query.TargetPath
	for _, p := range r.pairs {
		if p.query != nil {
			_, tmp := p.query.QueryTargets(ctx)
			paths = append(paths, tmp...)
		}
		for _, s := range p.statements {
			_, tmp := s.QueryTargets(ctx)
			paths = append(paths, tmp...)
		}
	}
	return ctx, paths
}

func (r *RootLevelIfStatement) AssignmentTargets() []TargetPath {
	var paths []TargetPath
	for _, p := range r.pairs {
		for _, s := range p.statements {
			paths = append(paths, s.AssignmentTargets()...)
		}
	}
	return paths
}

func (r *RootLevelIfStatement) Input() []rune {
	return r.input
}

func (r *RootLevelIfStatement) Execute(fnContext query.FunctionContext, asContext AssignmentContext) error {
	for i, p := range r.pairs {
		if p.query != nil {
			queryVal, err := p.query.Exec(fnContext)
			if err != nil {
				return fmt.Errorf("failed to check if condition %v: %w", i+1, err)
			}
			queryRes, isBool := queryVal.(bool)
			if !isBool {
				return fmt.Errorf("%v resolved to a non-boolean value %v (%T)", p.query.Annotation(), queryVal, queryVal)
			}
			if !queryRes {
				continue
			}
		}
		for _, stmt := range p.statements {
			if err := stmt.Execute(fnContext, asContext); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}
