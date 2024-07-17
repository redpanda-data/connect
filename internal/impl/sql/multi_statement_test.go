// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func assertSplitEquals(t *testing.T, message string, statement string, wanted []string) {
	result := splitSQLStatements(statement)
	assert.Equal(t, wanted, result, message)
}

func TestSplitStatements(t *testing.T) {
	assertSplitEquals(t, "no semicolon", "select null", []string{"select null"})

	assertSplitEquals(t, "basic semicolon", "select 1; select 2", []string{"select 1;", "select 2"})

	assertSplitEquals(t, "semicolon in single-quoted string",
		"select 'singlequoted;string'; select null",
		[]string{"select 'singlequoted;string';", "select null"})

	assertSplitEquals(t, "semicolon in double-quoted identifier",
		"select \"doublequoted;ident\"; select null",
		[]string{"select \"doublequoted;ident\";", "select null"})

	assertSplitEquals(t, "semicolon in backtick-quoted identifier",
		"select `backtick;ident`; select null",
		[]string{"select `backtick;ident`;", "select null"})

	assertSplitEquals(t, "semicolon in hash-comment", `
		select #hash;comment
		1; select 2
	`, []string{"select #hash;comment\n\t\t1;", "select 2"})

	assertSplitEquals(t, "semicolon in double-dash comment", `
		select --double-dash;comment
		1; select 2
	`, []string{"select --double-dash;comment\n\t\t1;", "select 2"})

	assertSplitEquals(t, "semicolon in double-slash comment", `
		select //double-slash;comment
		1; select 2
	`, []string{"select //double-slash;comment\n\t\t1;", "select 2"})

	assertSplitEquals(t, "semicolon in multi-line comment", `
		select /*multi;
		line;comment*/
		1; select 2
	`, []string{"select /*multi;\n\t\tline;comment*/\n\t\t1;", "select 2"})

	assertSplitEquals(t, "semicolon at end should be single statement",
		"select null;",
		[]string{"select null;"})

	assertSplitEquals(t, "comment with no newline should not fail",
		"select null // comment with no newline",
		[]string{"select null // comment with no newline"})

	assertSplitEquals(t, "semicolon followed by comment at end should be single statement",
		"select null; // trailing comment",
		[]string{"select null; // trailing comment"})

	assertSplitEquals(t, "coalesce empty statements into previous but not nonempty statements",
		`select 1; // comment
		;
		select 2;`,
		[]string{"select 1; // comment\n\t\t;", "select 2;"})

}
