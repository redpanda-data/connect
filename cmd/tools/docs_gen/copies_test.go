// Copyright 2026 Redpanda Data, Inc.
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

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestFieldDescriptionsAreNotCopied fails when the same field's description is
// written out as a string literal in more than one place, whether the copies
// are identical or have drifted apart. The published reference docs are
// generated from these strings, so every copy must be one shared constant or
// helper: a fix to one copy then reaches every component that uses it.
//
// It compares description string literals of at least 40 characters, from
// `.Description(...)` calls and from benthos's `docs.FieldString("name",
// "description")` style constructors, on fields with the same name. It scans
// this repo and the `public` and `internal` packages of benthos, and reports
// pairs whose words are at least 90% the same. Pairs where both copies are in
// benthos are left to benthos. A copy of benthos text is shared by using what
// benthos exports from `public/service`, because this repo cannot import
// benthos's internal packages.
func TestFieldDescriptionsAreNotCopied(t *testing.T) {
	roots := []string{"../../../internal"}
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/redpanda-data/benthos/v4").Output()
	if err != nil {
		t.Fatalf("locating the benthos module: %v", err)
	}
	benthos := strings.TrimSpace(string(out))
	roots = append(roots, filepath.Join(benthos, "public"), filepath.Join(benthos, "internal"))

	var lits []descLiteral
	for _, r := range roots {
		lits = append(lits, collectDescriptions(t, r)...)
	}
	var problems []string
	for i := range lits {
		for j := i + 1; j < len(lits); j++ {
			a, b := lits[i], lits[j]
			if a.name != b.name || a.name == "" {
				continue
			}
			if strings.HasPrefix(a.pos, benthos) && strings.HasPrefix(b.pos, benthos) {
				continue // benthos copies are benthos's to fix
			}
			if la, lb := len(a.words), len(b.words); la > 2*lb || lb > 2*la {
				continue
			}
			if r := wordSimilarity(a.words, b.words); r >= 0.9 {
				problems = append(problems, fmt.Sprintf("%.2f %s: %s and %s", r, a.name, rel(a.pos, benthos), rel(b.pos, benthos)))
			}
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Errorf("copied description, share it through one constant or helper: %s", p)
	}
}

type descLiteral struct {
	name, pos string
	words     []string
}

func rel(pos, benthos string) string {
	if rest, ok := strings.CutPrefix(pos, benthos); ok {
		return "benthos" + rest
	}
	return strings.TrimPrefix(pos, "../../../")
}

func collectDescriptions(t *testing.T, root string) []descLiteral {
	t.Helper()
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	consts := map[string]string{}
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return err
		}
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			return nil
		}
		files[p] = f
		for _, d := range f.Decls {
			if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.CONST {
				for _, sp := range g.Specs {
					vs := sp.(*ast.ValueSpec)
					for i, n := range vs.Names {
						if i < len(vs.Values) {
							if v, ok := stringValue(vs.Values[i]); ok {
								consts[filepath.Dir(p)+"|"+n.Name] = v
							}
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var lits []descLiteral
	for p, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			dir := filepath.Dir(p)
			var name string
			var desc ast.Expr
			switch fn := calleeName(call); {
			case fn == "Description" && len(call.Args) == 1:
				// service.NewStringField("x").Description("...")
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					name, desc = constructorName(sel.X, dir, consts), call.Args[0]
				}
			case strings.HasPrefix(fn, "Field") && len(call.Args) >= 2:
				// docs.FieldString("x", "...") in benthos's internal docs package
				name, desc = fieldNameArg(call.Args[0], dir, consts), call.Args[1]
			}
			if desc == nil {
				return true
			}
			s, ok := stringValue(desc)
			if !ok || len(strings.TrimSpace(s)) < 40 {
				return true
			}
			lits = append(lits, descLiteral{
				name:  name,
				pos:   fmt.Sprintf("%s:%d", p, fset.Position(desc.Pos()).Line),
				words: strings.Fields(s),
			})
			return true
		})
	}
	return lits
}

// stringValue evaluates a string literal or a + concatenation of literals.
func stringValue(e ast.Expr) (string, bool) {
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(v.Value)
		return s, err == nil
	case *ast.BinaryExpr:
		if v.Op != token.ADD {
			return "", false
		}
		a, ok1 := stringValue(v.X)
		b, ok2 := stringValue(v.Y)
		return a + b, ok1 && ok2
	case *ast.ParenExpr:
		return stringValue(v.X)
	}
	return "", false
}

// calleeName returns the name of the called function or method.
func calleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		return fn.Sel.Name
	case *ast.Ident:
		return fn.Name
	}
	return ""
}

// fieldNameArg evaluates a field name argument: a string literal or a constant
// declared in the same package.
func fieldNameArg(e ast.Expr, dir string, consts map[string]string) string {
	switch a := e.(type) {
	case *ast.BasicLit:
		v, _ := strconv.Unquote(a.Value)
		return v
	case *ast.Ident:
		return consts[dir+"|"+a.Name]
	}
	return ""
}

// constructorName follows a chain such as NewStringField("x").Default(...)
// back to the constructor and returns the field name it was given. The
// constructor may be qualified (service.NewStringField) or not, as inside the
// benthos service package.
func constructorName(e ast.Expr, dir string, consts map[string]string) string {
	for {
		call, ok := e.(*ast.CallExpr)
		if !ok {
			return ""
		}
		fn := calleeName(call)
		if strings.HasPrefix(fn, "New") && strings.HasSuffix(fn, "Field") && len(call.Args) > 0 {
			return fieldNameArg(call.Args[0], dir, consts)
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return ""
		}
		e = sel.X
	}
}

// wordSimilarity is 2*LCS/(len(a)+len(b)) over words.
func wordSimilarity(a, b []string) float64 {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			switch {
			case a[i-1] == b[j-1]:
				cur[j] = prev[j-1] + 1
			case prev[j] > cur[j-1]:
				cur[j] = prev[j]
			default:
				cur[j] = cur[j-1]
			}
		}
		prev, cur = cur, prev
	}
	return 2 * float64(prev[len(b)]) / float64(len(a)+len(b))
}
