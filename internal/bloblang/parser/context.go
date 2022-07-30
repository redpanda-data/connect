package parser

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

// Context contains context used throughout a Bloblang parser for
// accessing function and method constructors.
type Context struct {
	Functions    *query.FunctionSet
	Methods      *query.MethodSet
	namedContext *namedContext
	importer     Importer
}

// EmptyContext returns a parser context with no functions, methods or import
// capabilities.
func EmptyContext() Context {
	return Context{
		Functions: query.NewFunctionSet(),
		Methods:   query.NewMethodSet(),
		importer:  newOSImporter(),
	}
}

// GlobalContext returns a parser context with globally defined functions and
// methods.
func GlobalContext() Context {
	return Context{
		Functions: query.AllFunctions,
		Methods:   query.AllMethods,
		importer:  newOSImporter(),
	}
}

type namedContext struct {
	name string
	next *namedContext
}

// WithNamedContext returns a Context with a named execution context.
func (pCtx Context) WithNamedContext(name string) Context {
	next := pCtx.namedContext
	pCtx.namedContext = &namedContext{name: name, next: next}
	return pCtx
}

// HasNamedContext returns true if a given name exists as a named context.
func (pCtx Context) HasNamedContext(name string) bool {
	tmp := pCtx.namedContext
	for tmp != nil {
		if tmp.name == name {
			return true
		}
		tmp = tmp.next
	}
	return false
}

// InitFunction attempts to initialise a function from the available
// constructors of the parser context.
func (pCtx Context) InitFunction(name string, args *query.ParsedParams) (query.Function, error) {
	return pCtx.Functions.Init(name, args)
}

// InitMethod attempts to initialise a method from the available constructors of
// the parser context.
func (pCtx Context) InitMethod(name string, target query.Function, args *query.ParsedParams) (query.Function, error) {
	return pCtx.Methods.Init(name, target, args)
}

// WithImporter returns a Context where imports are made from the provided
// Importer implementation.
func (pCtx Context) WithImporter(importer Importer) Context {
	pCtx.importer = importer
	return pCtx
}

// WithImporterRelativeToFile returns a Context where any relative imports will
// be made from the directory of the provided file path. The provided path can
// itself be relative (to the current importer directory) or absolute.
func (pCtx Context) WithImporterRelativeToFile(pathStr string) Context {
	pCtx.importer = pCtx.importer.RelativeToFile(pathStr)
	return pCtx
}

// Deactivated returns a version of the parser context where all functions and
// methods exist but can no longer be instantiated. This means it's possible to
// parse and validate mappings but not execute them. If the context also has an
// importer then it will also be replaced with an implementation that always
// returns empty files.
func (pCtx Context) Deactivated() Context {
	nextCtx := pCtx
	nextCtx.Functions = pCtx.Functions.Deactivated()
	nextCtx.Methods = pCtx.Methods.Deactivated()
	return nextCtx
}

// CustomImporter returns a version of the parser context where file imports are
// done exclusively through a provided closure function, which takes an import
// path (relative or absolute).
func (pCtx Context) CustomImporter(fn func(name string) ([]byte, error)) Context {
	nextCtx := pCtx
	nextCtx.importer = newCustomImporter(fn)
	return nextCtx
}

// DisabledImports returns a version of the parser context where file imports
// are entirely disabled. Any import statement within parsed mappings will
// return parse errors explaining that file imports are disabled.
func (pCtx Context) DisabledImports() Context {
	nextCtx := pCtx
	nextCtx.importer = disabledImporter{}
	return nextCtx
}

//------------------------------------------------------------------------------

// Importer represents a repository of bloblang files that can be imported by
// mappings. It's possible for mappings to import files using relative paths, if
// the import is from a mapping which was itself imported then the path should
// be interpretted as relative to that file.
type Importer interface {
	// Import a file from a relative or absolute path.
	Import(pathStr string) ([]byte, error)

	// Derive a new importer where relative import paths are resolved from the
	// directory of the provided file path. The provided path could be absolute,
	// or relative itself in which case it should be resolved from the
	// pre-existing relative directory.
	RelativeToFile(filePath string) Importer
}

//------------------------------------------------------------------------------

type osImporter struct {
	relativePath string
}

func newOSImporter() Importer {
	pwd, _ := os.Getwd()
	return &osImporter{
		relativePath: pwd,
	}
}

func (i *osImporter) Import(pathStr string) ([]byte, error) {
	if !filepath.IsAbs(pathStr) {
		pathStr = filepath.Join(i.relativePath, pathStr)
	}

	f, err := os.Open(pathStr)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(f)
}

func (i *osImporter) RelativeToFile(filePath string) Importer {
	dir := filepath.Dir(filePath)
	if dir == "" || dir == "." {
		return i
	}

	pathStr := filepath.Dir(filePath)
	if !filepath.IsAbs(pathStr) && i.relativePath != "" {
		pathStr = filepath.Join(i.relativePath, pathStr)
	}

	newI := *i
	newI.relativePath = pathStr
	return &newI
}

//------------------------------------------------------------------------------

type customImporter struct {
	relativePath string
	readFn       func(name string) ([]byte, error)
}

func newCustomImporter(readFn func(name string) ([]byte, error)) Importer {
	return &customImporter{
		relativePath: ".",
		readFn:       readFn,
	}
}

func (i *customImporter) Import(pathStr string) ([]byte, error) {
	if !filepath.IsAbs(pathStr) {
		pathStr = filepath.Join(i.relativePath, pathStr)
	}

	return i.readFn(pathStr)
}

func (i *customImporter) RelativeToFile(filePath string) Importer {
	dir := filepath.Dir(filePath)
	if dir == "" || dir == "." {
		return i
	}

	pathStr := filepath.Dir(filePath)
	if !filepath.IsAbs(pathStr) && i.relativePath != "" {
		pathStr = filepath.Join(i.relativePath, pathStr)
	}

	newI := *i
	newI.relativePath = pathStr
	return &newI
}

//------------------------------------------------------------------------------

type disabledImporter struct{}

func (d disabledImporter) Import(pathStr string) ([]byte, error) {
	return nil, errors.New("imports are disabled in this context")
}

func (d disabledImporter) RelativeToFile(filePath string) Importer {
	return d
}
