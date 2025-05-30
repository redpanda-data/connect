// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpcplugin

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/redpanda-data/connect/v4/internal/template"
)

// PluginLanguage represents the programming language of the plugin.
type PluginLanguage string

const (
	// PluginLanguageGo is the language for Go plugins.
	PluginLanguageGo PluginLanguage = "golang"
	// PluginLanguagePython is the language for Python plugins.
	PluginLanguagePython PluginLanguage = "python"
)

var allPluginLanguages = []PluginLanguage{PluginLanguageGo, PluginLanguagePython}

//go:embed golangtemplate/input
var golangInputEmbeddedTemplate embed.FS

//go:embed golangtemplate/output
var golangOutputEmbeddedTemplate embed.FS

//go:embed golangtemplate/processor
var golangProcessorEmbeddedTemplate embed.FS

//go:embed pythontemplate/input
var pythonInputEmbeddedTemplate embed.FS

//go:embed pythontemplate/output
var pythonOutputEmbeddedTemplate embed.FS

//go:embed pythontemplate/processor
var pythonProcessorEmbeddedTemplate embed.FS

// InitializeProject initializes a new plugin project in the specified directory.
func InitializeProject(lang PluginLanguage, compType ComponentType, directory string) error {
	abs, err := filepath.Abs(directory)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for directory %s: %w", directory, err)
	}
	projectName := filepath.Base(abs)
	if err := compType.Validate(); err != nil {
		return err
	}
	var fs fs.ReadFileFS
	switch lang {
	case PluginLanguageGo:
		switch compType {
		case ComponentTypeInput:
			fs = golangInputEmbeddedTemplate
		case ComponentTypeOutput:
			fs = golangOutputEmbeddedTemplate
		case ComponentTypeProcessor:
			fs = golangProcessorEmbeddedTemplate
		}
	case PluginLanguagePython:
		switch compType {
		case ComponentTypeInput:
			fs = pythonInputEmbeddedTemplate
		case ComponentTypeOutput:
			fs = pythonOutputEmbeddedTemplate
		case ComponentTypeProcessor:
			fs = pythonProcessorEmbeddedTemplate
		}
	}
	if fs == nil {
		return fmt.Errorf("unexpected plugin language, valid options %v, got: %s", allPluginLanguages, lang)
	}
	err = template.CreateTemplate(
		fs,
		directory,
		template.WithStrippedPrefix(fmt.Sprintf("%stemplate/%s", lang, compType)),
		template.WithRenames(map[string]string{
			"go.mod.tmpl": "go.mod",
		}),
		template.WithVariables(map[string]string{
			"PROJECT_NAME_HERE": projectName,
			"GO_VERSION":        strings.TrimPrefix(runtime.Version(), "go"),
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create template for %s: %w", lang, err)
	}
	fmt.Printf("plugin `%s` created at `%s`\n", projectName, abs)
	switch lang {
	case PluginLanguageGo:
		if _, err := exec.LookPath("go"); errors.Is(err, exec.ErrNotFound) {
			fmt.Println("go not found in $PATH, please install go to build golang plugins: https://go.dev/doc/install")
		}
		fmt.Println("to add module requirements and sums:")
		fmt.Println("\tgo mod tidy")
		fmt.Println("before running the plugin, first build it using `go build .` in the plugin directory")
	case PluginLanguagePython:
		if _, err := exec.LookPath("uv"); errors.Is(err, exec.ErrNotFound) {
			fmt.Println("uv not found in $PATH, please install uv to run python plugins: https://docs.astral.sh/uv/getting-started/installation/")
		}
	}
	fmt.Println("run the plugin using `redpanda-connect run --rpcplugin=./plugin.yaml connect.yaml` in the plugin directory")
	return nil
}
