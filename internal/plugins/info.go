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

package plugins

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "embed"
)

// TypeName is an explicit name for a component plugin type.
type TypeName string

// Explicit names for each plugin component type.
const (
	TypeNone      TypeName = ""
	TypeBuffer    TypeName = "buffer"
	TypeCache     TypeName = "cache"
	TypeInput     TypeName = "input"
	TypeMetric    TypeName = "metric"
	TypeOutput    TypeName = "output"
	TypeProcessor TypeName = "processor"
	TypeRateLimit TypeName = "rate_limit"
	TypeScanner   TypeName = "scanner"
	TypeTracer    TypeName = "tracer"
	TypeSQLDriver TypeName = "sql_driver"
)

// IsCore returns true if the type name is for a core benthos plugin type.
func (t TypeName) IsCore() bool {
	_, isCore := map[TypeName]struct{}{
		TypeBuffer:    {},
		TypeCache:     {},
		TypeInput:     {},
		TypeMetric:    {},
		TypeOutput:    {},
		TypeProcessor: {},
		TypeRateLimit: {},
		TypeScanner:   {},
		TypeTracer:    {},
	}[t]
	return isCore
}

//go:embed info.csv
var baseInfoCSV []byte

// PluginInfo describes a given component
type PluginInfo struct {
	Name           string
	Type           TypeName
	CommercialName string
	Support        string
	Deprecated     bool
	Cloud          bool
	CloudAI        bool
}

func basePluginInfo(name string, typeStr TypeName, view *service.ConfigView) PluginInfo {
	return PluginInfo{
		Name:           name,
		Type:           typeStr,
		CommercialName: name,
		Deprecated:     view.IsDeprecated(),
		Support:        "community",
	}
}

func (c PluginInfo) key() string {
	return fmt.Sprintf("%v-%v", c.Name, c.Type)
}

func pluginInfoFromMap(m map[string]string) PluginInfo {
	supportStr := m["support"]
	if supportStr == "" {
		supportStr = "community"
	}
	return PluginInfo{
		Name:           m["name"],
		Type:           TypeName(m["type"]),
		CommercialName: m["commercial_name"],
		Support:        supportStr,
		Deprecated:     m["deprecated"] == "y",
		Cloud:          m["cloud"] == "y",
		CloudAI:        m["cloud_ai"] == "y",
	}
}

type columnInfo struct {
	name     string
	minWidth int
}

func pluginInfoMapColumns() []columnInfo {
	return []columnInfo{{"name", 24}, {"type", 10}, {"commercial_name", 24}, {"support", 11}, {"deprecated", 11}, {"cloud", 6}, {"cloud_ai", 0}}
}

func (c PluginInfo) toMap() map[string]string {
	return map[string]string{
		"name":            c.Name,
		"type":            string(c.Type),
		"commercial_name": c.CommercialName,
		"support":         c.Support,
		"deprecated":      formatBool(c.Deprecated),
		"cloud":           formatBool(c.Cloud),
		"cloud_ai":        formatBool(c.CloudAI),
	}
}

func formatBool(b bool) string {
	if b {
		return "y"
	}
	return "n"
}

// InfoCollection is a map of plugin information indexed by the name and type.
type InfoCollection map[string]PluginInfo

func (i InfoCollection) addIfMissing(info PluginInfo) {
	if existingInfo, exists := i[info.key()]; !exists {
		i[info.key()] = info
	} else {
		if existingInfo.Deprecated != info.Deprecated {
			existingInfo.Deprecated = info.Deprecated
			i[info.key()] = existingInfo
		}
	}
}

// BaseInfo represents the information defined within info.csv.
var BaseInfo = InfoCollection{}

func init() {
	cReader := csv.NewReader(bytes.NewReader(baseInfoCSV))
	componentRecords, err := cReader.ReadAll()
	if err != nil {
		panic(err)
	}

	columnNames := componentRecords[0]
	for i, v := range columnNames {
		columnNames[i] = strings.TrimSpace(v)
	}

	for _, c := range componentRecords[1:] {
		cMap := map[string]string{}
		for i, v := range c {
			cMap[columnNames[i]] = strings.TrimSpace(v)
		}
		info := pluginInfoFromMap(cMap)
		BaseInfo[info.key()] = info
	}
}

// PluginNamesForCloudAI returns a list of component plugin names supported in
// the cloud AI product.
func PluginNamesForCloudAI(typeStr TypeName) []string {
	var names []string
	seen := map[string]struct{}{}
	for _, info := range BaseInfo {
		if !info.CloudAI {
			continue
		}
		if typeStr != TypeNone {
			if info.Type != typeStr {
				continue
			}
		} else if !info.Type.IsCore() {
			continue
		}
		if _, exists := seen[info.Name]; !exists {
			names = append(names, info.Name)
			seen[info.Name] = struct{}{}
		}
	}
	return names
}

// PluginNamesForCloud returns a list of component plugin names supported in the
// cloud product.
func PluginNamesForCloud(typeStr TypeName) []string {
	var names []string
	seen := map[string]struct{}{}
	for _, info := range BaseInfo {
		if !info.Cloud {
			continue
		}
		if typeStr != TypeNone {
			if info.Type != typeStr {
				continue
			}
		} else if !info.Type.IsCore() {
			continue
		}
		if _, exists := seen[info.Name]; !exists {
			names = append(names, info.Name)
			seen[info.Name] = struct{}{}
		}
	}
	return names
}

// Hydrate uses a reference environment in order to hydrate plugins that
// are currently unrepresented in the collection.
func (i InfoCollection) Hydrate(env *service.Environment) {
	env.WalkBuffers(func(name string, config *service.ConfigView) {
		i.addIfMissing(basePluginInfo(name, TypeBuffer, config))
	})

	env.WalkCaches(func(name string, config *service.ConfigView) {
		i.addIfMissing(basePluginInfo(name, TypeCache, config))
	})

	env.WalkInputs(func(name string, config *service.ConfigView) {
		i.addIfMissing(basePluginInfo(name, TypeInput, config))
	})

	env.WalkMetrics(func(name string, config *service.ConfigView) {
		i.addIfMissing(basePluginInfo(name, TypeMetric, config))
	})

	env.WalkOutputs(func(name string, config *service.ConfigView) {
		i.addIfMissing(basePluginInfo(name, TypeOutput, config))
	})

	env.WalkProcessors(func(name string, config *service.ConfigView) {
		i.addIfMissing(basePluginInfo(name, TypeProcessor, config))
	})

	env.WalkRateLimits(func(name string, config *service.ConfigView) {
		i.addIfMissing(basePluginInfo(name, TypeRateLimit, config))
	})

	env.WalkScanners(func(name string, config *service.ConfigView) {
		i.addIfMissing(basePluginInfo(name, TypeScanner, config))
	})

	env.WalkTracers(func(name string, config *service.ConfigView) {
		i.addIfMissing(basePluginInfo(name, TypeTracer, config))
	})
}

func padString(v string, size int) string {
	if len(v) >= size {
		return v
	}
	return v + strings.Repeat(" ", size-len(v))
}

// FormatCSV attempts to format the defined suite of components as CSV.
func (i InfoCollection) FormatCSV() ([]byte, error) {
	var baseKeys []string
	for k := range i {
		baseKeys = append(baseKeys, k)
	}
	sort.Strings(baseKeys)

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	headersInfo := pluginInfoMapColumns()

	headerKeysResized := make([]string, len(headersInfo))
	for i, v := range headersInfo {
		headerKeysResized[i] = padString(v.name, v.minWidth)
	}
	if err := w.Write(headerKeysResized); err != nil {
		return nil, err
	}

	for _, componentKey := range baseKeys {
		componentMap := i[componentKey].toMap()

		componentRow := make([]string, len(headersInfo))
		for i, column := range headersInfo {
			componentRow[i] = padString(componentMap[column.name], column.minWidth)
		}

		if err := w.Write(componentRow); err != nil {
			return nil, err
		}
	}

	w.Flush()
	return buf.Bytes(), nil
}
