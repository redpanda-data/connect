// Copyright 2025 Redpanda Data, Inc.
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

package migrator

// nameConverter provides optimized bidirectional topic name translation.
// It only stores mappings when source and destination names differ,
// using passthrough for identical names to minimize memory usage.
type nameConverter struct {
	srcToDst map[string]string
	dstToSrc map[string]string
}

func nameConverterFromTopicMappings(mappings []TopicMapping) nameConverter {
	var nc nameConverter

	for _, m := range mappings {
		if m.Src.Topic != m.Dst.Topic {
			if nc.srcToDst == nil {
				nc.srcToDst = make(map[string]string)
				nc.dstToSrc = make(map[string]string)
			}
			nc.srcToDst[m.Src.Topic] = m.Dst.Topic
			nc.dstToSrc[m.Dst.Topic] = m.Src.Topic
		}
	}

	return nc
}

// ToDst converts source name to destination name.
func (nc nameConverter) ToDst(src string) string {
	if nc.srcToDst == nil {
		return src
	}
	if dst, ok := nc.srcToDst[src]; ok {
		return dst
	}
	return src
}

// ToSrc converts destination name to source name.
func (nc nameConverter) ToSrc(dst string) string {
	if nc.dstToSrc == nil {
		return dst
	}
	if src, ok := nc.dstToSrc[dst]; ok {
		return src
	}
	return dst
}
