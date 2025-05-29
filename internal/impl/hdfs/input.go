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

package hdfs

import (
	"context"
	"path/filepath"

	"github.com/colinmarc/hdfs"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	iFieldHosts     = "hosts"
	iFieldUser      = "user"
	iFieldDirectory = "directory"
)

func inputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Reads files from a HDFS directory, where each discrete file will be consumed as a single message payload.`).
		Description(`
== Metadata

This input adds the following metadata fields to each message:

- hdfs_name
- hdfs_path

You can access these metadata fields using
xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].`).
		Fields(
			service.NewStringListField(iFieldHosts).
				Description("A list of target host addresses to connect to.").
				Example("localhost:9000"),
			service.NewStringField(iFieldUser).
				Description("A user ID to connect as.").
				Default(""),
			service.NewStringField(iFieldDirectory).
				Description("The directory to consume from."),
		)
}

func init() {
	service.MustRegisterInput(
		"hdfs", inputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Input, err error) {
			rdr := &hdfsReader{
				log: mgr.Logger(),
			}
			out = rdr
			if rdr.hosts, err = conf.FieldStringList(iFieldHosts); err != nil {
				return
			}
			if rdr.user, err = conf.FieldString(iFieldUser); err != nil {
				return
			}
			if rdr.directory, err = conf.FieldString(iFieldDirectory); err != nil {
				return
			}
			return
		})
}

type hdfsReader struct {
	hosts     []string
	user      string
	directory string

	targets []string

	client *hdfs.Client

	log *service.Logger
}

func (h *hdfsReader) Connect(context.Context) error {
	if h.client != nil {
		return nil
	}

	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: h.hosts,
		User:      h.user,
	})
	if err != nil {
		return err
	}

	h.client = client
	targets, err := client.ReadDir(h.directory)
	if err != nil {
		return err
	}

	for _, info := range targets {
		if !info.IsDir() {
			h.targets = append(h.targets, info.Name())
		}
	}
	return nil
}

func (h *hdfsReader) Read(context.Context) (*service.Message, service.AckFunc, error) {
	if len(h.targets) == 0 {
		return nil, nil, service.ErrEndOfInput
	}

	fileName := h.targets[0]
	h.targets = h.targets[1:]

	filePath := filepath.Join(h.directory, fileName)
	msgBytes, readerr := h.client.ReadFile(filePath)
	if readerr != nil {
		return nil, nil, readerr
	}

	msg := service.NewMessage(msgBytes)
	msg.MetaSetMut("hdfs_name", fileName)
	msg.MetaSetMut("hdfs_path", filePath)
	return msg, func(context.Context, error) error {
		return nil
	}, nil
}

func (*hdfsReader) Close(context.Context) error {
	return nil
}
