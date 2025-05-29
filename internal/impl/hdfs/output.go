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
	"fmt"
	"os"
	"path/filepath"

	"github.com/colinmarc/hdfs"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	oFieldHosts     = "hosts"
	oFieldUser      = "user"
	oFieldDirectory = "directory"
	oFieldPath      = "path"
	oFieldBatching  = "batching"
)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Sends message parts as files to a HDFS directory.`).
		Description(`Each file is written with the path specified with the 'path' field, in order to have a different path for each object you should use function interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here].`+service.OutputPerformanceDocs(true, false)).
		Fields(
			service.NewStringListField(oFieldHosts).
				Description("A list of target host addresses to connect to.").
				Example("localhost:9000"),
			service.NewStringField(oFieldUser).
				Description("A user ID to connect as.").
				Default(""),
			service.NewInterpolatedStringField(oFieldDirectory).
				Description("A directory to store message files within. If the directory does not exist it will be created."),
			service.NewInterpolatedStringField(oFieldPath).
				Description("The path to upload messages as, interpolation functions should be used in order to generate unique file paths.").
				Default(`${!counter()}-${!timestamp_unix_nano()}.txt`),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(oFieldBatching),
		)
}

func init() {
	service.MustRegisterBatchOutput(
		"hdfs", outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, pol service.BatchPolicy, mif int, err error) {
			w := &hdfsWriter{
				log: mgr.Logger(),
			}
			out = w
			if w.hosts, err = conf.FieldStringList(oFieldHosts); err != nil {
				return
			}
			if w.user, err = conf.FieldString(oFieldUser); err != nil {
				return
			}
			if w.directory, err = conf.FieldInterpolatedString(oFieldDirectory); err != nil {
				return
			}
			if w.path, err = conf.FieldInterpolatedString(oFieldPath); err != nil {
				return
			}
			if pol, err = conf.FieldBatchPolicy(oFieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			return
		})
}

type hdfsWriter struct {
	hosts     []string
	user      string
	directory *service.InterpolatedString
	path      *service.InterpolatedString

	client *hdfs.Client
	log    *service.Logger
}

func (h *hdfsWriter) Connect(context.Context) error {
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
	return nil
}

func (h *hdfsWriter) WriteBatch(_ context.Context, batch service.MessageBatch) error {
	if h.client == nil {
		return service.ErrNotConnected
	}

	return batch.WalkWithBatchedErrors(func(i int, m *service.Message) error {
		path, err := batch.TryInterpolatedString(i, h.path)
		if err != nil {
			return fmt.Errorf("path interpolation error: %w", err)
		}
		directory, err := batch.TryInterpolatedString(i, h.directory)
		if err != nil {
			return fmt.Errorf("directory interpolation error: %w", err)
		}
		filePath := filepath.Join(directory, path)

		if err := h.client.MkdirAll(directory, os.ModeDir|0o644); err != nil {
			return err
		}

		fw, err := h.client.Create(filePath)
		if err != nil {
			return err
		}

		mBytes, err := m.AsBytes()
		if err != nil {
			return err
		}

		if _, err := fw.Write(mBytes); err != nil {
			return err
		}
		fw.Close()
		return nil
	})
}

func (*hdfsWriter) Close(context.Context) error {
	return nil
}
