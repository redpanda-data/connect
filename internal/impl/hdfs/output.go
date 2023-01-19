package hdfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/colinmarc/hdfs"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newHDFSOutput), docs.ComponentSpec{
		Name:        "hdfs",
		Summary:     `Sends message parts as files to a HDFS directory.`,
		Description: output.Description(true, false, `Each file is written with the path specified with the 'path' field, in order to have a different path for each object you should use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("hosts", "A list of hosts to connect to.", "localhost:9000").Array(),
			docs.FieldString("user", "A user identifier."),
			docs.FieldString("directory", "A directory to store message files within. If the directory does not exist it will be created."),
			docs.FieldString(
				"path", "The path to upload messages as, interpolation functions should be used in order to generate unique file paths.",
				`${!count("files")}-${!timestamp_unix_nano()}.txt`,
			).IsInterpolated(),
			docs.FieldInt("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			policy.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewHDFSConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newHDFSOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	h, err := newHDFSWriter(conf.HDFS, mgr)
	if err != nil {
		return nil, err
	}
	w, err := output.NewAsyncWriter("hdfs", conf.HDFS.MaxInFlight, h, mgr)
	if err != nil {
		return nil, err
	}
	return batcher.NewFromConfig(conf.HDFS.Batching, output.OnlySinglePayloads(w), mgr)
}

type hdfsWriter struct {
	conf      output.HDFSConfig
	directory *field.Expression
	path      *field.Expression

	client *hdfs.Client
	log    log.Modular
}

func newHDFSWriter(conf output.HDFSConfig, mgr bundle.NewManagement) (*hdfsWriter, error) {
	path, err := mgr.BloblEnvironment().NewField(conf.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	directory, err := mgr.BloblEnvironment().NewField(conf.Directory)
	if err != nil {
		return nil, fmt.Errorf("failed to parse directory expression: %v", err)
	}
	return &hdfsWriter{
		conf:      conf,
		directory: directory,
		path:      path,
		log:       mgr.Logger(),
	}, nil
}

func (h *hdfsWriter) Connect(ctx context.Context) error {
	if h.client != nil {
		return nil
	}

	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: h.conf.Hosts,
		User:      h.conf.User,
	})
	if err != nil {
		return err
	}

	h.client = client

	h.log.Infof("Writing message parts as files to HDFS directory: %v\n", h.conf.Directory)
	return nil
}

func (h *hdfsWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	if h.client == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		path, err := h.path.String(i, msg)
		if err != nil {
			return fmt.Errorf("path interpolation error: %w", err)
		}
		directory, err := h.directory.String(i, msg)
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

		if _, err := fw.Write(p.AsBytes()); err != nil {
			return err
		}
		fw.Close()
		return nil
	})
}

func (h *hdfsWriter) Close(context.Context) error {
	return nil
}
