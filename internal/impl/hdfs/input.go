package hdfs

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/colinmarc/hdfs"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		return newHDFSInput(c, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    "hdfs",
		Summary: `Reads files from a HDFS directory, where each discrete file will be consumed as a single message payload.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- hdfs_name
- hdfs_path
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`,
		Categories: []string{
			"Services",
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("hosts", "A list of target host addresses to connect to.").Array(),
			docs.FieldString("user", "A user ID to connect as."),
			docs.FieldString("directory", "The directory to consume from."),
		).ChildDefaultAndTypesFromStruct(input.NewHDFSConfig()),
	})
	if err != nil {
		panic(err)
	}
}

func newHDFSInput(conf input.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	if conf.HDFS.Directory == "" {
		return nil, errors.New("invalid directory (cannot be empty)")
	}
	return input.NewAsyncReader("hdfs", input.NewAsyncPreserver(newHDFSReader(conf.HDFS, log)), mgr)
}

type hdfsReader struct {
	conf input.HDFSConfig

	targets []string

	client *hdfs.Client

	log log.Modular
}

func newHDFSReader(conf input.HDFSConfig, log log.Modular) *hdfsReader {
	return &hdfsReader{
		conf: conf,
		log:  log,
	}
}

func (h *hdfsReader) Connect(ctx context.Context) error {
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
	targets, err := client.ReadDir(h.conf.Directory)
	if err != nil {
		return err
	}

	for _, info := range targets {
		if !info.IsDir() {
			h.targets = append(h.targets, info.Name())
		}
	}

	h.log.Infof("Receiving files from HDFS directory: %v\n", h.conf.Directory)
	return nil
}

func (h *hdfsReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	if len(h.targets) == 0 {
		return nil, nil, component.ErrTypeClosed
	}

	fileName := h.targets[0]
	h.targets = h.targets[1:]

	filePath := filepath.Join(h.conf.Directory, fileName)
	msgBytes, readerr := h.client.ReadFile(filePath)
	if readerr != nil {
		return nil, nil, readerr
	}

	msg := message.QuickBatch([][]byte{msgBytes})
	msg.Get(0).MetaSetMut("hdfs_name", fileName)
	msg.Get(0).MetaSetMut("hdfs_path", filePath)
	return msg, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (h *hdfsReader) Close(ctx context.Context) error {
	return nil
}
