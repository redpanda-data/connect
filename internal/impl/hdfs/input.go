package hdfs

import (
	"context"
	"path/filepath"

	"github.com/colinmarc/hdfs"

	"github.com/benthosdev/benthos/v4/public/service"
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
### Metadata

This input adds the following metadata fields to each message:

`+"``` text"+`
- hdfs_name
- hdfs_path
`+"```"+`

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
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
	err := service.RegisterInput(
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
	if err != nil {
		panic(err)
	}
}

type hdfsReader struct {
	hosts     []string
	user      string
	directory string

	targets []string

	client *hdfs.Client

	log *service.Logger
}

func (h *hdfsReader) Connect(ctx context.Context) error {
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

func (h *hdfsReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
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
	return msg, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (h *hdfsReader) Close(ctx context.Context) error {
	return nil
}
