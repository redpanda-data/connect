package hdfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/colinmarc/hdfs"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/public/service"
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
		Description(output.Description(true, false, `Each file is written with the path specified with the 'path' field, in order to have a different path for each object you should use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).`)).
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
				Default(`${!count("files")}-${!timestamp_unix_nano()}.txt`),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(oFieldBatching),
		)
}

func init() {
	err := service.RegisterBatchOutput(
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
	if err != nil {
		panic(err)
	}
}

type hdfsWriter struct {
	hosts     []string
	user      string
	directory *service.InterpolatedString
	path      *service.InterpolatedString

	client *hdfs.Client
	log    *service.Logger
}

func (h *hdfsWriter) Connect(ctx context.Context) error {
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

func (h *hdfsWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
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

func (h *hdfsWriter) Close(context.Context) error {
	return nil
}
