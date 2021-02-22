package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/gcp/storage/client"
	"google.golang.org/api/googleapi"
)

//------------------------------------------------------------------------------

// GCPCloudStorageConfig contains configuration fields for the GCP Cloud Storage
// output type.
type GCPCloudStorageConfig struct {
	Bucket          string             `json:"bucket" yaml:"bucket"`
	Path            string             `json:"path" yaml:"path"`
	ContentType     string             `json:"content_type" yaml:"content_type"`
	ContentEncoding string             `json:"content_encoding" yaml:"content_encoding"`
	ChunkSize       int                `json:"chunk_size" yaml:"chunk_size"`
	Timeout         string             `json:"timeout" yaml:"timeout"`
	MaxInFlight     int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching        batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewGCPCloudStorageConfig creates a new Config with default values.
func NewGCPCloudStorageConfig() GCPCloudStorageConfig {
	return GCPCloudStorageConfig{
		Bucket:          "",
		Path:            `${!count("files")}-${!timestamp_unix_nano()}.txt`,
		ContentType:     "application/octet-stream",
		ContentEncoding: "",
		ChunkSize:       googleapi.DefaultUploadChunkSize,
		Timeout:         "5s",
		MaxInFlight:     1,
		Batching:        batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

// GCPCloudStorage is a benthos writer.Type implementation that writes messages
// to a GCP Cloud Storage bucket.
type GCPCloudStorage struct {
	conf GCPCloudStorageConfig

	path            field.Expression
	contentType     field.Expression
	contentEncoding field.Expression

	client  *storage.Client
	connMut sync.RWMutex
	timeout time.Duration

	log   log.Modular
	stats metrics.Type
}

// NewGCPCloudStorage creates a new GCP Cloud Storage bucket writer.Type.
func NewGCPCloudStorage(
	conf GCPCloudStorageConfig,
	log log.Modular,
	stats metrics.Type,
) (*GCPCloudStorage, error) {
	var timeout time.Duration
	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout period string: %v", err)
		}
	}
	g := &GCPCloudStorage{
		conf:    conf,
		log:     log,
		stats:   stats,
		timeout: timeout,
	}
	var err error
	if g.path, err = bloblang.NewField(conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	if g.contentType, err = bloblang.NewField(conf.ContentType); err != nil {
		return nil, fmt.Errorf("failed to parse content type expression: %v", err)
	}
	if g.contentEncoding, err = bloblang.NewField(conf.ContentEncoding); err != nil {
		return nil, fmt.Errorf("failed to parse content encoding expression: %v", err)
	}

	return g, nil
}

// ConnectWithContext attempts to establish a connection to the target Google
// Cloud Storage bucket.
func (g *GCPCloudStorage) ConnectWithContext(ctx context.Context) error {
	g.connMut.Lock()
	defer g.connMut.Unlock()

	var err error
	g.client, err = client.New(ctx)
	if err != nil {
		return err
	}

	g.log.Infof("Uploading message parts as objects to GCP Cloud Storage bucket: %v\n", g.conf.Bucket)
	return nil
}

// Connect attempts to establish a connection to the target GCP Cloud Storage
// bucket.
func (g *GCPCloudStorage) Connect() error {
	if g.client != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return g.ConnectWithContext(ctx)
}

// Write attempts to write message contents to a target GCP Cloud Storage bucket
// as files.
func (g *GCPCloudStorage) Write(msg types.Message) error {
	return g.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target GCP Cloud
// Storage bucket as files.
func (g *GCPCloudStorage) WriteWithContext(ctx context.Context, msg types.Message) error {
	g.connMut.RLock()
	client := g.client
	g.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		metadata := map[string]string{}
		p.Metadata().Iter(func(k, v string) error {
			metadata[k] = v
			return nil
		})

		w := client.Bucket(g.conf.Bucket).Object(g.path.String(i, msg)).NewWriter(ctx)
		w.ChunkSize = g.conf.ChunkSize
		w.ContentType = g.contentType.String(i, msg)
		w.ContentEncoding = g.contentEncoding.String(i, msg)
		w.Metadata = metadata
		_, err := w.Write(p.Get())
		if err != nil {
			return err
		}

		return w.Close()
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (g *GCPCloudStorage) CloseAsync() {
	go func() {
		g.connMut.Lock()
		if g.client != nil {
			g.client.Close()
			g.client = nil
		}
		g.connMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (g *GCPCloudStorage) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
