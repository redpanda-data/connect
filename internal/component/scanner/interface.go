package scanner

import (
	"context"
	"io"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// AckFn is a function provided to a scanner that it should call once the
// derived io.ReadCloser is fully consumed.
type AckFn func(context.Context, error) error

// Scanner is an interface implemented by all scanner implementations once a
// creator has instantiated it on a byte stream.
type Scanner interface {
	Next(context.Context) (message.Batch, AckFn, error)
	Close(context.Context) error
}

// SourceDetails contains exclusively optional information which could be used
// by codec implementations in order to determine the underlying data format.
type SourceDetails struct {
	Name string
}

// Creator is an interface implemented by all scanners, which allows components
// to construct a scanner from an unbounded io.ReadCloser.
type Creator interface {
	Create(rdr io.ReadCloser, aFn AckFn, details SourceDetails) (Scanner, error)
	Close(context.Context) error
}
