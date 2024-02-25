package service

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// ScannerSourceDetails contains exclusively optional information which could be
// used by scanner implementations in order to determine the underlying data
// format.
type ScannerSourceDetails struct {
	details scanner.SourceDetails
}

// NewScannerSourceDetails creates a ScannerSourceDetails object with default
// values.
func NewScannerSourceDetails() *ScannerSourceDetails {
	return &ScannerSourceDetails{
		details: scanner.SourceDetails{},
	}
}

// SetName sets a filename (or other equivalent name of the source) to details.
func (r *ScannerSourceDetails) SetName(name string) {
	r.details.Name = name
}

// Name returns a filename (or other equivalent name of the source), or an
// empty string if it has not been set.
func (r *ScannerSourceDetails) Name() string {
	return r.details.Name
}

// BatchScannerCreator is an interface implemented by Benthos scanner plugins.
// Calls to Create must create a new instantiation of BatchScanner that consumes
// the provided io.ReadCloser, produces batches of messages (batches containing
// a single message are valid) and calls the provided AckFunc once all derived
// data is delivered (or rejected).
type BatchScannerCreator interface {
	Create(io.ReadCloser, AckFunc, *ScannerSourceDetails) (BatchScanner, error)
	Close(context.Context) error
}

// BatchScanner is an interface implemented by instantiations of
// BatchScannerCreator responsible for consuming an io.ReadCloser and converting
// the stream of bytes into discrete message batches based on the underlying
// format of the scanner.
//
// The returned ack func will be called by downstream components once the
// produced message batch has been successfully processed and delivered. Only
// once all message batches extracted from a BatchScanner should the ack func
// provided at instantiation be called, unless an ack call is returned with an
// error.
//
// Once the input data has been fully consumed io.EOF should be returned.
type BatchScanner interface {
	NextBatch(context.Context) (MessageBatch, AckFunc, error)
	Close(context.Context) error
}

//------------------------------------------------------------------------------

// SimpleBatchScanner is a reduced version of BatchScanner where managing the
// aggregation of acknowledgments from yielded message batches is omitted.
type SimpleBatchScanner interface {
	NextBatch(context.Context) (MessageBatch, error)
	Close(context.Context) error
}

// AutoAggregateBatchScannerAcks wraps a simplified SimpleBatchScanner in a
// mechanism that automatically aggregates acknowledgments from yielded batches.
func AutoAggregateBatchScannerAcks(strm SimpleBatchScanner, aFn AckFunc) BatchScanner {
	return &managedAckBatchScanner{
		strm:      strm,
		sourceAck: ackOnce(aFn),
	}
}

func ackOnce(fn AckFunc) AckFunc {
	var once sync.Once
	return func(ctx context.Context, err error) error {
		var ackErr error
		once.Do(func() {
			ackErr = fn(ctx, err)
		})
		return ackErr
	}
}

type managedAckBatchScanner struct {
	strm SimpleBatchScanner

	sourceAck AckFunc
	mut       sync.Mutex
	finished  bool
	pending   int32
}

func (s *managedAckBatchScanner) ack(ctx context.Context, err error) error {
	s.mut.Lock()
	s.pending--
	doAck := s.pending == 0 && s.finished
	s.mut.Unlock()

	if err != nil {
		return s.sourceAck(ctx, err)
	}
	if doAck {
		return s.sourceAck(ctx, nil)
	}
	return nil
}

func (s *managedAckBatchScanner) NextBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	b, err := s.strm.NextBatch(ctx)

	s.mut.Lock()
	defer s.mut.Unlock()

	if err == nil {
		s.pending++
		return b, s.ack, nil
	}

	if errors.Is(err, io.EOF) {
		s.finished = true
	} else {
		_ = s.sourceAck(ctx, err)
	}
	return nil, nil, err
}

func (s *managedAckBatchScanner) Close(ctx context.Context) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if !s.finished {
		_ = s.sourceAck(ctx, errors.New("service shutting down"))
	}
	if s.pending == 0 {
		_ = s.sourceAck(ctx, nil)
	}
	return s.strm.Close(ctx)
}

//------------------------------------------------------------------------------

// Implements reader.Codec.
type airGapBatchScannerCreator struct {
	r BatchScannerCreator
}

func newAirGapBatchScannerCreator(r BatchScannerCreator) scanner.Creator {
	return &airGapBatchScannerCreator{r: r}
}

func (a *airGapBatchScannerCreator) Create(rdr io.ReadCloser, aFn scanner.AckFn, details scanner.SourceDetails) (scanner.Scanner, error) {
	s, err := a.r.Create(rdr, AckFunc(aFn), &ScannerSourceDetails{details: details})
	if err != nil {
		return nil, err
	}
	return &airGapBatchScanner{r: s}, nil
}

func (a *airGapBatchScannerCreator) Close(ctx context.Context) error {
	return a.r.Close(ctx)
}

// Implements reader.Stream.
type airGapBatchScanner struct {
	r BatchScanner
}

func (a *airGapBatchScanner) Next(ctx context.Context) (message.Batch, scanner.AckFn, error) {
	b, ackFn, err := a.r.NextBatch(ctx)
	if err != nil {
		return nil, nil, publicToInternalErr(err)
	}
	tBatch := make(message.Batch, len(b))
	for i, m := range b {
		tBatch[i] = m.part
	}
	return tBatch, scanner.AckFn(ackFn), nil
}

func (a *airGapBatchScanner) Close(ctx context.Context) error {
	return a.r.Close(ctx)
}

//------------------------------------------------------------------------------

// OwnedScannerCreator provides direct ownership of a batch scanner
// extracted from a plugin config.
type OwnedScannerCreator struct {
	rdr scanner.Creator
}

// Create a new scanner from an io.ReadCloser along with optional information
// about the source of the reader and a function to be called once the
// underlying data has been read and acknowledged in its entirety.
func (s *OwnedScannerCreator) Create(rdr io.ReadCloser, aFn AckFunc, details *ScannerSourceDetails) (*OwnedScanner, error) {
	var iDetails scanner.SourceDetails
	if details != nil {
		iDetails = details.details
	}
	is, err := s.rdr.Create(rdr, scanner.AckFn(aFn), iDetails)
	if err != nil {
		return nil, err
	}
	return &OwnedScanner{strm: is}, nil
}

func (s *OwnedScannerCreator) Close(ctx context.Context) error {
	return s.rdr.Close(ctx)
}

// OwnedScanner provides direct ownership of a scanner.
type OwnedScanner struct {
	strm scanner.Scanner
}

// NextBatch attempts to further consume the underlying reader in order to
// extract another message (or multiple).
func (s *OwnedScanner) NextBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	ib, aFn, err := s.strm.Next(ctx)
	if err != nil {
		return nil, nil, err
	}

	batch := make(MessageBatch, len(ib))
	for i := range ib {
		batch[i] = NewInternalMessage(ib[i])
	}
	return batch, AckFunc(aFn), nil
}

// Close the scanner, indicating that it will no longer be consumed.
func (s *OwnedScanner) Close(ctx context.Context) error {
	return s.strm.Close(ctx)
}
