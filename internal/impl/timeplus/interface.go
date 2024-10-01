package timeplus

import "context"

// Writer is the interface. Currently only http writer is implemented. Caller needs to make sure all writes contain the same `cols`
type Writer interface {
	Write(ctx context.Context, cols []string, rows [][]any) error
}
