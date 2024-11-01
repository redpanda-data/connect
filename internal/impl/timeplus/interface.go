package timeplus

import "context"

// Writer is the interface. Currently only http writer is implemented. Caller needs to make sure all writes contain the same `cols`
type Writer interface {
	Write(ctx context.Context, cols []string, rows [][]any) error
}

// Reader is the interface. Called MUST guarantee that the `Run` method is called before `Read` or `Close`
type Reader interface {
	Run(sql string) error
	Read(ctx context.Context) (map[string]any, error)
	Close(ctx context.Context) error
}
