package timeplus

import "context"

type Writer interface {
	Write(ctx context.Context, cols []string, rows [][]any) error
}
