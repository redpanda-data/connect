//go:build go1.21

package service

import (
	"log/slog"

	"github.com/benthosdev/benthos/v4/internal/log"
)

// SetLogger sets a customer logger via Go's standard logging interface,
// allowing you to replace the default Benthos logger with your own.
func (s *StreamBuilder) SetLogger(l *slog.Logger) {
	s.customLogger = log.NewBenthosLogAdapter(l)
}
