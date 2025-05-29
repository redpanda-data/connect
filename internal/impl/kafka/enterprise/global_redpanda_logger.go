// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type topicLogger struct {
	id string

	pipelineID    *atomic.Pointer[string]
	topic         *atomic.Pointer[string]
	o             *atomic.Pointer[service.OwnedOutput]
	level         *atomic.Pointer[slog.Level]
	pendingWrites *atomic.Int64
	attrs         []slog.Attr
}

func newTopicLogger(id string) *topicLogger {
	t := &topicLogger{
		id:            id,
		pipelineID:    &atomic.Pointer[string]{},
		topic:         &atomic.Pointer[string]{},
		o:             &atomic.Pointer[service.OwnedOutput]{},
		level:         &atomic.Pointer[slog.Level]{},
		pendingWrites: &atomic.Int64{},
	}
	return t
}

func (l *topicLogger) InitWithOutput(pipelineID, topic string, logsLevel slog.Level, o *service.OwnedOutput) {
	l.pipelineID.Store(&pipelineID)
	l.topic.Store(&topic)
	l.level.Store(&logsLevel)
	l.o.Store(o)
}

// Enabled returns true if the logger is enabled and false otherwise.
func (l *topicLogger) Enabled(_ context.Context, atLevel slog.Level) bool {
	lvl := l.level.Load()
	if lvl == nil {
		return true
	}
	return atLevel >= *lvl
}

func (l *topicLogger) Handle(_ context.Context, r slog.Record) error {
	topic, level, pipelineID := l.topic.Load(), l.level.Load(), l.pipelineID.Load()
	if topic == nil || level == nil || pipelineID == nil {
		return nil
	}

	if r.Level < *level {
		return nil
	}

	msg := service.NewMessage(nil)

	v := map[string]any{
		"message":     r.Message,
		"level":       r.Level.String(),
		"time":        r.Time.Format(time.RFC3339Nano),
		"instance_id": l.id,
		"pipeline_id": *pipelineID,
	}
	for _, a := range l.attrs {
		v[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		v[a.Key] = a.Value.String()
		return true
	})
	msg.SetStructured(v)
	msg.MetaSetMut(topicMetaKey, *topic)
	msg.MetaSetMut(keyMetaKey, *pipelineID)

	tmpO := l.o.Load()
	if tmpO == nil {
		return nil
	}

	l.pendingWrites.Add(1)
	if err := tmpO.WriteBatchNonBlocking(service.MessageBatch{msg}, func(context.Context, error) error {
		l.pendingWrites.Add(-1)
		return nil
	}); err != nil {
		l.pendingWrites.Add(-1)
	}
	return nil
}

func (l *topicLogger) WithAttrs(attrs []slog.Attr) slog.Handler {
	newL := *l
	newAttributes := make([]slog.Attr, 0, len(attrs)+len(l.attrs))
	newAttributes = append(newAttributes, l.attrs...)
	newAttributes = append(newAttributes, attrs...)
	newL.attrs = newAttributes
	return &newL
}

func (l *topicLogger) WithGroup(string) slog.Handler {
	return l // TODO
}

func (l *topicLogger) Close(ctx context.Context) error {
	for l.pendingWrites.Load() > 0 {
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
