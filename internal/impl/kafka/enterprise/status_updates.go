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
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/proto"

	"github.com/redpanda-data/connect/v4/internal/protoconnect"
)

// TriggerEventConfigParsed dispatches a connectivity event that states the
// service has successfully parsed a configuration file and is going to attempt
// to run it.
func (l *TopicLogger) TriggerEventConfigParsed() {
	l.sendStatusEvent(&protoconnect.StatusEvent{
		PipelineId: l.pipelineID,
		InstanceId: l.id,
		Type:       protoconnect.StatusEvent_TYPE_INITIALIZING,
		Timestamp:  time.Now().Unix(),
	})
}

// SetStreamSummary configures a stream summary to use for broadcasting
// connectivity statuses.
//
// This call triggers an event that the config has been run successfully.
func (l *TopicLogger) SetStreamSummary(s *service.RunningStreamSummary) {
	l.streamStatus.Store(s)
}

// TriggerEventStopped dispatches a connectivity event that states the service
// has stopped, either by intention or due to an issue described in the provided
// error.
func (l *TopicLogger) TriggerEventStopped(err error) {
	var eErr *protoconnect.ExitError
	if err != nil {
		eErr = &protoconnect.ExitError{
			Message: err.Error(),
		}
	}
	l.sendStatusEvent(&protoconnect.StatusEvent{
		PipelineId: l.pipelineID,
		InstanceId: l.id,
		Type:       protoconnect.StatusEvent_TYPE_EXITING,
		Timestamp:  time.Now().Unix(),
		ExitError:  eErr,
	})
}

func (l *TopicLogger) sendStatusEvent(e *protoconnect.StatusEvent) {
	tmpO := l.o.Load()
	if tmpO == nil || l.statusTopic == "" {
		return
	}

	data, err := proto.Marshal(e)
	if err != nil {
		l.fallbackLogger.Load().With("error", err).Error("Failed to marshal status event")
		return
	}

	msg := service.NewMessage(nil)
	msg.SetBytes(data)
	msg.MetaSetMut(topicMetaKey, l.statusTopic)

	_ = tmpO.WriteBatchNonBlocking(service.MessageBatch{msg}, func(ctx context.Context, err error) error {
		return nil // TODO: Log nacks
	}) // TODO: Log errors (occasionally)
}

func (l *TopicLogger) statusEventLoop() {
	for {
		_, open := <-l.streamStatusPollTicker.C
		if !open {
			return
		}

		status := l.streamStatus.Load()
		if status == nil {
			continue
		}

		e := &protoconnect.StatusEvent{
			PipelineId: l.pipelineID,
			InstanceId: l.id,
			Timestamp:  time.Now().Unix(),
			Type:       protoconnect.StatusEvent_TYPE_CONNECTION_HEALTHY,
		}

		conns := status.ConnectionStatuses()
		for _, c := range conns {
			if !c.Active() {
				e.Type = protoconnect.StatusEvent_TYPE_CONNECTION_ERROR
				cErr := &protoconnect.ConnectionError{
					Path: c.Path(),
				}
				if l := c.Label(); l != "" {
					cErr.Label = &l
				}
				if err := c.Err(); err != nil {
					cErr.Message = err.Error()
				}
				e.ConnectionErrors = append(e.ConnectionErrors, cErr)
			}
		}

		l.sendStatusEvent(e)
	}
}
