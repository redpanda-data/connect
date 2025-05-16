// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/protoconnect"
)

type statusEmitter struct {
	id string

	pipelineID     string
	topic          string
	fallbackLogger *service.Logger
	o              *service.OwnedOutput
	streamStatus   *atomic.Pointer[service.RunningStreamSummary]

	shutSig *shutdown.Signaller
}

func newStatusEmitter(id string) *statusEmitter {
	return &statusEmitter{
		id:           id,
		streamStatus: &atomic.Pointer[service.RunningStreamSummary]{},
		shutSig:      shutdown.NewSignaller(),
	}
}

// TriggerEventConfigParsed dispatches a connectivity event that states the
// service has successfully parsed a configuration file and is going to attempt
// to run it.
func (s *statusEmitter) TriggerEventConfigParsed() {
	s.sendStatusEvent(&protoconnect.StatusEvent{
		PipelineId: s.pipelineID,
		InstanceId: s.id,
		Type:       protoconnect.StatusEvent_TYPE_INITIALIZING,
		Timestamp:  time.Now().Unix(),
	})
}

// SetStreamSummary configures a stream summary to use for broadcasting
// connectivity statuses.
func (s *statusEmitter) SetStreamSummary(summary *service.RunningStreamSummary) {
	s.streamStatus.Store(summary)
}

// TriggerEventStopped dispatches a connectivity event that states the service
// has stopped, either by intention or due to an issue described in the provided
// error.
func (s *statusEmitter) TriggerEventStopped(err error) {
	var eErr *protoconnect.ExitError
	if err != nil {
		eErr = &protoconnect.ExitError{
			Message: err.Error(),
		}
	}
	s.sendStatusEvent(&protoconnect.StatusEvent{
		PipelineId: s.pipelineID,
		InstanceId: s.id,
		Type:       protoconnect.StatusEvent_TYPE_EXITING,
		Timestamp:  time.Now().Unix(),
		ExitError:  eErr,
	})
}

func (s *statusEmitter) sendStatusEvent(e *protoconnect.StatusEvent) {
	if s.topic == "" {
		return
	}

	data, err := protojson.Marshal(e)
	if err != nil {
		s.fallbackLogger.With("error", err).Error("Failed to marshal status event")
		return
	}

	msg := service.NewMessage(nil)
	msg.SetBytes(data)
	msg.MetaSetMut(topicMetaKey, s.topic)
	msg.MetaSetMut(keyMetaKey, s.pipelineID)

	_ = s.o.WriteBatchNonBlocking(service.MessageBatch{msg}, func(ctx context.Context, err error) error {
		return nil // TODO: Log nacks
	}) // TODO: Log errors (occasionally)
}

// Convert a slice to a dot path following https://docs.redpanda.com/redpanda-connect/configuration/field_paths/
func sliceToDotPath(path []string) string {
	var b bytes.Buffer
	for i, s := range path {
		s = strings.ReplaceAll(s, "~", "~0")
		s = strings.ReplaceAll(s, ".", "~1")
		b.WriteString(s)
		if i < len(path)-1 {
			b.WriteRune('.')
		}
	}
	return b.String()
}

func (s *statusEmitter) Close(ctx context.Context) error {
	s.shutSig.TriggerHardStop()
	select {
	case <-s.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (s *statusEmitter) InitWithOutput(pipelineID, topic string, fallbackLogger *service.Logger, o *service.OwnedOutput) {
	s.pipelineID = pipelineID
	s.topic = topic
	s.fallbackLogger = fallbackLogger
	s.o = o

	if topic == "" {
		s.shutSig.TriggerHasStopped()
		return
	}

	pollTicker := time.NewTicker(statusTickerDuration)

	go func() {
		defer s.shutSig.TriggerHasStopped()

		for {
			select {
			case <-pollTicker.C:
			case <-s.shutSig.HardStopChan():
				return
			}

			status := s.streamStatus.Load()
			if status == nil {
				continue
			}

			e := &protoconnect.StatusEvent{
				PipelineId: s.pipelineID,
				InstanceId: s.id,
				Timestamp:  time.Now().Unix(),
				Type:       protoconnect.StatusEvent_TYPE_CONNECTION_HEALTHY,
			}

			conns := status.ConnectionStatuses()
			for _, c := range conns {
				if !c.Active() {
					e.Type = protoconnect.StatusEvent_TYPE_CONNECTION_ERROR
					cErr := &protoconnect.ConnectionError{
						Path: sliceToDotPath(c.Path()),
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

			s.sendStatusEvent(e)
		}
	}()
}
