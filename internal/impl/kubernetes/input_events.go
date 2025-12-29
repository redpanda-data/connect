// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func kubernetesEventsInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "Kubernetes").
		Version("1.0.0").
		Summary("Streams Kubernetes events in real-time, similar to `kubectl get events --watch`.").
		Description(`
This input streams Kubernetes events as they occur in the cluster.
Events provide visibility into what's happening inside your cluster:
pod scheduling, image pulls, container crashes, and more.

### Event Types

Kubernetes events have two types:
- **Normal**: Routine operations (pod scheduled, container started)
- **Warning**: Issues that may need attention (failed scheduling, image pull errors)

### Use Cases

- Build alerting pipelines for Warning events
- Track pod lifecycle for debugging
- Monitor cluster health in real-time
` + MetadataDescription([]string{
			"kubernetes_event_type",
			"kubernetes_event_reason",
			"kubernetes_event_message",
			"kubernetes_event_count",
			"kubernetes_event_first_timestamp",
			"kubernetes_event_last_timestamp",
			"kubernetes_involved_kind",
			"kubernetes_involved_name",
			"kubernetes_involved_namespace",
			"kubernetes_involved_uid",
			"kubernetes_source_component",
			"kubernetes_source_host",
		})).
		Fields(AuthFields()...).
		Fields(CommonFields()...).
		Field(service.NewStringListField("types").
			Description("Event types to include. Empty means all types.").
			Default([]any{}).
			Example([]string{"Warning"}).
			Example([]string{"Normal", "Warning"})).
		Field(service.NewStringListField("involved_kinds").
			Description("Only include events for these resource kinds (e.g., Pod, Node, Deployment).").
			Default([]any{}).
			Example([]string{"Pod"}).
			Example([]string{"Pod", "Node"})).
		Field(service.NewStringListField("reasons").
			Description("Only include events with these reasons (e.g., Failed, Scheduled, Pulled).").
			Default([]any{}).
			Example([]string{"Failed", "FailedScheduling"}).
			Optional()).
		Field(service.NewBoolField("include_existing").
			Description("Include recent existing events when starting, not just new events.").
			Default(true)).
		Field(service.NewStringField("max_event_age").
			Description("Ignore events older than this duration when include_existing is true.").
			Default("1h").
			Advanced())
}

func init() {
	err := service.RegisterInput(
		"kubernetes_events", kubernetesEventsInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newKubernetesEventsInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type kubernetesEventsInput struct {
	clientSet *ClientSet
	log       *service.Logger

	// Configuration
	namespaces      []string
	labelSelector   string
	fieldSelector   string
	types           map[string]struct{}
	involvedKinds   map[string]struct{}
	reasons         map[string]struct{}
	includeExisting bool
	maxEventAge     time.Duration

	// State
	mu           sync.Mutex
	eventChan    chan *corev1.Event
	shutSig      *shutdown.Signaller
	resourceVers string
}

func newKubernetesEventsInput(conf *service.ParsedConfig, mgr *service.Resources) (*kubernetesEventsInput, error) {
	k := &kubernetesEventsInput{
		log:       mgr.Logger(),
		eventChan: make(chan *corev1.Event, 1000),
		shutSig:   shutdown.NewSignaller(),
	}

	var err error

	// Parse namespaces
	if k.namespaces, err = conf.FieldStringList("namespaces"); err != nil {
		return nil, err
	}

	// Parse selectors
	if k.labelSelector, err = conf.FieldString("label_selector"); err != nil {
		return nil, err
	}
	if k.fieldSelector, err = conf.FieldString("field_selector"); err != nil {
		return nil, err
	}

	// Parse type filter
	typesList, err := conf.FieldStringList("types")
	if err != nil {
		return nil, err
	}
	k.types = make(map[string]struct{})
	for _, t := range typesList {
		k.types[t] = struct{}{}
	}

	// Parse involved kinds filter
	kindsList, err := conf.FieldStringList("involved_kinds")
	if err != nil {
		return nil, err
	}
	k.involvedKinds = make(map[string]struct{})
	for _, kind := range kindsList {
		k.involvedKinds[kind] = struct{}{}
	}

	// Parse reasons filter
	reasonsList, err := conf.FieldStringList("reasons")
	if err != nil {
		return nil, err
	}
	k.reasons = make(map[string]struct{})
	for _, r := range reasonsList {
		k.reasons[r] = struct{}{}
	}

	// Parse behavior options
	if k.includeExisting, err = conf.FieldBool("include_existing"); err != nil {
		return nil, err
	}

	maxAgeStr, err := conf.FieldString("max_event_age")
	if err != nil {
		return nil, err
	}
	if k.maxEventAge, err = time.ParseDuration(maxAgeStr); err != nil {
		return nil, fmt.Errorf("failed to parse max_event_age: %w", err)
	}

	// Get Kubernetes client
	if k.clientSet, err = GetClientSet(context.Background(), conf); err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	return k, nil
}

func (k *kubernetesEventsInput) Connect(ctx context.Context) error {
	// Start watch loops for each namespace
	namespaces := k.namespaces
	if len(namespaces) == 0 {
		namespaces = []string{""} // Empty string = all namespaces
	}

	for _, ns := range namespaces {
		go k.watchNamespace(ns)
	}

	return nil
}

func (k *kubernetesEventsInput) watchNamespace(namespace string) {
	client := k.clientSet.Typed
	retryAttempt := 0

	for {
		select {
		case <-k.shutSig.SoftStopChan():
			return
		default:
		}

		listOpts := metav1.ListOptions{
			LabelSelector: k.labelSelector,
			FieldSelector: k.fieldSelector,
		}

		// If we have a resource version from a previous watch, use it
		k.mu.Lock()
		resourceVer := k.resourceVers
		k.mu.Unlock()

		if resourceVer != "" {
			listOpts.ResourceVersion = resourceVer
		}

		// If include_existing is true on first run, list existing events first
		if k.includeExisting && resourceVer == "" {
			k.listExistingEvents(namespace)
		}

		// Start watching
		watchCtx, watchCancel := requestContext(0, k.shutSig.SoftStopChan())
		watcher, err := client.CoreV1().Events(namespace).Watch(watchCtx, listOpts)
		if err != nil {
			watchCancel()
			// Handle 410 Gone - resource version too old
			if serr, ok := err.(*k8serrors.StatusError); ok && serr.ErrStatus.Code == http.StatusGone {
				k.log.Warnf("Watch for events in namespace %s returned 410 Gone, resetting resource version", namespace)
				k.mu.Lock()
				k.resourceVers = ""
				k.mu.Unlock()
			} else {
				k.log.Errorf("Failed to watch events in namespace %s: %v", namespace, err)
			}
			backoff := calculateBackoff(retryAttempt)
			retryAttempt++
			select {
			case <-time.After(backoff):
			case <-k.shutSig.SoftStopChan():
				return
			}
			continue
		}

		// Reset retry counter on successful connection
		retryAttempt = 0
		k.processWatchEvents(watcher, namespace)
		watchCancel()
		watcher.Stop()
	}
}

func (k *kubernetesEventsInput) listExistingEvents(namespace string) {
	client := k.clientSet.Typed
	cutoff := time.Now().Add(-k.maxEventAge)

	listOpts := metav1.ListOptions{
		LabelSelector: k.labelSelector,
		FieldSelector: k.fieldSelector,
	}

	events, err := client.CoreV1().Events(namespace).List(context.Background(), listOpts)
	if err != nil {
		k.log.Errorf("Failed to list events in namespace %s: %v", namespace, err)
		return
	}

	// Store resource version for watch
	k.mu.Lock()
	k.resourceVers = events.ResourceVersion
	k.mu.Unlock()

	for i := range events.Items {
		event := &events.Items[i]

		// Filter by age
		eventTime := event.LastTimestamp.Time
		if eventTime.IsZero() {
			eventTime = event.EventTime.Time
		}
		if eventTime.Before(cutoff) {
			continue
		}

		if k.filterEvent(event) {
			select {
			case k.eventChan <- event:
			case <-k.shutSig.SoftStopChan():
				return
			}
		}
	}
}

func (k *kubernetesEventsInput) processWatchEvents(watcher watch.Interface, namespace string) {
	for {
		select {
		case <-k.shutSig.SoftStopChan():
			return
		case watchEvent, ok := <-watcher.ResultChan():
			if !ok {
				// Watch channel closed, need to restart
				return
			}

			switch watchEvent.Type {
			case watch.Added, watch.Modified:
				event, ok := watchEvent.Object.(*corev1.Event)
				if !ok {
					continue
				}

				// Store resource version for reconnection
				k.mu.Lock()
				k.resourceVers = event.ResourceVersion
				k.mu.Unlock()

				if k.filterEvent(event) {
					select {
					case k.eventChan <- event:
					case <-k.shutSig.SoftStopChan():
						return
					}
				}

			case watch.Bookmark:
				// Update resource version from bookmark
				if event, ok := watchEvent.Object.(*corev1.Event); ok {
					k.mu.Lock()
					k.resourceVers = event.ResourceVersion
					k.mu.Unlock()
				}

			case watch.Error:
				if status, ok := watchEvent.Object.(*metav1.Status); ok && status.Code == http.StatusGone {
					k.log.Warnf("Watch for events in namespace %s returned 410 Gone, resetting resource version", namespace)
					k.mu.Lock()
					k.resourceVers = ""
					k.mu.Unlock()
					return
				}
				k.log.Errorf("Watch error in namespace %s: %v", namespace, watchEvent.Object)
				return
			}
		}
	}
}

func (k *kubernetesEventsInput) filterEvent(event *corev1.Event) bool {
	// Filter by type
	if len(k.types) > 0 {
		if _, ok := k.types[event.Type]; !ok {
			return false
		}
	}

	// Filter by involved kind
	if len(k.involvedKinds) > 0 {
		if _, ok := k.involvedKinds[event.InvolvedObject.Kind]; !ok {
			return false
		}
	}

	// Filter by reason
	if len(k.reasons) > 0 {
		if _, ok := k.reasons[event.Reason]; !ok {
			return false
		}
	}

	return true
}

func (k *kubernetesEventsInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case event := <-k.eventChan:
		return k.eventToMessage(event)
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-k.shutSig.SoftStopChan():
		return nil, nil, service.ErrEndOfInput
	}
}

func (k *kubernetesEventsInput) eventToMessage(event *corev1.Event) (*service.Message, service.AckFunc, error) {
	// Serialize event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := service.NewMessage(eventJSON)

	// Add metadata
	msg.MetaSetMut("kubernetes_event_type", event.Type)
	msg.MetaSetMut("kubernetes_event_reason", event.Reason)
	msg.MetaSetMut("kubernetes_event_message", event.Message)
	msg.MetaSetMut("kubernetes_event_count", int64(event.Count))

	if !event.FirstTimestamp.IsZero() {
		msg.MetaSetMut("kubernetes_event_first_timestamp", event.FirstTimestamp.Format(time.RFC3339))
	}
	if !event.LastTimestamp.IsZero() {
		msg.MetaSetMut("kubernetes_event_last_timestamp", event.LastTimestamp.Format(time.RFC3339))
	}

	// Involved object metadata
	msg.MetaSetMut("kubernetes_involved_kind", event.InvolvedObject.Kind)
	msg.MetaSetMut("kubernetes_involved_name", event.InvolvedObject.Name)
	msg.MetaSetMut("kubernetes_involved_namespace", event.InvolvedObject.Namespace)
	msg.MetaSetMut("kubernetes_involved_uid", string(event.InvolvedObject.UID))

	// Source metadata
	msg.MetaSetMut("kubernetes_source_component", event.Source.Component)
	msg.MetaSetMut("kubernetes_source_host", event.Source.Host)

	return msg, func(ctx context.Context, err error) error {
		return nil // Events don't require acknowledgment
	}, nil
}

func (k *kubernetesEventsInput) Close(ctx context.Context) error {
	k.shutSig.TriggerSoftStop()
	k.shutSig.TriggerHasStopped()
	return nil
}
