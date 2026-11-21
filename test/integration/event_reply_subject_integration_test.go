//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
)

// An event can carry a reply subject: one published through JetStream is a
// request whose reply subject is the inbox of the publish acknowledgement. A
// full event consumer pool treated it as a request, so the event was lost for
// the consumer and answered with an overloaded error (issue #87).

// TestEventConsumer_EventWithReplySubjectIsHeldBack: every event with a reply
// subject is handled by a consumer whose pool is full, and nothing is sent to
// the reply subject.
func TestEventConsumer_EventWithReplySubjectIsHeldBack(t *testing.T) {
	fw := newWorkerPoolApp(t)
	emitter := &poolEmitterModule{}
	consumer := &poolConsumerModule{
		name:  "pool-consumer",
		delay: 20 * time.Millisecond,
		opts:  &mono.EventConsumerOptions{ServiceOptions: mono.ServiceOptions{MaxConcurrency: 1, MaxPending: 1}},
	}
	for _, m := range []mono.Module{emitter, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.Name(), err)
		}
	}
	startWorkerPoolApp(t, fw)

	const replySubject = "test.event-replies"
	var answers atomic.Int32
	sub, err := emitter.eventBus.Subscribe(replySubject, func(context.Context, *mono.Msg) { answers.Add(1) })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	const events = 10
	for i := range events {
		data, _ := json.Marshal(poolTickEvent{ID: i})
		msg := &mono.Msg{Subject: poolTickEventDef.Subject, Reply: replySubject, Data: data}
		if err := emitter.eventBus.PublishMsg(msg); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitForCount(t, func() int { return len(consumer.tracker.finished()) }, events, 10*time.Second)
	if n := answers.Load(); n != 0 {
		t.Errorf("%d messages were sent to the events' reply subject, want none", n)
	}
	for i, id := range consumer.tracker.finished() {
		if id != i {
			t.Fatalf("event %d was handled at position %d: order not kept", id, i)
		}
	}
}

// TestEventConsumer_JetStreamPublishedEventsAreAllHandled: a burst of events
// published through EventStream, to a subject a stream holds, is handled
// completely by a core event consumer with the default options, and no
// publish fails.
func TestEventConsumer_JetStreamPublishedEventsAreAllHandled(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithJetStreamStorageDir(t.TempDir()))
	emitter := &poolEmitterModule{}
	consumer := &poolConsumerModule{name: "pool-consumer", delay: 5 * time.Millisecond}
	for _, m := range []mono.Module{emitter, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.Name(), err)
		}
	}
	startWorkerPoolApp(t, fw)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	es, err := emitter.eventBus.EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	subject := poolTickEventDef.Subject
	if _, err := es.CreateOrUpdateStream(ctx, mono.StreamConfig{Name: "TICKS", Subjects: []string{subject}}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// More than the default pending queue (256) plus the one running, and
	// published far faster than the handler takes them.
	const events = 400
	for i := range events {
		data, _ := json.Marshal(poolTickEvent{ID: i})
		if _, err := es.Publish(ctx, subject, data); err != nil {
			t.Fatalf("publish %d of an event the stream stores: %v", i, err)
		}
	}
	waitForCount(t, func() int { return len(consumer.tracker.finished()) }, events, 30*time.Second)
}
