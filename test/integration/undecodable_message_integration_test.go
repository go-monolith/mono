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
	"github.com/go-monolith/mono/pkg/helper"
)

type undecodableOrder struct {
	ID int `json:"id"`
}

// One message that cannot be unmarshaled used to cost its batch neighbours:
// the typed handler was not called, nothing was acknowledged, and after
// MaxDeliver deliveries the valid messages were given up on with it. The
// handler must get the others, and the undecodable message must stay in the
// stream, unacknowledged.
func TestIntegration_TypedStreamConsumer_UndecodableMessageDoesNotCostItsBatch(t *testing.T) {
	const stream = "UNDECODABLE_ORDERS"
	fw := newWorkerPoolApp(t, mono.WithJetStreamStorageDir(t.TempDir()))
	var handled atomic.Int32
	m := &testModule{name: "orders"}
	m.registerServicesFunc = func(c mono.ServiceContainer) error {
		return helper.RegisterTypedStreamConsumerService(c, "process",
			mono.StreamConsumerConfig{
				Stream:   mono.StreamConfig{Name: stream},
				Consumer: mono.ConsumerConfig{AckWait: 500 * time.Millisecond},
				Fetch:    mono.FetchConfig{BatchSize: 10, Timeout: 300 * time.Millisecond},
			},
			json.Unmarshal,
			func(_ context.Context, orders []undecodableOrder, msgs []*mono.Msg) error {
				for i := range orders {
					if err := msgs[i].Ack(); err != nil {
						return err
					}
					handled.Add(1)
				}
				return nil
			})
	}
	if err := fw.Register(m); err != nil {
		t.Fatalf("Register: %v", err)
	}
	startWorkerPoolApp(t, fw)

	bus := fw.EventBus("")
	payloads := []string{`{"id":1}`, `{"id":2}`, `{"id":3}`, `{"id":4}`, `not json`, `{"id":5}`, `{"id":6}`, `{"id":7}`, `{"id":8}`}
	for _, p := range payloads {
		if err := bus.Publish("services.orders.process", []byte(p)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for handled.Load() < 8 {
		if time.Now().After(deadline) {
			t.Fatalf("handled %d of 8 valid messages", handled.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Past MaxDeliver (3) deliveries of the undecodable message: it was
	// neither acknowledged nor removed, and no valid message was handled twice.
	time.Sleep(2500 * time.Millisecond)
	if got := handled.Load(); got != 8 {
		t.Errorf("handled %d messages, want exactly the 8 valid ones", got)
	}
	handle := conflictStream(t, fw, stream)
	info, err := handle.Info(context.Background())
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != uint64(len(payloads)) {
		t.Errorf("stream holds %d messages, want all %d (nothing removed)", info.State.Msgs, len(payloads))
	}
	consumer := conflictConsumer(t, handle, "orders-process")
	if consumer.AckFloor.Stream != 4 {
		t.Errorf("acknowledged up to stream sequence %d, want 4 (the undecodable message is the 5th)", consumer.AckFloor.Stream)
	}
	if consumer.NumRedelivered == 0 {
		t.Error("the undecodable message was not redelivered")
	}
}
