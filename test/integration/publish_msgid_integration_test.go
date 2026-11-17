//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
)

type msgIDPayload struct {
	Order string `json:"order"`
}

// TestPublish_WithMsgIDDeduplicates covers issue #38: a publish retried with
// the same message ID is stored once. The stream acknowledges the repeat as a
// duplicate with the sequence of the stored message, and the consumer
// receives the message once.
func TestPublish_WithMsgIDDeduplicates(t *testing.T) {
	def := helper.StreamConsumerService[msgIDPayload]("orders", "placed")

	var mu sync.Mutex
	var received []string
	module := &testModule{name: "orders"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		config := mono.StreamConsumerConfig{
			Stream:   mono.StreamConfig{Name: "ORDERS_PLACED", Subjects: []string{def.ToBase().Subject()}},
			Consumer: mono.ConsumerConfig{AckPolicy: mono.AckExplicitPolicy},
			Fetch:    mono.FetchConfig{BatchSize: 10, Timeout: 200 * time.Millisecond},
		}
		return def.Register(container, config, func(_ context.Context, payloads []msgIDPayload, msgs []*mono.Msg) error {
			mu.Lock()
			defer mu.Unlock()
			for i, p := range payloads {
				received = append(received, p.Order)
				if err := msgs[i].Ack(); err != nil {
					return err
				}
			}
			return nil
		})
	}

	fw := setupFrameworkForTest(t)
	if err := fw.Register(module); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = fw.Stop(context.Background()) })

	services := fw.Services("orders")
	first, err := def.Publish(ctx, services, msgIDPayload{Order: "o-1"}, helper.WithMsgID("order-o-1"))
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	retry, err := def.Publish(ctx, services, msgIDPayload{Order: "o-1"}, helper.WithMsgID("order-o-1"))
	if err != nil {
		t.Fatalf("retried publish: %v", err)
	}
	other, err := def.Publish(ctx, services, msgIDPayload{Order: "o-2"}, helper.WithMsgID("order-o-2"))
	if err != nil {
		t.Fatalf("other publish: %v", err)
	}
	plain, err := def.Publish(ctx, services, msgIDPayload{Order: "o-3"})
	if err != nil {
		t.Fatalf("publish without ID: %v", err)
	}

	if first.Duplicate() || other.Duplicate() || plain.Duplicate() {
		t.Errorf("only the retry is a duplicate: first=%v other=%v plain=%v", first.Duplicate(), other.Duplicate(), plain.Duplicate())
	}
	if !retry.Duplicate() || retry.Sequence() != first.Sequence() {
		t.Errorf("retry: duplicate=%v sequence=%d, want true and %d", retry.Duplicate(), retry.Sequence(), first.Sequence())
	}
	if other.Sequence() != first.Sequence()+1 || plain.Sequence() != first.Sequence()+2 {
		t.Errorf("sequences first=%d other=%d plain=%d: the duplicate must not take a sequence", first.Sequence(), other.Sequence(), plain.Sequence())
	}

	if _, err := def.Publish(ctx, services, msgIDPayload{}, helper.WithMsgID("")); !errors.Is(err, helper.ErrEmptyMsgID) {
		t.Errorf("empty ID: err = %v, want ErrEmptyMsgID", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // a duplicate would arrive meanwhile
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 || received[0] != "o-1" || received[1] != "o-2" || received[2] != "o-3" {
		t.Errorf("received = %v, want [o-1 o-2 o-3] (the retried o-1 stored once)", received)
	}
}
