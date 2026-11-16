package eventbus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/logger"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNatsJetStream_Ready(t *testing.T) {
	conn := setupTestNATS(t)
	js, err := NewJetStream(conn, logger.NewDefaultLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := js.Ready(context.Background()); err != nil {
		t.Errorf("Ready on a JetStream server: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := js.Ready(ctx); err == nil {
		t.Error("Ready with a cancelled context must fail")
	}
}

func TestNatsJetStream_ExtendInactiveThreshold(t *testing.T) {
	conn := setupTestNATS(t)
	js, err := NewJetStream(conn, logger.NewDefaultLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	raw := js.js
	if _, err := raw.CreateStream(ctx, jetstream.StreamConfig{Name: "PROTECT", Subjects: []string{"protect.>"}}); err != nil {
		t.Fatal(err)
	}
	create := func(cfg jetstream.ConsumerConfig) {
		t.Helper()
		if _, err := raw.CreateOrUpdateConsumer(ctx, "PROTECT", cfg); err != nil {
			t.Fatalf("create %s: %v", cfg.Name, err)
		}
	}
	create(jetstream.ConsumerConfig{Name: "plain", AckPolicy: jetstream.AckExplicitPolicy})
	create(jetstream.ConsumerConfig{Name: "durable", Durable: "durable", AckPolicy: jetstream.AckExplicitPolicy})
	create(jetstream.ConsumerConfig{Name: "explicit", AckPolicy: jetstream.AckExplicitPolicy, InactiveThreshold: 30 * time.Second})
	create(jetstream.ConsumerConfig{Name: "push", DeliverSubject: "push.inbox", AckPolicy: jetstream.AckExplicitPolicy})

	tests := []struct {
		consumer string
		want     bool
	}{
		{"plain", true},
		{"durable", false},
		{"explicit", false},
		{"push", false},
		{"missing", false},
	}
	for _, tt := range tests {
		t.Run(tt.consumer, func(t *testing.T) {
			got, err := js.ExtendInactiveThreshold(ctx, "PROTECT", tt.consumer, time.Hour)
			if err != nil || got != tt.want {
				t.Fatalf("ExtendInactiveThreshold = %v, %v; want %v, nil", got, err, tt.want)
			}
		})
	}

	info, err := js.ConsumerInfo(ctx, "PROTECT", "plain")
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.InactiveThreshold != time.Hour || info.Config.Durable != "" {
		t.Errorf("plain consumer after extension: threshold %s durable %q", info.Config.InactiveThreshold, info.Config.Durable)
	}
	if got, err := js.ExtendInactiveThreshold(ctx, "PROTECT", "plain", time.Hour); err != nil || got {
		t.Errorf("a second extension must leave it: %v, %v", got, err)
	}
	if _, err := js.ExtendInactiveThreshold(ctx, "NOSTREAM", "plain", time.Hour); err == nil || !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("missing stream: %v", err)
	}
}
