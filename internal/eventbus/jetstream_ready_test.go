package eventbus

import (
	"context"
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

func TestNatsJetStream_ProtectConsumers(t *testing.T) {
	conn := setupTestNATS(t)
	js, err := NewJetStream(conn, logger.NewDefaultLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := js.js.CreateStream(ctx, jetstream.StreamConfig{Name: "PROTECT", Subjects: []string{"protect.>"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.js.CreateOrUpdateConsumer(ctx, "PROTECT", jetstream.ConsumerConfig{Name: "plain", AckPolicy: jetstream.AckExplicitPolicy}); err != nil {
		t.Fatal(err)
	}
	if protected, complete := js.ProtectConsumers(ctx, time.Hour); protected != 1 || !complete {
		t.Fatalf("ProtectConsumers = %d, %v; want 1, true", protected, complete)
	}
	info, err := js.ConsumerInfo(ctx, "PROTECT", "plain")
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.InactiveThreshold != time.Hour || info.Config.Durable != "" {
		t.Errorf("after protection: threshold %s durable %q", info.Config.InactiveThreshold, info.Config.Durable)
	}
}
