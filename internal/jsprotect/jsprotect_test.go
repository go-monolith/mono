package jsprotect

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNeedsProtection(t *testing.T) {
	tests := []struct {
		name string
		cfg  jetstream.ConsumerConfig
		want bool
	}{
		{"non-durable, no threshold", jetstream.ConsumerConfig{Name: "a"}, true},
		{"non-durable, default threshold", jetstream.ConsumerConfig{Name: "a", InactiveThreshold: DefaultInactiveThreshold}, true},
		{"non-durable, other threshold", jetstream.ConsumerConfig{Name: "a", InactiveThreshold: 30 * time.Second}, false},
		{"non-durable push", jetstream.ConsumerConfig{Name: "a", DeliverSubject: "x"}, false},
		{"durable", jetstream.ConsumerConfig{Name: "a", Durable: "a"}, false},
		{"interrupted conversion", jetstream.ConsumerConfig{Name: "a", Durable: "a", InactiveThreshold: DefaultInactiveThreshold}, true},
		{"durable with another name", jetstream.ConsumerConfig{Name: "a", Durable: "b", InactiveThreshold: DefaultInactiveThreshold}, false},
	}
	for _, tt := range tests {
		if got := NeedsProtection(tt.cfg); got != tt.want {
			t.Errorf("%s: NeedsProtection = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func runJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	opts := test.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := test.RunServer(&opts)
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

func TestProtect(t *testing.T) {
	js := runJetStream(t)
	ctx := context.Background()
	for _, stream := range []string{"ONE", "TWO"} {
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: stream, Subjects: []string{stream + ".>"}}); err != nil {
			t.Fatal(err)
		}
	}
	create := func(stream string, cfg jetstream.ConsumerConfig) {
		t.Helper()
		cfg.AckPolicy = jetstream.AckExplicitPolicy
		if _, err := js.CreateOrUpdateConsumer(ctx, stream, cfg); err != nil {
			t.Fatalf("create %s: %v", cfg.Name, err)
		}
	}
	create("ONE", jetstream.ConsumerConfig{Name: "plain"})
	create("ONE", jetstream.ConsumerConfig{Name: "durable", Durable: "durable"})
	create("TWO", jetstream.ConsumerConfig{Name: "explicit", InactiveThreshold: 30 * time.Second})
	create("TWO", jetstream.ConsumerConfig{Name: "push", DeliverSubject: "push.inbox"})
	create("TWO", jetstream.ConsumerConfig{Name: "halfway", Durable: "halfway", InactiveThreshold: DefaultInactiveThreshold})

	protected, complete := Protect(ctx, js, Threshold)
	if protected != 2 || !complete {
		t.Fatalf("Protect = %d, %v; want 2 (plain, halfway), true", protected, complete)
	}
	threshold := func(stream, name string) time.Duration {
		t.Helper()
		c, err := js.Consumer(ctx, stream, name)
		if err != nil {
			t.Fatalf("consumer %s: %v", name, err)
		}
		return c.CachedInfo().Config.InactiveThreshold
	}
	if got := threshold("ONE", "plain"); got != Threshold {
		t.Errorf("plain threshold = %s", got)
	}
	if got := threshold("TWO", "halfway"); got != Threshold {
		t.Errorf("halfway threshold = %s", got)
	}
	if got := threshold("TWO", "explicit"); got != 30*time.Second {
		t.Errorf("explicit threshold changed to %s", got)
	}
	if c, _ := js.Consumer(ctx, "ONE", "plain"); c.CachedInfo().Config.Durable != "" {
		t.Error("protection must not change Durable")
	}

	// A second pass finds nothing left to do.
	if protected, complete := Protect(ctx, js, Threshold); protected != 0 || !complete {
		t.Errorf("second Protect = %d, %v", protected, complete)
	}
}

// TestProtectOne_RereadsBeforeUpdating covers a consumer converted to a
// durable between the listing and the update: the update must not write the
// listed (non-durable) configuration back.
func TestProtectOne_RereadsBeforeUpdating(t *testing.T) {
	js := runJetStream(t)
	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "S", Subjects: []string{"s.>"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateOrUpdateConsumer(ctx, "S", jetstream.ConsumerConfig{Name: "c", AckPolicy: jetstream.AckExplicitPolicy}); err != nil {
		t.Fatal(err)
	}
	// Converted after it was listed: a durable with the caller's settings.
	if _, err := js.UpdateConsumer(ctx, "S", jetstream.ConsumerConfig{Name: "c", Durable: "c", AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.UpdateConsumer(ctx, "S", jetstream.ConsumerConfig{Name: "c", Durable: "c", AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 7}); err != nil {
		t.Fatal(err)
	}
	changed, err := protectOne(ctx, js, "S", "c", Threshold)
	if err != nil || changed {
		t.Fatalf("protectOne = %v, %v; want no change", changed, err)
	}
	c, err := js.Consumer(ctx, "S", "c")
	if err != nil {
		t.Fatal(err)
	}
	if cfg := c.CachedInfo().Config; cfg.Durable != "c" || cfg.MaxDeliver != 7 {
		t.Errorf("converted consumer was overwritten: %+v", cfg)
	}
	if changed, err := protectOne(ctx, js, "S", "missing", Threshold); err != nil || changed {
		t.Errorf("missing consumer: %v, %v", changed, err)
	}
}

func TestProtect_IncompleteWhenUnreachable(t *testing.T) {
	js := runJetStream(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, complete := Protect(ctx, js, Threshold); complete {
		t.Error("a pass that could not list the streams must report incomplete")
	}
}
