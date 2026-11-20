//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
)

// streamConfigOf returns the configuration of the named stream as the server
// holds it.
func streamConfigOf(t *testing.T, app mono.MonoApplication, module, stream string) jetstream.StreamConfig {
	t.Helper()
	es, err := app.EventBus(module).EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	s, err := es.Stream(context.Background(), stream)
	if err != nil {
		t.Fatalf("Stream %s: %v", stream, err)
	}
	return s.CachedInfo().Config
}

// TestIntegration_StreamConsumerService_OneWaySettings: each setting that NATS
// lets a stream switch on but not off again. A restart with it on is left
// alone, switching it off fails Start with ErrStreamConfigImmutable naming
// the setting, and the previous configuration starts again.
func TestIntegration_StreamConsumerService_OneWaySettings(t *testing.T) {
	tests := []struct {
		name string
		set  func(*mono.StreamConfig)
	}{
		{"DenyDelete", func(s *mono.StreamConfig) { s.DenyDelete = true }},
		{"DenyPurge", func(s *mono.StreamConfig) { s.DenyPurge = true }},
		{"AllowMsgTTL", func(s *mono.StreamConfig) { s.AllowMsgTTL = true }},
		{"AllowMsgSchedules", func(s *mono.StreamConfig) { s.AllowMsgSchedules = true }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			on := mono.StreamConfig{Name: "ORDERS"}
			tt.set(&on)
			off := mono.StreamConfig{Name: "ORDERS"}

			for start := 1; start <= 2; start++ {
				_, stop, err := startImmutableStreamApp(t, dir, serviceStreamModule("orders", "ingest", on))
				stop()
				if err != nil {
					t.Fatalf("start %d with %s on: %v", start, tt.name, err)
				}
			}
			_, stop, err := startImmutableStreamApp(t, dir, serviceStreamModule("orders", "ingest", off))
			stop()
			requireImmutableStreamError(t, err,
				"stream ORDERS of stream consumer service orders.ingest",
				"has "+tt.name+" enabled and is configured with it disabled",
				"keep the setting enabled", "10052")
			if strings.Contains(err.Error(), "storage type of an existing stream") {
				t.Errorf("error %q names storage and retention, which did not change", err.Error())
			}
			_, stop, err = startImmutableStreamApp(t, dir, serviceStreamModule("orders", "ingest", on))
			stop()
			if err != nil {
				t.Fatalf("Start with %s on again: %v", tt.name, err)
			}
		})
	}
}

// TestIntegration_StreamConsumerService_Sealed: a stream is sealed after it
// was created and stays startable, although sealing switches DenyDelete and
// DenyPurge on at the server. Unsealing fails Start and names Sealed only.
// Sealing a stream that does not exist yet fails with a clear error.
func TestIntegration_StreamConsumerService_Sealed(t *testing.T) {
	module := func(sealed bool) *immutableStreamModule {
		return serviceStreamModule("orders", "archive", mono.StreamConfig{Name: "ARCHIVE", Sealed: sealed})
	}

	t.Run("sealed after creation", func(t *testing.T) {
		dir := t.TempDir()
		_, stop, err := startImmutableStreamApp(t, dir, module(false))
		stop()
		if err != nil {
			t.Fatalf("Start unsealed: %v", err)
		}
		for start := 1; start <= 3; start++ {
			app, stop, err := startImmutableStreamApp(t, dir, module(true))
			if err != nil {
				stop()
				t.Fatalf("start %d sealed: %v", start, err)
			}
			if cfg := streamConfigOf(t, app, "orders", "ARCHIVE"); !cfg.Sealed || !cfg.DenyDelete || !cfg.DenyPurge {
				t.Errorf("start %d: Sealed=%v DenyDelete=%v DenyPurge=%v, want all on", start, cfg.Sealed, cfg.DenyDelete, cfg.DenyPurge)
			}
			stop()
		}

		_, stop, err = startImmutableStreamApp(t, dir, module(false))
		stop()
		requireImmutableStreamError(t, err, "stream ARCHIVE", "has Sealed enabled and is configured with it disabled")
		for _, implied := range []string{"has DenyDelete enabled", "has DenyPurge enabled"} {
			if strings.Contains(err.Error(), implied) {
				t.Errorf("error %q names %q, which the configuration never set", err.Error(), implied)
			}
		}
	})

	t.Run("a stream with roll-ups is sealed", func(t *testing.T) {
		// Sealing switches roll-ups off; a configuration that still has
		// AllowRollup set must keep starting.
		dir := t.TempDir()
		rollup := func(sealed bool) *immutableStreamModule {
			return serviceStreamModule("orders", "archive", mono.StreamConfig{Name: "ARCHIVE", AllowRollup: true, Sealed: sealed})
		}
		_, stop, err := startImmutableStreamApp(t, dir, rollup(false))
		stop()
		if err != nil {
			t.Fatalf("Start unsealed: %v", err)
		}
		for start := 1; start <= 3; start++ {
			app, stop, err := startImmutableStreamApp(t, dir, rollup(true))
			if err != nil {
				stop()
				t.Fatalf("start %d sealed: %v", start, err)
			}
			if cfg := streamConfigOf(t, app, "orders", "ARCHIVE"); !cfg.Sealed || cfg.AllowRollup {
				t.Errorf("start %d: Sealed=%v AllowRollup=%v, want sealed without roll-ups", start, cfg.Sealed, cfg.AllowRollup)
			}
			stop()
		}
	})

	t.Run("a stream with a subject delete marker TTL is sealed", func(t *testing.T) {
		dir := t.TempDir()
		marker := func(sealed bool) *immutableStreamModule {
			return serviceStreamModule("orders", "archive", mono.StreamConfig{
				Name: "ARCHIVE", MaxAge: time.Hour, SubjectDeleteMarkerTTL: time.Minute, Sealed: sealed,
			})
		}
		_, stop, err := startImmutableStreamApp(t, dir, marker(false))
		stop()
		if err != nil {
			t.Fatalf("Start unsealed: %v", err)
		}
		for start := 1; start <= 4; start++ {
			app, stop, err := startImmutableStreamApp(t, dir, marker(true))
			if err != nil {
				stop()
				t.Fatalf("start %d sealed: %v", start, err)
			}
			cfg := streamConfigOf(t, app, "orders", "ARCHIVE")
			t.Logf("start %d: Sealed=%v DenyDelete=%v DenyPurge=%v AllowRollup=%v AllowMsgTTL=%v marker=%v",
				start, cfg.Sealed, cfg.DenyDelete, cfg.DenyPurge, cfg.AllowRollup, cfg.AllowMsgTTL, cfg.SubjectDeleteMarkerTTL)
			// Pinned against a server upgrade: sealing wins over the marker
			// TTL, which alone would clear DenyPurge and force roll-ups on.
			if !cfg.Sealed || !cfg.DenyDelete || !cfg.DenyPurge || cfg.AllowRollup || !cfg.AllowMsgTTL {
				t.Errorf("start %d: Sealed=%v DenyDelete=%v DenyPurge=%v AllowRollup=%v AllowMsgTTL=%v, "+
					"want sealed with both denials and per-message TTLs on and roll-ups off",
					start, cfg.Sealed, cfg.DenyDelete, cfg.DenyPurge, cfg.AllowRollup, cfg.AllowMsgTTL)
			}
			stop()
		}
	})

	t.Run("a stream with message schedules is not sealed", func(t *testing.T) {
		// The server would seal it and then reject every later write, for
		// good. Start refuses before the stream is touched.
		dir := t.TempDir()
		schedules := func(sealed bool) *immutableStreamModule {
			return serviceStreamModule("orders", "archive", mono.StreamConfig{Name: "ARCHIVE", AllowMsgSchedules: true, Sealed: sealed})
		}
		_, stop, err := startImmutableStreamApp(t, dir, schedules(false))
		stop()
		if err != nil {
			t.Fatalf("Start unsealed: %v", err)
		}
		_, stop, err = startImmutableStreamApp(t, dir, schedules(true))
		stop()
		if !errors.Is(err, monoerrors.ErrInvalidConfiguration) || !strings.Contains(err.Error(), "Sealed and AllowMsgSchedules") {
			t.Fatalf("Start error = %v, want ErrInvalidConfiguration about Sealed and AllowMsgSchedules", err)
		}
		app, stop, err := startImmutableStreamApp(t, dir, schedules(false))
		defer stop()
		if err != nil {
			t.Fatalf("Start without Sealed again: %v", err)
		}
		if streamConfigOf(t, app, "orders", "ARCHIVE").Sealed {
			t.Error("the stream was sealed by the refused start")
		}
	})

	t.Run("sealed before the stream exists", func(t *testing.T) {
		_, stop, err := startImmutableStreamApp(t, t.TempDir(), module(true))
		stop()
		if !errors.Is(err, monoerrors.ErrInvalidConfiguration) {
			t.Fatalf("Start error = %v, want ErrInvalidConfiguration", err)
		}
		for _, part := range []string{"stream ARCHIVE of stream consumer service orders.archive", "does not exist yet", "start once without Sealed"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("Start error %q does not contain %q", err.Error(), part)
			}
		}
	})
}

// TestIntegration_EventStreamConsumer_OneWaySetting: the same detection for
// the stream of an event stream consumer.
func TestIntegration_EventStreamConsumer_OneWaySetting(t *testing.T) {
	dir := t.TempDir()
	module := func(denyPurge bool) *immutableStreamModule {
		return eventStreamModule("billing", mono.StreamConfig{Name: "ORDER_EVENTS", DenyPurge: denyPurge})
	}
	for start := 1; start <= 2; start++ {
		_, stop, err := startImmutableStreamApp(t, dir, module(true))
		stop()
		if err != nil {
			t.Fatalf("start %d with DenyPurge on: %v", start, err)
		}
	}
	_, stop, err := startImmutableStreamApp(t, dir, module(false))
	stop()
	requireImmutableStreamError(t, err,
		"stream ORDER_EVENTS of event stream consumer of module billing for event immutable-orders.OrderPlaced",
		"has DenyPurge enabled and is configured with it disabled")
}

// TestIntegration_StreamRegistrations_DisagreeOnOneWaySetting: two
// registrations of one stream of which only one enables a one-way setting
// fail the first Start, in either order, before the stream is created.
func TestIntegration_StreamRegistrations_DisagreeOnOneWaySetting(t *testing.T) {
	for _, firstOn := range []bool{true, false} {
		name := "the first enables it"
		if !firstOn {
			name = "the second enables it"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			module := func(first, second bool) *immutableStreamModule {
				return &immutableStreamModule{name: "orders", regs: []immutableStreamReg{
					{service: "first", stream: mono.StreamConfig{Name: "SHARED", DenyDelete: first}},
					{service: "second", stream: mono.StreamConfig{Name: "SHARED", DenyDelete: second}},
				}}
			}
			_, stop, err := startImmutableStreamApp(t, dir, module(firstOn, !firstOn))
			stop()
			requireImmutableStreamError(t, err,
				"stream SHARED is registered by the stream consumer service orders.first and by the stream consumer service orders.second",
				"only one enables DenyDelete")

			// The failed start created nothing: had it enabled DenyDelete,
			// this start with it off would fail.
			_, stop, err = startImmutableStreamApp(t, dir, module(false, false))
			stop()
			if err != nil {
				t.Fatalf("Start with agreeing registrations: %v", err)
			}
		})
	}
}

// oneWayCronModule registers one cron service.
type oneWayCronModule struct {
	config mono.CronServiceConfig
}

func (m *oneWayCronModule) Name() string                { return "reports" }
func (m *oneWayCronModule) Start(context.Context) error { return nil }
func (m *oneWayCronModule) Stop(context.Context) error  { return nil }
func (m *oneWayCronModule) RegisterServices(c mono.ServiceContainer) error {
	return c.RegisterCronService("rollup", m.config, func(context.Context, *mono.Msg) error { return nil })
}

// TestIntegration_CronService_TTLRemoved: a cron service whose TTL is removed
// starts, on that start and on the following ones. The stream keeps
// per-message TTLs enabled, which the server cannot switch off. A cron
// service that never had a TTL does not get them enabled.
func TestIntegration_CronService_TTLRemoved(t *testing.T) {
	const stream = "MONO_CRON_reports_rollup"
	start := func(t *testing.T, dir string, ttl time.Duration) jetstream.StreamConfig {
		t.Helper()
		app, err := mono.NewMonoApplication(
			mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithJetStreamStorageDir(dir),
			mono.WithNATSDontListen(),
			mono.WithNATSInProcessConn(),
		)
		if err != nil {
			t.Fatalf("NewMonoApplication: %v", err)
		}
		if err := app.Register(&oneWayCronModule{config: mono.CronServiceConfig{Schedule: "@every 1h", Payload: []byte("tick"), TTL: ttl}}); err != nil {
			t.Fatalf("Register: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		defer func() { _ = app.Stop(context.Background()) }()
		if err := app.Start(ctx); err != nil {
			t.Fatalf("Start with TTL %v: %v", ttl, err)
		}
		return streamConfigOf(t, app, "reports", stream)
	}

	t.Run("TTL added and removed again", func(t *testing.T) {
		dir := t.TempDir()
		steps := []struct {
			ttl     time.Duration
			wantTTL bool
		}{
			{0, false},
			{time.Hour, true},
			{0, true}, // removed: the stream keeps the setting
			{0, true}, // and keeps starting
			{time.Hour, true},
		}
		for i, step := range steps {
			if got := start(t, dir, step.ttl).AllowMsgTTL; got != step.wantTTL {
				t.Errorf("start %d (TTL %v): AllowMsgTTL = %v, want %v", i+1, step.ttl, got, step.wantTTL)
			}
		}
	})

	t.Run("never had a TTL", func(t *testing.T) {
		dir := t.TempDir()
		for i := 1; i <= 2; i++ {
			if start(t, dir, 0).AllowMsgTTL {
				t.Errorf("start %d: AllowMsgTTL is on for a cron service that never had a TTL", i)
			}
		}
	})
}
