//go:build integration
// +build integration

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
)

// cronTestModule registers a single cron service and records every delivered
// occurrence. It can optionally fail the very first delivery (to exercise the
// framework's auto-Nak/redelivery path).
type cronTestModule struct {
	name      string
	service   string
	config    mono.CronServiceConfig
	eventBus  mono.EventBus
	failFirst bool

	mu       sync.Mutex
	received [][]byte
	subjects []string

	invocations int64
	acks        int64
	failed      int64

	threshold int64
	doneCh    chan struct{}
}

func newCronTestModule(name, service string, config mono.CronServiceConfig, threshold int64) *cronTestModule {
	return &cronTestModule{
		name:      name,
		service:   service,
		config:    config,
		threshold: threshold,
		doneCh:    make(chan struct{}),
	}
}

func (m *cronTestModule) Name() string                       { return m.name }
func (m *cronTestModule) Dependencies() []string             { return nil }
func (m *cronTestModule) Start(_ context.Context) error      { return nil }
func (m *cronTestModule) Stop(_ context.Context) error       { return nil }
func (m *cronTestModule) SetEventBus(eventBus mono.EventBus) { m.eventBus = eventBus }

func (m *cronTestModule) RegisterServices(container mono.ServiceContainer) error {
	return container.RegisterCronService(m.service, m.config, m.handle)
}

func (m *cronTestModule) handle(_ context.Context, msg *mono.Msg) error {
	atomic.AddInt64(&m.invocations, 1)

	// Induce exactly one failure to verify the framework Naks and redelivers.
	if m.failFirst && atomic.CompareAndSwapInt64(&m.failed, 0, 1) {
		return fmt.Errorf("induced failure")
	}

	m.mu.Lock()
	m.received = append(m.received, msg.Data)
	m.subjects = append(m.subjects, msg.Subject)
	m.mu.Unlock()

	if atomic.AddInt64(&m.acks, 1) >= m.threshold {
		select {
		case <-m.doneCh:
		default:
			close(m.doneCh)
		}
	}
	return nil
}

func (m *cronTestModule) waitForTicks(timeout time.Duration) bool {
	select {
	case <-m.doneCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (m *cronTestModule) firstReceived() ([]byte, string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.received) == 0 {
		return nil, "", false
	}
	return m.received[0], m.subjects[0], true
}

// startCronFramework wires a module into a fresh JetStream-enabled framework and
// starts it. The caller is responsible for stopping the framework.
func startCronFramework(t *testing.T, module mono.Module) mono.MonoApplication {
	t.Helper()
	fw := setupFrameworkForTest(t)
	if err := fw.Register(module); err != nil {
		fw.Stop(context.Background())
		t.Fatalf("Register failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fw.Start(ctx); err != nil {
		fw.Stop(context.Background())
		t.Fatalf("Start failed: %v", err)
	}
	return fw
}

func TestCronServiceIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("static payload fires on schedule", func(t *testing.T) {
		module := newCronTestModule("reports", "rollup", mono.CronServiceConfig{
			Schedule: "@every 1s",
			Payload:  []byte(`{"job":"rollup"}`),
		}, 2)

		fw := startCronFramework(t, module)
		defer fw.Stop(context.Background())

		if !module.waitForTicks(8 * time.Second) {
			t.Fatalf("expected >=2 ticks, got %d", atomic.LoadInt64(&module.acks))
		}

		data, subject, ok := module.firstReceived()
		if !ok {
			t.Fatal("no tick recorded")
		}
		if string(data) != `{"job":"rollup"}` {
			t.Errorf("payload = %q, want rollup payload", data)
		}
		if subject != "services.reports.rollup" {
			t.Errorf("subject = %q, want services.reports.rollup", subject)
		}
	})

	t.Run("deprecated schedule does not fire", func(t *testing.T) {
		module := newCronTestModule("reports", "rollup", mono.CronServiceConfig{
			Schedule:   "@every 1s",
			Payload:    []byte("tick"),
			Deprecated: true,
		}, 1)

		fw := startCronFramework(t, module)
		defer fw.Stop(context.Background())

		// Give the schedule ample time to (not) fire.
		if module.waitForTicks(3 * time.Second) {
			t.Fatal("deprecated cron should not deliver any ticks")
		}
		if got := atomic.LoadInt64(&module.invocations); got != 0 {
			t.Errorf("handler invoked %d times, want 0", got)
		}
	})

	t.Run("error is auto-nakked and redelivered", func(t *testing.T) {
		module := newCronTestModule("reports", "rollup", mono.CronServiceConfig{
			Schedule: "@every 1s",
			Payload:  []byte("tick"),
		}, 2)
		module.failFirst = true

		fw := startCronFramework(t, module)
		defer fw.Stop(context.Background())

		if !module.waitForTicks(10 * time.Second) {
			t.Fatalf("expected >=2 successful ticks, got %d", atomic.LoadInt64(&module.acks))
		}

		// One occurrence failed and was redelivered, so total invocations must
		// exceed the number of successful acks (the framework owns ack/nak).
		invocations := atomic.LoadInt64(&module.invocations)
		acks := atomic.LoadInt64(&module.acks)
		if invocations <= acks {
			t.Errorf("expected redelivery (invocations > acks); invocations=%d acks=%d", invocations, acks)
		}
	})

	t.Run("source subject delivers last message", func(t *testing.T) {
		const sourceSubject = "events.metrics.snapshot"
		module := newCronTestModule("metrics", "downsample", mono.CronServiceConfig{
			Schedule:      "@every 1s",
			SourceSubject: sourceSubject,
		}, 1)

		fw := startCronFramework(t, module)
		defer fw.Stop(context.Background())

		// Publish the source message that each occurrence should carry.
		js, err := module.eventBus.EventStream()
		if err != nil {
			t.Fatalf("EventStream failed: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := js.Publish(ctx, sourceSubject, []byte("snapshot-payload")); err != nil {
			t.Fatalf("publish to source subject failed: %v", err)
		}

		if !module.waitForTicks(8 * time.Second) {
			t.Fatal("expected a tick carrying the source payload")
		}
		data, subject, _ := module.firstReceived()
		if string(data) != "snapshot-payload" {
			t.Errorf("payload = %q, want snapshot-payload", data)
		}
		if subject != "services.metrics.downsample" {
			t.Errorf("subject = %q, want services.metrics.downsample", subject)
		}
	})

	t.Run("fails fast without JetStream", func(t *testing.T) {
		fw, err := mono.NewMonoApplication(
			mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithNATSDontListen(),
			mono.WithNATSInProcessConn(),
			// JetStream intentionally NOT enabled.
		)
		if err != nil {
			t.Fatalf("NewMonoApplication failed: %v", err)
		}
		defer fw.Stop(context.Background())

		module := newCronTestModule("reports", "rollup", mono.CronServiceConfig{
			Schedule: "@every 1s",
			Payload:  []byte("tick"),
		}, 1)
		if err := fw.Register(module); err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := fw.Start(ctx); err == nil {
			t.Fatal("expected Start to fail when JetStream is not enabled")
		}
	})
}
