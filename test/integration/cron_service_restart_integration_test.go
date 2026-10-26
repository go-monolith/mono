//go:build integration
// +build integration

package integration_test

import (
	"bytes"
	"context"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// These tests cover the durable consumer of a cron service across restarts.
// Up to v0.0.12 the consumer was created with a Name but no Durable, so the
// server deleted it once nothing had pulled from it for about 5s and nothing
// awaited acknowledgement. On shutdown the framework stops the fetch loops
// before the modules and stops the embedded server last, so when modules took
// longer than that to stop, every idle cron consumer was deleted. The next
// start recreated them, and each delivered every occurrence still in the
// stream (up to 256) to the handler again.

const (
	cronRestartStream   = "MONO_CRON_reports_rollup"
	cronRestartConsumer = "reports-rollup-cron"
	cronRestartSubject  = "services.reports.rollup"

	// cronSlowStop keeps the server running without pull requests for longer
	// than its inactivity threshold for non-durable consumers. The server
	// deletes one about 5.1-6s after the last pull request arrived, which is
	// at most shortly before the shutdown began.
	cronSlowStop = 10 * time.Second

	// cronStartSequenceKey is the consumer metadata entry the framework
	// records on a cron consumer it creates, until the consumer is positioned.
	cronStartSequenceKey = "mono.cron-start-sequence"

	// cronCreatedLog starts every log line of a cron consumer being created.
	cronCreatedLog = "Cron consumer created"
)

// slowStopModule delays shutdown, like a module that drains connections. The
// framework stops consumer fetch loops before modules and the embedded server
// after them, so the server runs with no pull requests for that long.
type slowStopModule struct{ delay time.Duration }

func (m *slowStopModule) Name() string                  { return "slow-stop" }
func (m *slowStopModule) Start(_ context.Context) error { return nil }
func (m *slowStopModule) Stop(_ context.Context) error {
	time.Sleep(m.delay)
	return nil
}

// skippedPattern matches the warning logged when a missing cron consumer is
// created, capturing the number of stored occurrences it skipped.
var skippedPattern = regexp.MustCompile(`skipped occurrences stored before it.*skipped=(\d+)`)

// assertNotRecreated fails if the cron consumer was created during the boot
// that wrote logs.
func assertNotRecreated(t *testing.T, logs *syncBuffer) {
	t.Helper()
	if bytes.Contains([]byte(logs.String()), []byte(cronCreatedLog)) {
		t.Errorf("cron consumer was recreated:\n%s", logs.String())
	}
}

// pauseCronSchedule cancels the server-side schedule so that no occurrence is
// in flight when the application stops, as with an hourly or daily job. A
// message the fetch loop receives after it is stopped is Nak'd, and a message
// awaiting acknowledgement keeps even a non-durable consumer alive for
// AckWait. The next start publishes the schedule again.
func pauseCronSchedule(ctx context.Context, t *testing.T, js jetstream.JetStream) {
	t.Helper()
	msg := nats.NewMsg(cronRestartSubject + ".control")
	msg.Header.Set("Nats-Schedule-Next", "purge")
	msg.Header.Set("Nats-Scheduler", cronRestartSubject+".schedule")
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		t.Fatalf("pause schedule: %v", err)
	}
	// Let an occurrence fired just before the purge be handled.
	time.Sleep(1500 * time.Millisecond)
}

func newRestartCronModule(threshold int64) *cronTestModule {
	return newCronTestModule("reports", "rollup", mono.CronServiceConfig{
		Schedule: "@every 1s",
		Payload:  []byte("tick"),
	}, threshold)
}

// cronJetStream returns raw JetStream access through the framework's own NATS
// connection.
func cronJetStream(t *testing.T, m *cronTestModule) jetstream.JetStream {
	t.Helper()
	provider, ok := m.eventBus.(interface{ Conn() *nats.Conn })
	if !ok {
		t.Fatal("event bus does not expose its NATS connection")
	}
	js, err := jetstream.New(provider.Conn())
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}

// handledBeforeStop returns the occurrences m completed, except the last one.
// The framework acknowledges an occurrence after the handler returns, so the
// acknowledgement of the last one may be lost when the application stops right
// after it; the server then rightly delivers it again.
func handledBeforeStop(t *testing.T, m *cronTestModule) map[string]struct{} {
	t.Helper()
	occurrences := m.handledOccurrences()
	if len(occurrences) < 2 {
		t.Fatalf("expected at least 2 handled occurrences, got %v", occurrences)
	}
	handled := make(map[string]struct{}, len(occurrences))
	for _, occurrence := range occurrences[:len(occurrences)-1] {
		handled[occurrence] = struct{}{}
	}
	return handled
}

// assertNoReplay waits until m has handled a new occurrence, gives a replay
// time to arrive, and fails if m handled any occurrence in handled again.
func assertNoReplay(t *testing.T, m *cronTestModule, handled map[string]struct{}) {
	t.Helper()
	if !m.waitForTicks(10 * time.Second) {
		t.Fatal("the schedule did not keep firing after the restart")
	}
	// A replay is delivered back to back as soon as the consumer starts, so
	// it would have arrived by now; wait a little longer to be sure.
	time.Sleep(1500 * time.Millisecond)
	for _, occurrence := range m.handledOccurrences() {
		if _, ok := handled[occurrence]; ok {
			t.Errorf("occurrence %s was handled before the restart and was delivered again (all after restart: %v)",
				occurrence, m.handledOccurrences())
		}
	}
}

// publishOccurrence stores a message on the cron target subject, as the
// scheduler does on each occurrence, identified by its payload. (The server
// rejects a Nats-Schedule-Next header published by a client.)
func publishOccurrence(ctx context.Context, t *testing.T, js jetstream.JetStream, id string) {
	t.Helper()
	if _, err := js.Publish(ctx, cronRestartSubject, []byte(id)); err != nil {
		t.Fatalf("publish occurrence %s: %v", id, err)
	}
}

func TestCronServiceRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// The reported scenario: a restart whose shutdown outlasts the inactivity
	// threshold.
	t.Run("slow shutdown does not replay handled occurrences", func(t *testing.T) {
		storeDir := t.TempDir()
		ctx := context.Background()

		first := newRestartCronModule(3)
		stop := runNamingApp(t, storeDir, nil, &slowStopModule{delay: cronSlowStop}, first)
		if !first.waitForTicks(10 * time.Second) {
			t.Fatal("first boot: expected 3 occurrences")
		}
		pauseCronSchedule(ctx, t, cronJetStream(t, first))
		stop()
		handled := handledBeforeStop(t, first)

		logs := &syncBuffer{}
		second := newRestartCronModule(1)
		runNamingApp(t, storeDir, logs, second)
		assertNoReplay(t, second, handled)
		assertDurable(ctx, t, cronJetStream(t, second), cronRestartStream, cronRestartConsumer)
		assertNotRecreated(t, logs)
	})

	// An application upgrading from v0.0.12 finds the consumer that version
	// created without Durable. It must become a durable in place, keeping its
	// position, and survive a later slow shutdown.
	t.Run("non-durable consumer of an earlier version is converted in place", func(t *testing.T) {
		storeDir := t.TempDir()
		ctx := context.Background()

		// First boot: a deprecated registration provisions the stream without
		// a schedule or a fetch loop, so the test plays the earlier version.
		first := newCronTestModule("reports", "rollup", mono.CronServiceConfig{
			Schedule:   "@every 1s",
			Payload:    []byte("tick"),
			Deprecated: true,
		}, 1)
		stop := runNamingApp(t, storeDir, nil, first)
		js := cronJetStream(t, first)
		handled := map[string]struct{}{}
		for _, id := range []string{"legacy-1", "legacy-2", "legacy-3"} {
			publishOccurrence(ctx, t, js, id)
			handled[id] = struct{}{}
		}
		// The consumer as v0.0.12 created it: a Name but no Durable. Its
		// inactivity timer runs from the Fetch below, so this boot must stop
		// quickly (no slow module) for the consumer to survive until the
		// upgrade, as it does after a fast shutdown.
		legacy, err := js.CreateOrUpdateConsumer(ctx, cronRestartStream, jetstream.ConsumerConfig{
			Name:          cronRestartConsumer,
			FilterSubject: cronRestartSubject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    3,
		})
		if err != nil {
			t.Fatalf("create legacy consumer: %v", err)
		}
		if legacy.CachedInfo().Config.Durable != "" {
			t.Fatal("fixture: legacy consumer must not be durable")
		}
		created := legacy.CachedInfo().Created
		batch, err := legacy.Fetch(len(handled), jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		acked := 0
		for msg := range batch.Messages() {
			if err := msg.DoubleAck(ctx); err != nil {
				t.Fatalf("ack: %v", err)
			}
			acked++
		}
		if acked != len(handled) {
			t.Fatalf("fixture: acked %d occurrences, want %d", acked, len(handled))
		}
		// An occurrence that fired while the application was down.
		const missed = "legacy-4"
		publishOccurrence(ctx, t, js, missed)
		stop()

		// Second boot: the legacy consumer is still there and is converted.
		logs := &syncBuffer{}
		second := newRestartCronModule(2)
		stop = runNamingApp(t, storeDir, logs, &slowStopModule{delay: cronSlowStop}, second)
		assertNoReplay(t, second, handled)
		if occurrences := second.handledOccurrences(); occurrences[0] != missed {
			t.Errorf("first occurrence after the upgrade = %s, want the missed %s", occurrences[0], missed)
		}
		js = cronJetStream(t, second)
		assertDurable(ctx, t, js, cronRestartStream, cronRestartConsumer)
		consumer, err := js.Consumer(ctx, cronRestartStream, cronRestartConsumer)
		if err != nil {
			t.Fatalf("consumer: %v", err)
		}
		info := consumer.CachedInfo()
		if !info.Created.Equal(created) || info.Config.DeliverPolicy != jetstream.DeliverAllPolicy {
			t.Errorf("consumer was recreated: created %v (legacy %v), deliver policy %v", info.Created, created, info.Config.DeliverPolicy)
		}
		if !bytes.Contains([]byte(logs.String()), []byte("Cron consumer converted to durable")) {
			t.Errorf("expected a conversion log line, got:\n%s", logs.String())
		}
		assertNotRecreated(t, logs)
		pauseCronSchedule(ctx, t, js)
		stop()
		for occurrence := range handledBeforeStop(t, second) {
			handled[occurrence] = struct{}{}
		}

		// The second boot stopped slowly: the converted consumer must have
		// been kept, so the third boot does not create one. (Created is no
		// reliable signal here: the server resets it on recovery once the
		// configuration has been updated.)
		logs = &syncBuffer{}
		third := newRestartCronModule(1)
		runNamingApp(t, storeDir, logs, third)
		assertNoReplay(t, third, handled)
		assertNotRecreated(t, logs)
	})

	// A consumer that is missing on startup (deleted by an operator, or the
	// non-durable consumer of an earlier version already timed out) is
	// created without delivering the occurrences stored before it, and their
	// number is logged. Rolling back to v0.0.12 must still start.
	t.Run("missing consumer skips stored occurrences and logs their number", func(t *testing.T) {
		storeDir := t.TempDir()
		ctx := context.Background()

		first := newRestartCronModule(3)
		stop := runNamingApp(t, storeDir, nil, first)
		if !first.waitForTicks(10 * time.Second) {
			t.Fatal("first boot: expected 3 occurrences")
		}
		js := cronJetStream(t, first)
		if err := js.DeleteConsumer(ctx, cronRestartStream, cronRestartConsumer); err != nil {
			t.Fatalf("delete consumer: %v", err)
		}
		stop()
		handled := handledBeforeStop(t, first)

		logs := &syncBuffer{}
		second := newRestartCronModule(1)
		runNamingApp(t, storeDir, logs, second)
		assertNoReplay(t, second, handled)
		js = cronJetStream(t, second)
		assertDurable(ctx, t, js, cronRestartStream, cronRestartConsumer)
		consumer, err := js.Consumer(ctx, cronRestartStream, cronRestartConsumer)
		if err != nil {
			t.Fatalf("consumer: %v", err)
		}
		if _, ok := consumer.CachedInfo().Config.Metadata[cronStartSequenceKey]; ok {
			t.Error("the start sequence was not removed after the consumer was positioned")
		}

		// v0.0.12 sends this configuration on startup. Deliver policy cannot
		// be changed on an existing consumer, so the server would reject it
		// unless the created consumer kept deliver policy all.
		if _, err := js.CreateOrUpdateConsumer(ctx, cronRestartStream, jetstream.ConsumerConfig{
			Name:          cronRestartConsumer,
			FilterSubject: cronRestartSubject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    3,
		}); err != nil {
			t.Errorf("an earlier version could not update the created consumer: %v", err)
		}

		// Every occurrence handled in the first boot, and any stored after the
		// consumer was deleted, was skipped.
		match := skippedPattern.FindStringSubmatch(logs.String())
		if match == nil {
			t.Fatalf("expected a skipped-occurrences warning, got:\n%s", logs.String())
		}
		skipped, err := strconv.Atoi(match[1])
		if err != nil {
			t.Fatalf("parse skipped count %q: %v", match[1], err)
		}
		if want := len(handled) + 1; skipped < want {
			t.Errorf("skipped = %d, want at least %d", skipped, want)
		}
	})
}
