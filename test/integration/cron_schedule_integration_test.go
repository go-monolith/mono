//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The schedule message of a cron service was published at every Start. The
// server counts an interval from the time of that message, so every start of
// any instance began the interval anew, and an interval longer than the time
// between two starts never fired (issue #87).

const cronScheduleSubject = cronRestartSubject + ".schedule"

// CronScheduleSequence returns the stream sequence of the schedule message of
// the reports.rollup cron service, or 0 when the stream holds none.
func CronScheduleSequence(ctx context.Context, t *testing.T, js jetstream.JetStream) uint64 {
	t.Helper()
	stream, err := js.Stream(ctx, cronRestartStream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	msg, err := stream.GetLastMsgForSubject(ctx, cronScheduleSubject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0
	}
	if err != nil {
		t.Fatalf("schedule message: %v", err)
	}
	return msg.Sequence
}

func TestCronSchedule_PublishedOnlyWhenItChanged(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	storeDir := t.TempDir()
	ctx := context.Background()
	module := func(payload string) *cronTestModule {
		return newCronTestModule("reports", "rollup", mono.CronServiceConfig{
			Schedule: "@every 1h",
			Payload:  []byte(payload),
		}, 1)
	}

	first := module("tick")
	stop := runNamingApp(t, storeDir, nil, first)
	published := CronScheduleSequence(ctx, t, cronJetStream(t, first))
	if published == 0 {
		t.Fatal("the first start published no schedule")
	}
	stop()

	// The same schedule: a second start leaves the stored message alone, so
	// the hour that began with the first start runs on.
	second := module("tick")
	stop = runNamingApp(t, storeDir, nil, second)
	if seq := CronScheduleSequence(ctx, t, cronJetStream(t, second)); seq != published {
		t.Errorf("an unchanged schedule was published again: sequence %d, was %d", seq, published)
	}
	stop()

	// A changed schedule replaces the stored one.
	third := module("tock")
	stop = runNamingApp(t, storeDir, nil, third)
	js := cronJetStream(t, third)
	changed := CronScheduleSequence(ctx, t, js)
	if changed <= published {
		t.Errorf("a changed schedule was not published: sequence %d, was %d", changed, published)
	}
	// A schedule that is gone (purged by an operator, or by an earlier
	// Deprecated registration) is published by the next start.
	purge := nats.NewMsg(cronRestartSubject + ".control")
	purge.Header.Set("Nats-Schedule-Next", "purge")
	purge.Header.Set("Nats-Scheduler", cronScheduleSubject)
	if _, err := js.PublishMsg(ctx, purge); err != nil {
		t.Fatalf("purge schedule: %v", err)
	}
	waitWithin(t, 5*time.Second, func() bool { return CronScheduleSequence(ctx, t, js) == 0 }, "the schedule was not purged")
	stop()

	fourth := module("tock")
	runNamingApp(t, storeDir, nil, fourth)
	if seq := CronScheduleSequence(ctx, t, cronJetStream(t, fourth)); seq <= changed {
		t.Errorf("a purged schedule was not published again: sequence %d, was %d before the purge", seq, changed)
	}
}

// TestCronSchedule_IntervalFiresAcrossRestarts: an application restarted more
// often than the interval of its schedule still handles occurrences. Each
// start used to begin the interval anew, so none ever came.
func TestCronSchedule_IntervalFiresAcrossRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	storeDir := t.TempDir()
	var handled int64
	begin := time.Now()
	for time.Since(begin) < 12*time.Second {
		m := newCronTestModule("reports", "rollup", mono.CronServiceConfig{
			Schedule: "@every 3s",
			Payload:  []byte("tick"),
		}, 1000)
		stop := runNamingApp(t, storeDir, nil, m)
		time.Sleep(2 * time.Second)
		stop()
		handled += atomic.LoadInt64(&m.invocations)
	}
	if handled == 0 {
		t.Fatal("an interval of 3s never fired in 12s of an application restarted every 2s")
	}
	t.Logf("handled %d occurrences", handled)
}

// TestCronSchedule_OneShotFiresOnce: a one-shot schedule whose time has passed
// fires at the first start and not again at later ones (the server removes a
// one-shot schedule when it has fired, and every Start published it again);
// another time fires once more.
func TestCronSchedule_OneShotFiresOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	storeDir := t.TempDir()
	run := func(at time.Time) int64 {
		t.Helper()
		m := newCronTestModule("reports", "rollup", mono.CronServiceConfig{
			Schedule: "@at " + at.UTC().Format(time.RFC3339),
			Payload:  []byte("once"),
		}, 1)
		stop := runNamingApp(t, storeDir, nil, m)
		m.waitForTicks(2 * time.Second)
		// Time for a second delivery, which would be the defect.
		time.Sleep(500 * time.Millisecond)
		stop()
		return atomic.LoadInt64(&m.invocations)
	}
	past := time.Now().Add(-time.Hour)
	if n := run(past); n != 1 {
		t.Fatalf("first start handled %d occurrences of a one-shot schedule in the past, want 1", n)
	}
	for i := range 2 {
		if n := run(past); n != 0 {
			t.Errorf("start %d handled %d occurrences of a one-shot schedule that had fired, want none", i+2, n)
		}
	}
	if n := run(past.Add(time.Minute)); n != 1 {
		t.Errorf("a changed time handled %d occurrences, want 1", n)
	}
}
