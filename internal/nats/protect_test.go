package nats

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/jsprotect"
	"github.com/nats-io/nats.go/jetstream"
)

// TestNATSManager_ProtectsConsumersRecoveredAtStart restarts a server over a
// store that holds a Name-only consumer: since nats-server v2.14.10 the server
// deletes such a consumer about 5s after the start unless it is pulled. The
// manager extends its threshold during Start, so it survives.
func TestNATSManager_ProtectsConsumersRecoveredAtStart(t *testing.T) {
	store := t.TempDir()
	first, err := startClusterNode(t, 0, WithJetStream(store))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	js := first.js
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "KEEP", Subjects: []string{"keep.>"}, Retention: jetstream.InterestPolicy}); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []jetstream.ConsumerConfig{
		{Name: "legacy", AckPolicy: jetstream.AckExplicitPolicy},
		{Name: "explicit", AckPolicy: jetstream.AckExplicitPolicy, InactiveThreshold: 2 * time.Second},
		// A conversion an earlier start interrupted after its first update.
		{Name: "halfway", Durable: "halfway", AckPolicy: jetstream.AckExplicitPolicy, InactiveThreshold: jsprotect.DefaultInactiveThreshold},
	} {
		if _, err := js.CreateOrUpdateConsumer(ctx, "KEEP", cfg); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := js.Publish(ctx, "keep.one", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	second, err := startClusterNode(t, 0, WithJetStream(store))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(6500 * time.Millisecond) // longer than the 5s default threshold
	legacy, err := second.js.Consumer(ctx, "KEEP", "legacy")
	if err != nil {
		t.Fatalf("the Name-only consumer was deleted after the restart: %v", err)
	}
	if got := legacy.CachedInfo().Config.InactiveThreshold; got != jsprotect.Threshold {
		t.Errorf("threshold = %s, want %s", got, jsprotect.Threshold)
	}
	if _, err := second.js.Consumer(ctx, "KEEP", "halfway"); err != nil {
		t.Errorf("the half-converted durable was deleted after the restart: %v", err)
	}
	if _, err := second.js.Consumer(ctx, "KEEP", "explicit"); err == nil {
		t.Error("a consumer with an explicit threshold must keep it (and expire)")
	}
	stream, err := second.js.Stream(ctx, "KEEP")
	if err != nil {
		t.Fatal(err)
	}
	if info, err := stream.Info(ctx); err != nil || info.State.Msgs != 1 {
		t.Errorf("interest stream lost its message: %+v, %v", info, err)
	}
	if !second.logger.(*mockLogger).hasMessage("INFO", "Extended the inactivity threshold") {
		t.Error("protection not logged")
	}
}

func TestNATSManager_ServerNameMustMatchConfigFile(t *testing.T) {
	conf := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(conf, []byte("server_name: from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := startClusterNode(t, 0, WithConfigFile(conf), WithServerName("from-option"))
	if err == nil || !strings.Contains(err.Error(), "discards its local replicas") {
		t.Fatalf("error = %v, want a server name mismatch", err)
	}
	if _, err := startClusterNode(t, 0, WithConfigFile(conf), WithServerName("from-file")); err != nil {
		t.Fatalf("the same name must be accepted: %v", err)
	}
}

// TestNATSManager_ConsumerProtectionRetriesAndStops drives the background
// retry with passes that can never complete (the connection is closed): it
// gives up after the budget with a warning, and StopConsumerProtection ends it
// at once and may be called again.
func TestNATSManager_ConsumerProtectionRetriesAndStops(t *testing.T) {
	oldEvery, oldBudget := protectRetryEvery, protectRetryBudget
	protectRetryEvery, protectRetryBudget = 10*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { protectRetryEvery, protectRetryBudget = oldEvery, oldBudget })

	node, err := startClusterNode(t, 0, WithJetStream(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	js := node.js
	node.conn.Close()

	logger := newMockLogger()
	m := &natsManager{logger: logger}
	m.startConsumerProtection(js)
	done := m.protectDone
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("background protection did not end after its budget")
	}
	if !logger.hasMessage("WARN", "Could not check every JetStream consumer") {
		t.Error("giving up not logged")
	}
	m.StopConsumerProtection()
	m.StopConsumerProtection() // idempotent

	// Stopped early: returns promptly.
	protectRetryBudget = time.Hour
	m2 := &natsManager{logger: newMockLogger()}
	m2.startConsumerProtection(js)
	stopped := make(chan struct{})
	go func() {
		m2.StopConsumerProtection()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("StopConsumerProtection did not return promptly")
	}
}
