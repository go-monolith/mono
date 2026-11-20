package eventbus

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/internal/logger"
	"github.com/go-monolith/mono/pkg/types"
)

// staleAnswerConsumer is a jetstream.Consumer of which only CachedInfo is
// implemented.
type staleAnswerConsumer struct {
	jetstream.Consumer
	info *jetstream.ConsumerInfo
}

func (c *staleAnswerConsumer) CachedInfo() *jetstream.ConsumerInfo { return c.info }

func TestAppliedDespiteStaleAnswer(t *testing.T) {
	updateErr := jetstream.ErrConsumerMultipleFilterSubjectsNotSupported
	requested := jetstream.ConsumerConfig{Durable: "orders", FilterSubjects: []string{"a", "b"}}
	withFilters := func(filters ...string) *staleAnswerConsumer {
		return &staleAnswerConsumer{info: &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{FilterSubjects: filters}}}
	}

	tests := []struct {
		name      string
		requested jetstream.ConsumerConfig
		found     jetstream.Consumer
		lookupErr error
		wantName  string
		applied   bool
	}{
		{name: "applied", requested: requested, found: withFilters("b", "a"), wantName: "orders", applied: true},
		{name: "a consumer with a name only", requested: jetstream.ConsumerConfig{Name: "legacy", FilterSubjects: []string{"a", "b"}},
			found: withFilters("a", "b"), wantName: "legacy", applied: true},
		{name: "not applied", requested: requested, found: withFilters(), wantName: "orders"},
		{name: "other filters", requested: requested, found: withFilters("a", "c"), wantName: "orders"},
		{name: "no info", requested: requested, found: &staleAnswerConsumer{}, wantName: "orders"},
		{name: "the read-back fails", requested: requested, lookupErr: errors.New("no responders"), wantName: "orders"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var looked string
			consumer, err := AppliedDespiteStaleAnswer(context.Background(), tt.requested, updateErr,
				func(_ context.Context, name string) (jetstream.Consumer, error) {
					looked = name
					return tt.found, tt.lookupErr
				})
			if looked != tt.wantName {
				t.Errorf("looked up %q, want %q", looked, tt.wantName)
			}
			if tt.applied {
				if err != nil || consumer != tt.found {
					t.Fatalf("got (%v, %v), want the consumer read back", consumer, err)
				}
				return
			}
			if !errors.Is(err, updateErr) || consumer != nil {
				t.Fatalf("got (%v, %v), want the update error", consumer, err)
			}
			if tt.lookupErr != nil && !errors.Is(err, tt.lookupErr) {
				t.Errorf("error %v does not carry the read-back error", err)
			}
		})
	}
}

// runStoredJetStream starts a server on the store directory and returns a
// wrapper connected to it and a function that stops both.
func runStoredJetStream(t *testing.T, dir string) (*NatsJetStream, func()) {
	t.Helper()
	opts := test.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = dir
	srv := test.RunServer(&opts)
	conn, err := nats.Connect(srv.ClientURL(), nats.Timeout(2*time.Second))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("connect: %v", err)
	}
	js, err := NewJetStream(conn, logger.NewDefaultLogger())
	if err != nil {
		conn.Close()
		srv.Shutdown()
		t.Fatalf("NewJetStream: %v", err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		conn.Close()
		srv.Shutdown()
		srv.WaitForShutdown()
	}
	t.Cleanup(stop)
	return js, stop
}

// Adding several FilterSubjects to a consumer the server recovered from its
// store succeeds on the first attempt, through CreateOrUpdateConsumer and
// through UpdateConsumer, although the server answers that first request
// with the configuration from before it.
func TestJetStream_SeveralFiltersOnARecoveredConsumer(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	filters := []string{"orders.eu.>", "orders.us.>"}
	names := []string{"upserted", "updated", "raw"}

	js, stop := runStoredJetStream(t, dir)
	if _, err := js.CreateOrUpdateStream(ctx, types.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}}); err != nil {
		t.Fatalf("create the stream: %v", err)
	}
	for _, name := range names {
		if _, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", types.ConsumerConfig{Name: name, Durable: name, AckPolicy: types.AckExplicitPolicy}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	stop()

	js, _ = runStoredJetStream(t, dir)

	// The behaviour worked around, shown on a consumer of its own. Should a
	// later server answer with the new configuration, the calls below pass
	// without the workaround.
	_, rawErr := js.js.UpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Name: "raw", Durable: "raw", AckPolicy: jetstream.AckExplicitPolicy, FilterSubjects: filters,
	})
	if !errors.Is(rawErr, jetstream.ErrConsumerMultipleFilterSubjectsNotSupported) {
		t.Logf("the server answered the first update of a recovered consumer with: %v", rawErr)
	}

	cfg := func(name string) types.ConsumerConfig {
		return types.ConsumerConfig{Name: name, Durable: name, AckPolicy: types.AckExplicitPolicy, FilterSubjects: filters}
	}
	upserted, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", cfg("upserted"))
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer: %v", err)
	}
	updated, err := js.UpdateConsumer(ctx, "ORDERS", cfg("updated"))
	if err != nil {
		t.Fatalf("UpdateConsumer: %v", err)
	}
	for name, consumer := range map[string]jetstream.Consumer{"upserted": upserted, "updated": updated} {
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatalf("%s: Info: %v", name, err)
		}
		if got := info.Config.FilterSubjects; !slices.Equal(got, filters) {
			t.Errorf("%s: filters = %v, want %v", name, got, filters)
		}
	}
}
