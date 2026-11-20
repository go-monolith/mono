package registry

import (
	"context"
	"strings"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

// TestEventRegistryCheckpoint: restoring a checkpoint puts back the events,
// consumers, stream consumers and the stream consumer sequence, so that what
// a failed Start registered can be registered again, with the same IDs.
func TestEventRegistryCheckpoint(t *testing.T) {
	r, ok := NewEventRegistry(&mockLogger{}).(*eventRegistry)
	if !ok {
		t.Fatal("NewEventRegistry does not return the framework's registry")
	}
	module := &mockModule{name: "orders"}
	before := types.BaseEventDefinition{ModuleName: "billing", Name: "Charged", Version: "v1", Subject: "events.billing.charged"}
	during := types.BaseEventDefinition{ModuleName: "catalog", Name: "ProductAdded", Version: "v1", Subject: "events.catalog.product-added"}
	handler := func(context.Context, *types.Msg) error { return nil }
	batch := func(context.Context, []*types.Msg) error { return nil }
	stream := func(def types.BaseEventDefinition) types.StreamConsumerConfig {
		return types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "EVENTS_" + def.Name}, Fetch: types.FetchConfig{BatchSize: 1}}
	}

	// Registered before the checkpoint: kept by a restore.
	if err := r.RegisterEvent(before); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterEventConsumer(before, handler, module); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterEventStreamConsumer(before, stream(before), batch, module); err != nil {
		t.Fatal(err)
	}
	firstID := r.StreamConsumerEntries()[0].SequenceID

	register := func() int {
		t.Helper()
		if err := r.RegisterEvent(during); err != nil {
			t.Fatalf("RegisterEvent: %v", err)
		}
		if err := r.RegisterEventConsumer(during, handler, module); err != nil {
			t.Fatalf("RegisterEventConsumer: %v", err)
		}
		if err := r.RegisterEventStreamConsumer(during, stream(during), batch, module); err != nil {
			t.Fatalf("RegisterEventStreamConsumer: %v", err)
		}
		entries := r.StreamConsumerEntries()
		return entries[len(entries)-1].SequenceID
	}

	restore := r.Checkpoint()
	id := register()
	if err := r.RegisterEvent(during); err == nil {
		t.Fatal("control: the event can be registered twice without a restore")
	}
	restore()

	if got := r.GetAllEvents(); len(got) != 1 || got[0].Name != "Charged" {
		t.Fatalf("events after the restore = %v", got)
	}
	if _, found := r.GetEventByName("ProductAdded", "v1", "catalog"); found {
		t.Fatal("the event registered after the checkpoint is still found")
	}
	if _, found := r.GetEventByName("Charged", "v1", "billing"); !found {
		t.Fatal("the event registered before the checkpoint is gone")
	}
	if got := len(r.Entries()); got != 1 {
		t.Fatalf("%d consumers after the restore, want 1", got)
	}
	if got := r.StreamConsumerEntries(); len(got) != 1 || got[0].SequenceID != firstID {
		t.Fatalf("stream consumers after the restore = %+v", got)
	}

	// The retry registers the same things and gets the same sequence ID.
	if again := register(); again != id {
		t.Fatalf("sequence ID of the retried registration = %d, want %d", again, id)
	}
	if got := len(r.Entries()); got != 2 {
		t.Fatalf("%d consumers after the retry, want 2", got)
	}
}

// TestRegisterEventStreamConsumer_NegativeFetch: a negative batch size used
// to be registered and crash the process in the fetch loop.
func TestRegisterEventStreamConsumer_NegativeFetch(t *testing.T) {
	r := NewEventRegistry(&mockLogger{})
	def := types.BaseEventDefinition{ModuleName: "catalog", Name: "ProductAdded", Version: "v1", Subject: "events.catalog.product-added"}
	batch := func(context.Context, []*types.Msg) error { return nil }
	module := &mockModule{name: "orders"}
	for name, fetch := range map[string]types.FetchConfig{
		"batch size": {BatchSize: -1},
		"timeout":    {Timeout: -1},
	} {
		err := r.RegisterEventStreamConsumer(def, types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "EVENTS"}, Fetch: fetch}, batch, module)
		if err == nil || !strings.Contains(err.Error(), "must not be negative") || !strings.Contains(err.Error(), "catalog.ProductAdded.v1") {
			t.Errorf("negative %s: err = %v, want a rejection naming the event", name, err)
		}
	}
	if got := len(r.StreamConsumerEntries()); got != 0 {
		t.Fatalf("%d consumers registered", got)
	}
	// Zero still selects the defaults.
	if err := r.RegisterEventStreamConsumer(def, types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "EVENTS"}}, batch, module); err != nil {
		t.Fatal(err)
	}
	if got := r.StreamConsumerEntries()[0].Config.Fetch; got.BatchSize != 10 || got.Timeout <= 0 {
		t.Fatalf("defaults = %+v", got)
	}
}
