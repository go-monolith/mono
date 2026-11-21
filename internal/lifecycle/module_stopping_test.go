package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/pkg/types"
)

type lifecycleEventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *lifecycleEventRecorder) record(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *lifecycleEventRecorder) getEvents() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make([]string, len(r.events))
	copy(copied, r.events)
	return copied
}

type orderTrackingMiddleware struct {
	mockMiddlewareModule
	recorder *lifecycleEventRecorder
}

func (m *orderTrackingMiddleware) OnModuleLifecycle(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
	m.recorder.record("middleware:" + string(event.Type) + ":" + event.ModuleName)
	return event
}

type orderTrackingModule struct {
	mockModule
	recorder *lifecycleEventRecorder
}

func (m *orderTrackingModule) Stop(ctx context.Context) error {
	m.recorder.record("module_stop:" + m.name)
	return m.mockModule.Stop(ctx)
}

// TestLifecycle_ModuleStoppingEvent_NormalStop verifies that ModuleStoppingEvent is
// emitted by the lifecycle manager before the module's Stop() executes on normal shutdown.
func TestLifecycle_ModuleStoppingEvent_NormalStop(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	lm := NewLifecycleManager(reg, pluginReg, eventBus, registry.NewEventRegistry(logger), logger, 0)

	recorder := &lifecycleEventRecorder{}

	mw := &orderTrackingMiddleware{
		mockMiddlewareModule: mockMiddlewareModule{mockModule: mockModule{name: "tracker-mw"}},
		recorder:             recorder,
	}
	mod := &orderTrackingModule{
		mockModule: mockModule{name: "test-module"},
		recorder:   recorder,
	}

	if err := reg.Register(mw); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(mod); err != nil {
		t.Fatal(err)
	}

	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if err := lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	events := recorder.getEvents()

	stopEventIdx := -1
	moduleStopIdx := -1
	stoppedEventIdx := -1

	for i, ev := range events {
		switch ev {
		case "middleware:" + string(types.ModuleStoppingEvent) + ":test-module":
			stopEventIdx = i
		case "module_stop:test-module":
			moduleStopIdx = i
		case "middleware:" + string(types.ModuleStoppedEvent) + ":test-module":
			stoppedEventIdx = i
		}
	}

	if stopEventIdx == -1 {
		t.Fatalf("expected ModuleStoppingEvent for test-module in events: %v", events)
	}
	if moduleStopIdx == -1 {
		t.Fatalf("expected module_stop for test-module in events: %v", events)
	}
	if stoppedEventIdx == -1 {
		t.Fatalf("expected ModuleStoppedEvent for test-module in events: %v", events)
	}

	if stopEventIdx >= moduleStopIdx || moduleStopIdx >= stoppedEventIdx {
		t.Fatalf("expected order ModuleStoppingEvent (%d) < module.Stop (%d) < ModuleStoppedEvent (%d), events: %v",
			stopEventIdx, moduleStopIdx, stoppedEventIdx, events)
	}
}

// TestLifecycle_ModuleStoppingEvent_Rollback verifies that ModuleStoppingEvent is
// emitted by the lifecycle manager before module.Stop() executes during rollback.
func TestLifecycle_ModuleStoppingEvent_Rollback(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	lm := NewLifecycleManager(reg, pluginReg, eventBus, registry.NewEventRegistry(logger), logger, 0)

	recorder := &lifecycleEventRecorder{}

	mw := &orderTrackingMiddleware{
		mockMiddlewareModule: mockMiddlewareModule{mockModule: mockModule{name: "tracker-mw"}},
		recorder:             recorder,
	}
	modA := &orderTrackingModule{
		mockModule: mockModule{name: "mod-a"},
		recorder:   recorder,
	}
	modB := &mockModule{
		name:         "mod-b",
		dependencies: []string{"mod-a"},
		startErr:     errors.New("start failed on mod-b"),
	}

	if err := reg.Register(mw); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(modA); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(modB); err != nil {
		t.Fatal(err)
	}

	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("expected Start to fail due to mod-b startErr")
	}

	events := recorder.getEvents()

	stopEventIdx := -1
	moduleStopIdx := -1
	stoppedEventIdx := -1

	for i, ev := range events {
		switch ev {
		case "middleware:" + string(types.ModuleStoppingEvent) + ":mod-a":
			stopEventIdx = i
		case "module_stop:mod-a":
			moduleStopIdx = i
		case "middleware:" + string(types.ModuleStoppedEvent) + ":mod-a":
			stoppedEventIdx = i
		}
	}

	if stopEventIdx == -1 {
		t.Fatalf("expected ModuleStoppingEvent for mod-a in rollback events: %v", events)
	}
	if moduleStopIdx == -1 {
		t.Fatalf("expected module_stop for mod-a in rollback events: %v", events)
	}
	if stoppedEventIdx == -1 {
		t.Fatalf("expected ModuleStoppedEvent for mod-a in rollback events: %v", events)
	}

	if stopEventIdx >= moduleStopIdx || moduleStopIdx >= stoppedEventIdx {
		t.Fatalf("expected rollback order ModuleStoppingEvent (%d) < mod-a.Stop (%d) < ModuleStoppedEvent (%d), events: %v",
			stopEventIdx, moduleStopIdx, stoppedEventIdx, events)
	}
}
