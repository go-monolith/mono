package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-monolith/mono/internal/registry"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// consumerModule is a dependent module that declares service contracts.
type consumerModule struct {
	mockModule
	contracts []types.BaseServiceDefinition
}

func (m *consumerModule) ConsumeServices() []types.BaseServiceDefinition { return m.contracts }

// pluginWithService is a plugin that registers a request-reply service.
type pluginWithService struct {
	mockEventBusAwarePluginModule
}

func (p *pluginWithService) RegisterServices(container types.ServiceContainer) error {
	return container.RegisterRequestReplyService("lookup", func(context.Context, *types.Msg) ([]byte, error) {
		return []byte("ok"), nil
	})
}

func newProvider(name string) *mockModuleWithServices {
	return &mockModuleWithServices{
		mockModule: mockModule{name: name},
		serviceEntries: []*types.ServiceEntry{
			{Name: "check-stock", Type: types.ServiceTypeRequestReply,
				RequestHandler: func(context.Context, *types.Msg) ([]byte, error) { return []byte("{}"), nil }},
			{Name: "on-event", Type: types.ServiceTypeQueueGroup,
				QueueHandlers: []types.QGHP{{QueueGroup: "w", Handler: func(context.Context, *types.Msg) error { return nil }}}},
		},
	}
}

func startWithConsumer(t *testing.T, consumer *consumerModule, plugins ...types.PluginModule) error {
	t.Helper()
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	for i, p := range plugins {
		if err := pluginReg.Register(p, "plugin-"+string(rune('a'+i))); err != nil {
			t.Fatalf("register plugin: %v", err)
		}
	}
	if err := reg.Register(newProvider("inventory")); err != nil {
		t.Fatalf("register provider: %v", err)
	}
	if err := reg.Register(consumer); err != nil {
		t.Fatalf("register consumer: %v", err)
	}
	lm := NewLifecycleManager(reg, pluginReg, newMockEventBus(), nil, logger, 0)
	err := lm.Start(context.Background())
	if err == nil {
		t.Cleanup(func() { _ = lm.Stop(context.Background()) })
	}
	return err
}

func TestServiceContractValidation(t *testing.T) {
	rr := types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}
	qg := types.BaseServiceDefinition{ModuleName: "inventory", Name: "on-event", Type: types.ServiceTypeQueueGroup}

	t.Run("valid contracts start", func(t *testing.T) {
		consumer := &consumerModule{
			mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
			contracts:  []types.BaseServiceDefinition{rr, qg},
		}
		if err := startWithConsumer(t, consumer); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		if !consumer.startCalled {
			t.Error("consumer was not started")
		}
	})

	t.Run("no contracts is fine", func(t *testing.T) {
		consumer := &consumerModule{mockModule: mockModule{name: "order", dependencies: []string{"inventory"}}}
		if err := startWithConsumer(t, consumer); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
	})

	t.Run("undeclared dependency fails fast", func(t *testing.T) {
		consumer := &consumerModule{
			mockModule: mockModule{name: "order"}, // no Dependencies()
			contracts:  []types.BaseServiceDefinition{rr},
		}
		err := startWithConsumer(t, consumer)
		if !errors.Is(err, monoerrors.ErrModuleStartFailed) || !errors.Is(err, monoerrors.ErrMissingDependency) {
			t.Fatalf("expected ErrModuleStartFailed wrapping ErrMissingDependency, got %v", err)
		}
		if !strings.Contains(err.Error(), "Dependencies()") {
			t.Errorf("error should tell the user to declare the dependency: %v", err)
		}
		if consumer.startCalled {
			t.Error("consumer must not start")
		}
	})

	t.Run("missing service fails fast", func(t *testing.T) {
		missing := rr
		missing.Name = "reserve-stock"
		consumer := &consumerModule{
			mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
			contracts:  []types.BaseServiceDefinition{missing},
		}
		err := startWithConsumer(t, consumer)
		if !errors.Is(err, monoerrors.ErrServiceNotFound) {
			t.Fatalf("expected ErrServiceNotFound, got %v", err)
		}
		if strings.Contains(err.Error(), "(channel)") {
			t.Fatalf("not-found error must carry the contract's type, got %v", err)
		}
	})

	t.Run("type mismatch fails fast", func(t *testing.T) {
		wrongType := rr
		wrongType.Type = types.ServiceTypeQueueGroup
		consumer := &consumerModule{
			mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
			contracts:  []types.BaseServiceDefinition{wrongType},
		}
		err := startWithConsumer(t, consumer)
		if !errors.Is(err, monoerrors.ErrServiceContractMismatch) {
			t.Fatalf("expected ErrServiceContractMismatch, got %v", err)
		}
	})

	t.Run("self reference is rejected", func(t *testing.T) {
		self := types.BaseServiceDefinition{ModuleName: "order", Name: "x", Type: types.ServiceTypeRequestReply}
		consumer := &consumerModule{
			mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
			contracts:  []types.BaseServiceDefinition{self},
		}
		if err := startWithConsumer(t, consumer); err == nil || !strings.Contains(err.Error(), "names the module itself") {
			t.Fatalf("expected self-reference error, got %v", err)
		}
	})

	t.Run("incomplete contract is rejected", func(t *testing.T) {
		consumer := &consumerModule{
			mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
			contracts:  []types.BaseServiceDefinition{{ModuleName: "inventory"}},
		}
		if err := startWithConsumer(t, consumer); err == nil || !strings.Contains(err.Error(), "incomplete") {
			t.Fatalf("expected incomplete-contract error, got %v", err)
		}
	})

	t.Run("plugin services need no dependency declaration", func(t *testing.T) {
		plugin := &pluginWithService{mockEventBusAwarePluginModule{name: "kv-store"}}
		consumer := &consumerModule{
			mockModule: mockModule{name: "order"},
			contracts: []types.BaseServiceDefinition{
				{ModuleName: "kv-store", Name: "lookup", Type: types.ServiceTypeRequestReply},
			},
		}
		if err := startWithConsumer(t, consumer, plugin); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
	})

	t.Run("plugin service type mismatch fails", func(t *testing.T) {
		plugin := &pluginWithService{mockEventBusAwarePluginModule{name: "kv-store"}}
		consumer := &consumerModule{
			mockModule: mockModule{name: "order"},
			contracts: []types.BaseServiceDefinition{
				{ModuleName: "kv-store", Name: "lookup", Type: types.ServiceTypeStreamConsumer},
			},
		}
		if err := startWithConsumer(t, consumer, plugin); !errors.Is(err, monoerrors.ErrServiceContractMismatch) {
			t.Fatalf("expected ErrServiceContractMismatch, got %v", err)
		}
	})
}
