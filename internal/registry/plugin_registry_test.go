package registry

import (
	"context"
	"testing"

	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/types"
)

// mockPluginModule implements types.PluginModule for testing.
type mockPluginModule struct {
	name      string
	container types.ServiceContainer
}

func (m *mockPluginModule) Name() string {
	return m.name
}

func (m *mockPluginModule) Start(ctx context.Context) error {
	return nil
}

func (m *mockPluginModule) Stop(ctx context.Context) error {
	return nil
}

func (m *mockPluginModule) SetContainer(container types.ServiceContainer) {
	m.container = container
}

func (m *mockPluginModule) Container() types.ServiceContainer {
	return m.container
}

// Test PluginRegistry basic operations

func TestNewPluginRegistry(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})
	if registry == nil {
		t.Fatal("NewPluginRegistry returned nil")
	}

	aliases := registry.List()
	if len(aliases) != 0 {
		t.Errorf("expected empty registry, got %d plugins", len(aliases))
	}
}

func TestNewPluginRegistryNilLogger(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil logger")
		}
	}()

	_ = NewPluginRegistry(nil)
}

func TestRegisterPlugin(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	plugin := &mockPluginModule{name: "test-plugin"}
	err := registry.Register(plugin, "test-alias")
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	aliases := registry.List()
	if len(aliases) != 1 {
		t.Fatalf("expected 1 plugin, got %d", len(aliases))
	}

	if aliases[0] != "test-alias" {
		t.Errorf("expected 'test-alias', got '%s'", aliases[0])
	}
}

func TestRegisterNilPlugin(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	err := registry.Register(nil, "test-alias")
	if err == nil {
		t.Fatal("expected error for nil plugin")
	}

	if !monoerrors.IsModuleError(err) {
		t.Errorf("expected ModuleError, got %T", err)
	}
}

func TestRegisterPluginEmptyAlias(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	plugin := &mockPluginModule{name: "test-plugin"}
	err := registry.Register(plugin, "")
	if err == nil {
		t.Fatal("expected error for empty alias")
	}

	if !monoerrors.IsModuleError(err) {
		t.Errorf("expected ModuleError, got %T", err)
	}
}

func TestRegisterDuplicatePluginAlias(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	plugin1 := &mockPluginModule{name: "plugin-1"}
	plugin2 := &mockPluginModule{name: "plugin-2"}

	err := registry.Register(plugin1, "duplicate-alias")
	if err != nil {
		t.Fatalf("first Register failed: %v", err)
	}

	// Try to register another plugin with same alias
	err = registry.Register(plugin2, "duplicate-alias")
	if err == nil {
		t.Fatal("expected error for duplicate alias")
	}

	if !monoerrors.IsModuleError(err) {
		t.Errorf("expected ModuleError, got %T", err)
	}
}

func TestRegisterSamePluginDifferentAliases(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	// Same plugin instance can be registered under different aliases
	plugin := &mockPluginModule{name: "shared-plugin"}

	err := registry.Register(plugin, "alias-1")
	if err != nil {
		t.Fatalf("first Register failed: %v", err)
	}

	err = registry.Register(plugin, "alias-2")
	if err != nil {
		t.Fatalf("second Register failed: %v", err)
	}

	aliases := registry.List()
	if len(aliases) != 2 {
		t.Fatalf("expected 2 aliases, got %d", len(aliases))
	}
}

func TestGetPlugin(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	plugin := &mockPluginModule{name: "test-plugin"}
	_ = registry.Register(plugin, "my-alias")

	retrieved, err := registry.Get("my-alias")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Name() != "test-plugin" {
		t.Errorf("expected 'test-plugin', got '%s'", retrieved.Name())
	}
}

func TestGetPluginEmptyAlias(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	_, err := registry.Get("")
	if err == nil {
		t.Fatal("expected error for empty alias")
	}

	if !monoerrors.IsModuleError(err) {
		t.Errorf("expected ModuleError, got %T", err)
	}
}

func TestGetNonExistentPlugin(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	_, err := registry.Get("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent plugin")
	}

	if !monoerrors.IsModuleError(err) {
		t.Errorf("expected ModuleError, got %T", err)
	}
}

func TestPluginListPreservesOrder(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	aliases := []string{"first", "second", "third"}
	for i, alias := range aliases {
		plugin := &mockPluginModule{name: "plugin-" + alias}
		_ = registry.Register(plugin, alias)
		_ = i
	}

	listed := registry.List()
	if len(listed) != len(aliases) {
		t.Fatalf("expected %d plugins, got %d", len(aliases), len(listed))
	}

	for i, alias := range aliases {
		if listed[i] != alias {
			t.Errorf("position %d: expected %s, got %s", i, alias, listed[i])
		}
	}
}

func TestPluginAllReturnsAllPlugins(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	testData := []struct {
		alias  string
		plugin *mockPluginModule
	}{
		{"alias-a", &mockPluginModule{name: "A"}},
		{"alias-b", &mockPluginModule{name: "B"}},
		{"alias-c", &mockPluginModule{name: "C"}},
	}

	for _, td := range testData {
		_ = registry.Register(td.plugin, td.alias)
	}

	all := registry.All()
	if len(all) != len(testData) {
		t.Fatalf("expected %d plugins, got %d", len(testData), len(all))
	}

	for _, td := range testData {
		plugin, exists := all[td.alias]
		if !exists {
			t.Errorf("plugin with alias %s not found in All()", td.alias)
			continue
		}
		if plugin.Name() != td.plugin.name {
			t.Errorf("alias %s: expected module name %s, got %s", td.alias, td.plugin.name, plugin.Name())
		}
	}
}

func TestPluginListReturnsCopy(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	plugin := &mockPluginModule{name: "test"}
	_ = registry.Register(plugin, "test-alias")

	list1 := registry.List()
	list2 := registry.List()

	// Modify list1
	list1[0] = "modified"

	// list2 should not be affected
	if list2[0] != "test-alias" {
		t.Error("List() should return independent copies")
	}
}

func TestPluginAllReturnsCopy(t *testing.T) {
	registry := NewPluginRegistry(&mockLogger{})

	plugin := &mockPluginModule{name: "test"}
	_ = registry.Register(plugin, "test-alias")

	all1 := registry.All()
	all2 := registry.All()

	// Delete from all1
	delete(all1, "test-alias")

	// all2 should not be affected
	if _, exists := all2["test-alias"]; !exists {
		t.Error("All() should return independent copies")
	}
}
