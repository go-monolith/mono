package container

import (
	"context"
	"errors"
	"testing"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

func newContractContainer(t *testing.T, module string) *serviceContainer {
	t.Helper()
	c := NewServiceContainer(&mockLogger{}).(*serviceContainer)
	if err := c.BindModule(&mockModule{name: module}); err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}
	c.SetEventBus(&mockEventBus{})
	return c
}

func TestRegisterContract(t *testing.T) {
	handler := func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }
	def := types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}

	t.Run("records the contract on the entry", func(t *testing.T) {
		c := newContractContainer(t, "inventory")
		if err := c.RegisterRequestReplyService("check-stock", handler); err != nil {
			t.Fatalf("register failed: %v", err)
		}
		if err := c.RegisterContract(def); err != nil {
			t.Fatalf("RegisterContract failed: %v", err)
		}
		entries := c.Entries()
		if len(entries) != 1 || entries[0].Contract == nil || *entries[0].Contract != def {
			t.Fatalf("contract not recorded: %+v", entries)
		}
		// idempotent
		if err := c.RegisterContract(def); err != nil {
			t.Fatalf("second RegisterContract failed: %v", err)
		}
	})

	t.Run("unknown service", func(t *testing.T) {
		c := newContractContainer(t, "inventory")
		err := c.RegisterContract(def)
		if !errors.Is(err, monoerrors.ErrServiceNotFound) {
			t.Fatalf("expected ErrServiceNotFound, got %v", err)
		}
		var se *monoerrors.ServiceError
		if !errors.As(err, &se) || se.ServiceType != types.ServiceTypeRequestReply {
			t.Fatalf("not-found error should carry the contract's type, got %v", err)
		}
	})

	t.Run("different module", func(t *testing.T) {
		c := newContractContainer(t, "payment")
		if err := c.RegisterRequestReplyService("check-stock", handler); err != nil {
			t.Fatalf("register failed: %v", err)
		}
		err := c.RegisterContract(def)
		if !errors.Is(err, monoerrors.ErrServiceContractMismatch) {
			t.Fatalf("expected ErrServiceContractMismatch, got %v", err)
		}
		if c.Entries()[0].Contract != nil {
			t.Error("mismatching contract must not be recorded")
		}
	})

	t.Run("different type", func(t *testing.T) {
		c := newContractContainer(t, "inventory")
		qg := func(context.Context, *types.Msg) error { return nil }
		if err := c.RegisterQueueGroupService("check-stock", types.QGHP{QueueGroup: "w", Handler: qg}); err != nil {
			t.Fatalf("register failed: %v", err)
		}
		err := c.RegisterContract(def)
		if !errors.Is(err, monoerrors.ErrServiceContractMismatch) {
			t.Fatalf("expected ErrServiceContractMismatch, got %v", err)
		}
	})

	t.Run("unbound container", func(t *testing.T) {
		c := NewServiceContainer(&mockLogger{}).(*serviceContainer)
		if err := c.RegisterContract(def); !errors.Is(err, monoerrors.ErrContainerNotBound) {
			t.Fatalf("expected ErrContainerNotBound, got %v", err)
		}
	})
}

func TestComputeServiceSubjectUsesTypesServiceSubject(t *testing.T) {
	c := newContractContainer(t, "inventory")
	subject, err := c.computeServiceSubject("check-stock")
	if err != nil {
		t.Fatalf("computeServiceSubject failed: %v", err)
	}
	if subject != types.ServiceSubject("inventory", "check-stock") {
		t.Errorf("subject = %q", subject)
	}
}
