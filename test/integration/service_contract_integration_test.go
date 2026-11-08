//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/middleware/accesslog"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// Contracts of the test provider, as a real application would declare them in
// the provider's contract package.
type stockRequest struct {
	ProductID string `json:"product_id"`
}

type stockResponse struct {
	Stock int `json:"stock"`
}

var (
	checkStockContract = helper.RequestReplyService[stockRequest, stockResponse]("stock", "check")
	restockContract    = helper.QueueGroupService[stockRequest]("stock", "restock")
)

// contractProvider registers its services through the contracts.
type contractProvider struct {
	testModule
	restocked chan string
}

func (p *contractProvider) RegisterServices(c mono.ServiceContainer) error {
	if err := checkStockContract.Register(c, func(_ context.Context, req stockRequest, _ *mono.Msg) (stockResponse, error) {
		if req.ProductID == "none" {
			return stockResponse{}, errors.New("unknown product")
		}
		return stockResponse{Stock: len(req.ProductID)}, nil
	}); err != nil {
		return err
	}
	return restockContract.Register(c, types.TypedQGHP[stockRequest]{
		QueueGroup: "restockers",
		Handler: func(_ context.Context, req stockRequest, _ *mono.Msg) error {
			p.restocked <- req.ProductID
			return nil
		},
	})
}

// contractConsumer declares what it consumes and calls through the contracts.
type contractConsumer struct {
	testModule
	contracts []types.BaseServiceDefinition
	stock     mono.ServiceContainer
}

func (c *contractConsumer) Dependencies() []string { return []string{"stock"} }
func (c *contractConsumer) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
	if dep == "stock" {
		c.stock = container
	}
}
func (c *contractConsumer) ConsumeServices() []types.BaseServiceDefinition { return c.contracts }

func TestServiceContract_CallAndSendThroughContracts(t *testing.T) {
	fw := newWorkerPoolApp(t)
	provider := &contractProvider{testModule: testModule{name: "stock"}, restocked: make(chan string, 1)}
	consumer := &contractConsumer{
		testModule: testModule{name: "shop"},
		contracts:  []types.BaseServiceDefinition{checkStockContract.ToBase(), restockContract.ToBase()},
	}
	for _, m := range []mono.Module{provider, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("register %s: %v", m.Name(), err)
		}
	}
	startWorkerPoolApp(t, fw)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := checkStockContract.Call(ctx, consumer.stock, stockRequest{ProductID: "laptop"})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Stock != len("laptop") {
		t.Fatalf("Stock = %d", resp.Stock)
	}

	// A handler error travels back wrapped with the contract identity.
	_, err = checkStockContract.Call(ctx, consumer.stock, stockRequest{ProductID: "none"})
	if err == nil || !strings.Contains(err.Error(), "stock/check") || !strings.Contains(err.Error(), "unknown product") {
		t.Fatalf("expected a contract-wrapped handler error, got %v", err)
	}

	if err := restockContract.Send(ctx, consumer.stock, stockRequest{ProductID: "mouse"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case got := <-provider.restocked:
		if got != "mouse" {
			t.Fatalf("restocked %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queue group handler not called")
	}

	// The contracts are recorded on the provider's entries.
	recorded := 0
	for _, e := range fw.Services("stock").Entries() {
		if e.Contract != nil {
			recorded++
		}
	}
	if recorded != 2 {
		t.Fatalf("contract-backed entries = %d, want 2", recorded)
	}
}

func TestServiceContract_SurvivesMiddleware(t *testing.T) {
	// Middleware rebuilds the ServiceRegistration; the contract must still be
	// recorded on the entry and the startup validation must still pass.
	fw := newWorkerPoolApp(t)
	access, err := accesslog.New(accesslog.WithOutput(&strings.Builder{}))
	if err != nil {
		t.Fatalf("accesslog: %v", err)
	}
	provider := &contractProvider{testModule: testModule{name: "stock"}, restocked: make(chan string, 1)}
	consumer := &contractConsumer{
		testModule: testModule{name: "shop"},
		contracts:  []types.BaseServiceDefinition{checkStockContract.ToBase()},
	}
	for _, m := range []mono.Module{access, provider, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("register %s: %v", m.Name(), err)
		}
	}
	startWorkerPoolApp(t, fw)

	for _, e := range fw.Services("stock").Entries() {
		if e.Name == "check" && (e.Contract == nil || *e.Contract != checkStockContract.ToBase()) {
			t.Fatalf("contract dropped by middleware: %+v", e.Contract)
		}
	}
}

func TestServiceContract_StartupValidationFailsFast(t *testing.T) {
	cases := []struct {
		name     string
		contract types.BaseServiceDefinition
		wantErr  string
	}{
		{"missing service", types.BaseServiceDefinition{ModuleName: "stock", Name: "reserve", Type: types.ServiceTypeRequestReply}, monoerrors.ErrServiceNotFound.Error()},
		{"wrong type", types.BaseServiceDefinition{ModuleName: "stock", Name: "check", Type: types.ServiceTypeQueueGroup}, monoerrors.ErrServiceContractMismatch.Error()},
		{"undeclared module", types.BaseServiceDefinition{ModuleName: "warehouse", Name: "check", Type: types.ServiceTypeRequestReply}, monoerrors.ErrMissingDependency.Error()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fw := newWorkerPoolApp(t)
			provider := &contractProvider{testModule: testModule{name: "stock"}, restocked: make(chan string, 1)}
			consumer := &contractConsumer{
				testModule: testModule{name: "shop"},
				contracts:  []types.BaseServiceDefinition{tc.contract},
			}
			for _, m := range []mono.Module{provider, consumer} {
				if err := fw.Register(m); err != nil {
					t.Fatalf("register %s: %v", m.Name(), err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := fw.Start(ctx)
			if err == nil {
				_ = fw.Stop(ctx)
				t.Fatal("expected Start to fail")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err, tc.wantErr)
			}
			if consumer.wasStarted() {
				t.Fatal("consumer must not have started")
			}
		})
	}
}
