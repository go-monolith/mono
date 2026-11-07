// Package inventory implements an inventory management module for stock tracking
// and availability checking. Its port is the contract sub-package; this package
// holds the domain logic and the driving adapter (service registration).
package inventory

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/examples/multi-module/inventory/contract"
)

// NAME is the unique identifier for the inventory module.
const NAME = "inventory"

// InventoryModule implements the mono.Module for stock management
type InventoryModule struct {
	inventory map[string]int // product_id -> quantity
	mu        sync.RWMutex
}

// Compile-time interface checks
var _ mono.ServiceProviderModule = (*InventoryModule)(nil)

// NewModule creates a new inventory module with sample data
func NewModule() *InventoryModule {
	return &InventoryModule{
		inventory: map[string]int{
			"laptop":   10,
			"mouse":    50,
			"keyboard": 30,
			"monitor":  15,
		},
	}
}

// Name returns the module identifier
func (m *InventoryModule) Name() string {
	return NAME
}

// Start initializes the module
func (m *InventoryModule) Start(_ context.Context) error {
	fmt.Printf("  → Inventory module started with %d products in stock\n", len(m.inventory))
	return nil
}

// Stop gracefully shuts down the module
func (m *InventoryModule) Stop(_ context.Context) error {
	fmt.Println("  → Inventory module stopped")
	return nil
}

// RegisterServices is the driving adapter: it binds the domain handler to the
// check-stock contract. Deserialization of the request and serialization of
// the response are handled by the contract.
func (m *InventoryModule) RegisterServices(container mono.ServiceContainer) error {
	return contract.CheckStock.Register(container, m.checkStock)
}

// checkStock is the domain logic: pure, typed, and unit-testable without NATS.
func (m *InventoryModule) checkStock(_ context.Context, request contract.CheckStockRequest, _ *mono.Msg) (contract.CheckStockResponse, error) {
	if request.ProductID == "" {
		return contract.CheckStockResponse{}, fmt.Errorf("product_id is required")
	}
	if request.Quantity <= 0 {
		return contract.CheckStockResponse{}, fmt.Errorf("quantity must be positive")
	}

	m.mu.RLock()
	stock, exists := m.inventory[request.ProductID]
	m.mu.RUnlock()

	response := contract.CheckStockResponse{Stock: stock}
	if exists {
		response.Available = stock >= request.Quantity
	}

	fmt.Printf("  → Inventory: check_stock(%s, qty=%d) → available=%v, stock=%d\n",
		request.ProductID, request.Quantity, response.Available, response.Stock)

	return response, nil
}
