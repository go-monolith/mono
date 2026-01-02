// Package inventory implements an inventory management module for stock tracking
// and availability checking.
package inventory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-monolith/mono/v1"
	"github.com/go-monolith/mono/v1/pkg/helper"
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

// RegisterServices registers the check_stock service using type-safe helper
func (m *InventoryModule) RegisterServices(container mono.ServiceContainer) error {
	// Register the check_stock request-reply service using typed helper
	// This automatically handles JSON unmarshaling of requests and marshaling of responses
	return helper.RegisterTypedRequestReplyService(
		container,
		"check-stock",
		json.Unmarshal,
		json.Marshal,
		m.checkStock,
	)
}

// checkStock handles stock availability requests with typed request/response
func (m *InventoryModule) checkStock(_ context.Context, request CheckStockRequest, _ *mono.Msg) (CheckStockResponse, error) {
	// Validate request
	if request.ProductID == "" {
		return CheckStockResponse{}, fmt.Errorf("product_id is required")
	}
	if request.Quantity <= 0 {
		return CheckStockResponse{}, fmt.Errorf("quantity must be positive")
	}

	// Check stock
	m.mu.RLock()
	stock, exists := m.inventory[request.ProductID]
	m.mu.RUnlock()

	var response CheckStockResponse
	if !exists {
		response = CheckStockResponse{
			Available: false,
			Stock:     0,
		}
	} else {
		response = CheckStockResponse{
			Available: stock >= request.Quantity,
			Stock:     stock,
		}
	}

	fmt.Printf("  → Inventory: check_stock(%s, qty=%d) → available=%v, stock=%d\n",
		request.ProductID, request.Quantity, response.Available, response.Stock)

	return response, nil
}
