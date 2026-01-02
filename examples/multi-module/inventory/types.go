package inventory

import "context"

// InventoryAdapterPort provides a type-safe interface for inventory operations
type InventoryAdapterPort interface {
	CheckStock(ctx context.Context, req *CheckStockRequest) (*CheckStockResponse, error)
}

// CheckStockRequest represents a request to check stock availability
type CheckStockRequest struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// CheckStockResponse represents the stock availability response
type CheckStockResponse struct {
	Available bool `json:"available"`
	Stock     int  `json:"stock"`
}
