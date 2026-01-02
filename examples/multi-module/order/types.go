package order

import (
	"context"
)

// OrderAdapterPort provides a type-safe interface for order operations
type OrderAdapterPort interface {
	PlaceOrder(ctx context.Context, req *CreateOrderRequest) (*CreateOrderResponse, error)
}

// CreateOrderRequest represents a request to create an order
type CreateOrderRequest struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

// CreateOrderResponse represents the order creation response
type CreateOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"` // "success", "failed_out_of_stock", "payment_failed"
	Message string `json:"message,omitempty"`
}
