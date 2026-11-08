// Package contract is the port of the order module: the DTOs of its services,
// the typed service contracts callers use, and the events it emits. It is the
// only package of the order module another module (or the host program) may
// import.
package contract

import "github.com/go-monolith/mono/pkg/helper"

// CreateOrderRequest asks to place an order.
type CreateOrderRequest struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

// CreateOrderResponse reports the outcome of an order placement.
type CreateOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"` // "success", "failed_out_of_stock", "payment_failed"
	Message string `json:"message,omitempty"`
}

// PlaceOrder is the request-reply service that places an order.
var PlaceOrder = helper.RequestReplyService[CreateOrderRequest, CreateOrderResponse]("order", "place-order")
