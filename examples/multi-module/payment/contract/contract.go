// Package contract is the port of the payment module: its DTOs and the typed
// service contracts other modules call it through. It is the only package of
// the payment module another module may import.
package contract

import "github.com/go-monolith/mono/pkg/helper"

// ProcessPaymentRequest asks to charge Amount in Currency for OrderID.
type ProcessPaymentRequest struct {
	OrderID  string  `json:"order_id"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

// ProcessPaymentResponse reports the outcome of a payment attempt.
type ProcessPaymentResponse struct {
	Success       bool   `json:"success"`
	TransactionID string `json:"transaction_id"`
	Message       string `json:"message,omitempty"`
}

// Process is the request-reply service that processes a payment.
var Process = helper.RequestReplyService[ProcessPaymentRequest, ProcessPaymentResponse]("payment", "process")
