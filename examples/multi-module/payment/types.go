package payment

import "context"

// PaymentAdapterPort provides a type-safe interface for payment processing
type PaymentAdapterPort interface {
	Process(ctx context.Context, req *ProcessPaymentRequest) (*ProcessPaymentResponse, error)
}

// ProcessPaymentRequest represents a payment processing request
type ProcessPaymentRequest struct {
	OrderID  string  `json:"order_id"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

// ProcessPaymentResponse represents the payment processing response
type ProcessPaymentResponse struct {
	Success       bool   `json:"success"`
	TransactionID string `json:"transaction_id"`
	Message       string `json:"message,omitempty"`
}
