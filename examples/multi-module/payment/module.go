// Package payment implements a payment processing module for handling payment transactions
// with simulated success rates for demonstration purposes.
package payment

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
)

// NAME is the unique identifier for the payment module.
const NAME = "payment"

// PaymentModule implements the mono.Module for payment processing
type PaymentModule struct {
	rand *rand.Rand
}

// Compile-time interface checks
var _ mono.ServiceProviderModule = (*PaymentModule)(nil)

// NewModule creates a new payment module
func NewModule() *PaymentModule {
	return &PaymentModule{
		// #nosec G404 -- Using math/rand for demo/simulation purposes only, not for security
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Name returns the module identifier
func (m *PaymentModule) Name() string {
	return NAME
}

// Start initializes the module
func (m *PaymentModule) Start(_ context.Context) error {
	fmt.Println("  → Payment module started")
	return nil
}

// Stop gracefully shuts down the module
func (m *PaymentModule) Stop(_ context.Context) error {
	fmt.Println("  → Payment module stopped")
	return nil
}

// RegisterServices registers the process payment service using type-safe helper
func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
	// Register the process request-reply service using typed helper
	// This automatically handles JSON unmarshaling of requests and marshaling of responses
	return helper.RegisterTypedRequestReplyService(
		container,
		"process",
		json.Unmarshal,
		json.Marshal,
		m.process,
	)
}

// process handles payment processing requests with typed request/response
func (m *PaymentModule) process(_ context.Context, request ProcessPaymentRequest, _ *mono.Msg) (ProcessPaymentResponse, error) {
	// Validate request
	if request.OrderID == "" {
		return ProcessPaymentResponse{}, fmt.Errorf("order_id is required")
	}
	if request.Amount <= 0 {
		return ProcessPaymentResponse{}, fmt.Errorf("amount must be positive")
	}
	if request.Currency == "" {
		request.Currency = "USD" // Default currency
	}

	// Supported currencies
	validCurrencies := map[string]bool{
		"USD": true,
		"EUR": true,
		"GBP": true,
	}
	if !validCurrencies[request.Currency] {
		return ProcessPaymentResponse{}, fmt.Errorf("unsupported currency: %s", request.Currency)
	}

	// Simulate payment processing with artificial delay
	time.Sleep(50 * time.Millisecond)

	// Simulate 90% success rate (for demonstration)
	success := m.rand.Float64() < 0.9

	var response ProcessPaymentResponse
	if success {
		response = ProcessPaymentResponse{
			Success:       true,
			TransactionID: fmt.Sprintf("txn_%d", time.Now().Unix()),
			Message:       "Payment processed successfully",
		}
		fmt.Printf("  → Payment: process(order=%s, amount=%.2f %s) → SUCCESS, txn=%s\n",
			request.OrderID, request.Amount, request.Currency, response.TransactionID)
	} else {
		response = ProcessPaymentResponse{
			Success:       false,
			TransactionID: "",
			Message:       "Payment declined by gateway",
		}
		fmt.Printf("  → Payment: process(order=%s, amount=%.2f %s) → DECLINED\n",
			request.OrderID, request.Amount, request.Currency)
	}

	return response, nil
}
