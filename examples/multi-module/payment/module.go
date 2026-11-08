// Package payment implements a payment processing module for handling payment transactions
// with simulated success rates for demonstration purposes. Its port is the
// contract sub-package.
package payment

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/examples/multi-module/payment/contract"
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

// RegisterServices binds the domain handler to the process contract.
func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
	return contract.Process.Register(container, m.process)
}

// process handles payment processing requests with typed request/response
func (m *PaymentModule) process(_ context.Context, request contract.ProcessPaymentRequest, _ *mono.Msg) (contract.ProcessPaymentResponse, error) {
	if request.OrderID == "" {
		return contract.ProcessPaymentResponse{}, fmt.Errorf("order_id is required")
	}
	if request.Amount <= 0 {
		return contract.ProcessPaymentResponse{}, fmt.Errorf("amount must be positive")
	}
	if request.Currency == "" {
		request.Currency = "USD" // Default currency
	}

	validCurrencies := map[string]bool{"USD": true, "EUR": true, "GBP": true}
	if !validCurrencies[request.Currency] {
		return contract.ProcessPaymentResponse{}, fmt.Errorf("unsupported currency: %s", request.Currency)
	}

	// Simulate payment processing with artificial delay
	time.Sleep(50 * time.Millisecond)

	// Simulate 90% success rate (for demonstration)
	if m.rand.Float64() < 0.9 {
		response := contract.ProcessPaymentResponse{
			Success:       true,
			TransactionID: fmt.Sprintf("txn_%d", time.Now().Unix()),
			Message:       "Payment processed successfully",
		}
		fmt.Printf("  → Payment: process(order=%s, amount=%.2f %s) → SUCCESS, txn=%s\n",
			request.OrderID, request.Amount, request.Currency, response.TransactionID)
		return response, nil
	}

	fmt.Printf("  → Payment: process(order=%s, amount=%.2f %s) → DECLINED\n",
		request.OrderID, request.Amount, request.Currency)
	return contract.ProcessPaymentResponse{
		Success: false,
		Message: "Payment declined by gateway",
	}, nil
}
