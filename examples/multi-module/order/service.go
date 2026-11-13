package order

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// This file is the domain of the order module: plain Go that depends only on
// the ports of ports.go. It imports neither the framework nor another module,
// so it is tested with fakes and no infrastructure (see service_test.go).

// ErrInvalidOrder is returned by Service.PlaceOrder for an order that cannot be
// placed as requested (missing product, non-positive quantity or amount).
var ErrInvalidOrder = errors.New("invalid order")

// defaultCurrency is used when an order does not name one.
const defaultCurrency = "USD"

// Status is the outcome of placing an order.
type Status string

const (
	// StatusSuccess: the order was placed and paid.
	StatusSuccess Status = "success"
	// StatusOutOfStock: the stock does not cover the order.
	StatusOutOfStock Status = "failed_out_of_stock"
	// StatusPaymentFailed: the payment was declined.
	StatusPaymentFailed Status = "payment_failed"
)

// NewOrder is a request to place an order.
type NewOrder struct {
	ProductID string
	Quantity  int
	Amount    float64
	Currency  string // defaults to USD
}

// PlacedOrder is an order that was placed and paid.
type PlacedOrder struct {
	OrderID   string
	ProductID string
	Amount    float64
	PlacedAt  time.Time
}

// Result reports how placing an order ended.
type Result struct {
	OrderID string
	Status  Status
	Message string
}

// Service places orders. It holds the order rules and reaches everything
// outside the module through its ports.
type Service struct {
	inventory    InventoryPort
	payment      PaymentPort
	notification NotificationPort
	events       EventsPort
}

// NewService creates the order service over its four ports. It returns an
// error when a port is nil, so a module that was not handed one of its
// dependencies fails at start instead of on the first order.
func NewService(inventory InventoryPort, payment PaymentPort, notification NotificationPort, events EventsPort) (*Service, error) {
	switch {
	case inventory == nil:
		return nil, errors.New("order service: inventory port is required")
	case payment == nil:
		return nil, errors.New("order service: payment port is required")
	case notification == nil:
		return nil, errors.New("order service: notification port is required")
	case events == nil:
		return nil, errors.New("order service: events port is required")
	}
	return &Service{inventory: inventory, payment: payment, notification: notification, events: events}, nil
}

// PlaceOrder checks the stock, charges the payment and, once the order is
// placed, asks for a notification and announces the order.
//
// An order that is out of stock or whose payment is declined is a normal
// outcome, reported in Result.Status with a nil error. The error is
// ErrInvalidOrder for a request that cannot be placed, or the failure of the
// inventory or payment port. Notifying and announcing are best effort: their
// failure is logged and the order stays placed.
func (s *Service) PlaceOrder(ctx context.Context, order NewOrder) (Result, error) {
	switch {
	case order.ProductID == "":
		return Result{}, fmt.Errorf("%w: product_id is required", ErrInvalidOrder)
	case order.Quantity <= 0:
		return Result{}, fmt.Errorf("%w: quantity must be positive", ErrInvalidOrder)
	case order.Amount <= 0:
		return Result{}, fmt.Errorf("%w: amount must be positive", ErrInvalidOrder)
	}
	if order.Currency == "" {
		order.Currency = defaultCurrency
	}

	now := time.Now()
	orderID := fmt.Sprintf("order_%d", now.Unix())

	// Step 1: the stock must cover the order.
	available, err := s.inventory.Available(ctx, order.ProductID, order.Quantity)
	if err != nil {
		return Result{}, fmt.Errorf("place order %s: %w", orderID, err)
	}
	if !available {
		return Result{OrderID: orderID, Status: StatusOutOfStock, Message: "Product is out of stock"}, nil
	}

	// Step 2: the payment must be approved.
	approved, err := s.payment.Charge(ctx, orderID, order.Amount, order.Currency)
	if err != nil {
		return Result{}, fmt.Errorf("place order %s: %w", orderID, err)
	}
	if !approved {
		return Result{OrderID: orderID, Status: StatusPaymentFailed, Message: "Payment was declined"}, nil
	}

	// Steps 3 and 4: notify and announce. Both are best effort (graceful
	// degradation): the order is placed whether or not they succeed.
	placed := PlacedOrder{OrderID: orderID, ProductID: order.ProductID, Amount: order.Amount, PlacedAt: now}
	if err := s.notification.OrderPlaced(ctx, placed); err != nil {
		fmt.Printf("  → Warning: failed to send notification for order %s: %v\n", orderID, err)
	}
	if err := s.events.OrderPlaced(ctx, placed); err != nil {
		fmt.Printf("  → Warning: failed to publish OrderCreated event for order %s: %v\n", orderID, err)
	}

	return Result{OrderID: orderID, Status: StatusSuccess, Message: "Order created successfully"}, nil
}
