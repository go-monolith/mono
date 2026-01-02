// Package bench provides multi-module benchmark infrastructure for measuring
// inter-module communication performance in the mono-framework.
//
// This file contains simplified benchmark module implementations that simulate
// a realistic order workflow with 4 modules:
//   - BenchInventoryModule: Provides stock checking service (RequestReply)
//   - BenchPaymentModule: Provides payment processing service (RequestReply)
//   - BenchNotificationModule: Provides notification service (QueueGroup + EventConsumer)
//   - BenchOrderModule: Orchestrates the order workflow (DependentModule + EventEmitter)
package bench

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	mono "github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// MultiModuleResult is a sink variable to prevent compiler optimizations in multi-module benchmarks.
// Exported to satisfy the unused linter.
var MultiModuleResult any

// =============================================================================
// Benchmark Types (simplified for benchmarking - no business logic)
// =============================================================================

// BenchCheckStockRequest is a simplified stock check request.
type BenchCheckStockRequest struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// BenchCheckStockResponse is a simplified stock check response.
type BenchCheckStockResponse struct {
	Available bool `json:"available"`
	Stock     int  `json:"stock"`
}

// BenchPaymentRequest is a simplified payment request.
type BenchPaymentRequest struct {
	OrderID  string  `json:"order_id"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

// BenchPaymentResponse is a simplified payment response.
type BenchPaymentResponse struct {
	Success       bool   `json:"success"`
	TransactionID string `json:"transaction_id"`
}

// BenchNotificationRequest is a simplified notification request.
type BenchNotificationRequest struct {
	OrderID   string  `json:"order_id"`
	ProductID string  `json:"product_id"`
	Amount    float64 `json:"amount"`
}

// BenchOrderRequest is a simplified order request.
type BenchOrderRequest struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

// BenchOrderResponse is a simplified order response.
type BenchOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

// BenchOrderCreatedEvent represents the order created event for benchmarks.
type BenchOrderCreatedEvent struct {
	OrderID   string    `json:"order_id"`
	ProductID string    `json:"product_id"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

// BenchOrderCreatedV1 is the event definition for benchmark order events.
var BenchOrderCreatedV1 = helper.EventDefinition[BenchOrderCreatedEvent](
	"bench-order", "BenchOrderCreated", "v1",
)

// =============================================================================
// BenchInventoryModule - ServiceProviderModule with RequestReply
// =============================================================================

// BenchInventoryModule is a simplified inventory module for benchmarks.
// It implements Module and ServiceProviderModule interfaces.
type BenchInventoryModule struct {
	name string
}

// Compile-time interface check.
var _ mono.ServiceProviderModule = (*BenchInventoryModule)(nil)

// NewBenchInventoryModule creates a new benchmark inventory module.
func NewBenchInventoryModule() *BenchInventoryModule {
	return &BenchInventoryModule{name: "bench-inventory"}
}

// Name returns the module name.
func (m *BenchInventoryModule) Name() string { return m.name }

// Start is a no-op for benchmark modules.
func (m *BenchInventoryModule) Start(_ context.Context) error { return nil }

// Stop is a no-op for benchmark modules.
func (m *BenchInventoryModule) Stop(_ context.Context) error { return nil }

// RegisterServices registers the check-stock RequestReply service.
func (m *BenchInventoryModule) RegisterServices(container mono.ServiceContainer) error {
	return helper.RegisterTypedRequestReplyService(
		container,
		"check-stock",
		json.Unmarshal,
		json.Marshal,
		m.checkStock,
	)
}

// checkStock is a minimal handler that always returns available.
func (m *BenchInventoryModule) checkStock(
	_ context.Context,
	req BenchCheckStockRequest,
	_ *mono.Msg,
) (BenchCheckStockResponse, error) {
	// Minimal logic: always available, use request data to prevent optimization
	return BenchCheckStockResponse{
		Available: true,
		Stock:     req.Quantity + 100,
	}, nil
}

// =============================================================================
// BenchPaymentModule - ServiceProviderModule with RequestReply
// =============================================================================

// BenchPaymentModule is a simplified payment module for benchmarks.
// It implements Module and ServiceProviderModule interfaces.
type BenchPaymentModule struct {
	name string
}

// Compile-time interface check.
var _ mono.ServiceProviderModule = (*BenchPaymentModule)(nil)

// NewBenchPaymentModule creates a new benchmark payment module.
func NewBenchPaymentModule() *BenchPaymentModule {
	return &BenchPaymentModule{name: "bench-payment"}
}

// Name returns the module name.
func (m *BenchPaymentModule) Name() string { return m.name }

// Start is a no-op for benchmark modules.
func (m *BenchPaymentModule) Start(_ context.Context) error { return nil }

// Stop is a no-op for benchmark modules.
func (m *BenchPaymentModule) Stop(_ context.Context) error { return nil }

// RegisterServices registers the process RequestReply service.
func (m *BenchPaymentModule) RegisterServices(container mono.ServiceContainer) error {
	return helper.RegisterTypedRequestReplyService(
		container,
		"process",
		json.Unmarshal,
		json.Marshal,
		m.process,
	)
}

// process is a minimal handler that always returns success.
func (m *BenchPaymentModule) process(
	_ context.Context,
	req BenchPaymentRequest,
	_ *mono.Msg,
) (BenchPaymentResponse, error) {
	return BenchPaymentResponse{
		Success:       true,
		TransactionID: req.OrderID,
	}, nil
}

// =============================================================================
// BenchNotificationModule - ServiceProviderModule + EventConsumerModule
// =============================================================================

// BenchNotificationModule is a simplified notification module for benchmarks.
// It implements ServiceProviderModule (QueueGroup) and EventConsumerModule interfaces.
type BenchNotificationModule struct {
	name       string
	NotifCount atomic.Int64 // Counter for queue group notifications
	EventCount atomic.Int64 // Counter for event notifications
}

// Compile-time interface checks.
var (
	_ mono.ServiceProviderModule = (*BenchNotificationModule)(nil)
	_ mono.EventConsumerModule   = (*BenchNotificationModule)(nil)
)

// NewBenchNotificationModule creates a new benchmark notification module.
func NewBenchNotificationModule() *BenchNotificationModule {
	return &BenchNotificationModule{name: "bench-notification"}
}

// Name returns the module name.
func (m *BenchNotificationModule) Name() string { return m.name }

// Start is a no-op for benchmark modules.
func (m *BenchNotificationModule) Start(_ context.Context) error { return nil }

// Stop is a no-op for benchmark modules.
func (m *BenchNotificationModule) Stop(_ context.Context) error { return nil }

// RegisterServices registers the on-order-created QueueGroup service.
func (m *BenchNotificationModule) RegisterServices(container mono.ServiceContainer) error {
	return helper.RegisterTypedQueueGroupService(
		container,
		"on-order-created",
		json.Unmarshal,
		types.TypedQGHP[BenchNotificationRequest]{
			QueueGroup: "bench-notification-workers",
			Handler:    m.handleNotification,
		},
	)
}

// handleNotification processes queue group notifications.
func (m *BenchNotificationModule) handleNotification(
	_ context.Context,
	notif BenchNotificationRequest,
	_ *mono.Msg,
) error {
	m.NotifCount.Add(1)
	MultiModuleResult = notif.OrderID // sink to prevent optimization
	return nil
}

// RegisterEventConsumers registers the event consumer for BenchOrderCreatedV1.
func (m *BenchNotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	return helper.RegisterTypedEventConsumer(
		registry,
		BenchOrderCreatedV1,
		m.handleOrderCreatedEvent,
		m,
	)
}

// handleOrderCreatedEvent processes order created events.
func (m *BenchNotificationModule) handleOrderCreatedEvent(
	_ context.Context,
	event BenchOrderCreatedEvent,
	_ *mono.Msg,
) error {
	m.EventCount.Add(1)
	MultiModuleResult = event.OrderID // sink to prevent optimization
	return nil
}

// =============================================================================
// BenchOrderModule - DependentModule + ServiceProviderModule + EventEmitterModule
// =============================================================================

// BenchOrderModule is a simplified order module that orchestrates the workflow.
// It implements DependentModule, ServiceProviderModule, and EventEmitterModule interfaces.
type BenchOrderModule struct {
	name          string
	container     mono.ServiceContainer
	eventBus      mono.EventBus
	depContainers map[string]mono.ServiceContainer
}

// Compile-time interface checks.
var (
	_ mono.DependentModule       = (*BenchOrderModule)(nil)
	_ mono.ServiceProviderModule = (*BenchOrderModule)(nil)
	_ mono.EventEmitterModule    = (*BenchOrderModule)(nil)
	_ mono.EventBusAwareModule   = (*BenchOrderModule)(nil)
)

// NewBenchOrderModule creates a new benchmark order module.
func NewBenchOrderModule() *BenchOrderModule {
	return &BenchOrderModule{
		name:          "bench-order",
		depContainers: make(map[string]mono.ServiceContainer),
	}
}

// Name returns the module name.
func (m *BenchOrderModule) Name() string { return m.name }

// Start is a no-op for benchmark modules.
func (m *BenchOrderModule) Start(_ context.Context) error { return nil }

// Stop is a no-op for benchmark modules.
func (m *BenchOrderModule) Stop(_ context.Context) error { return nil }

// Dependencies returns the list of module dependencies.
func (m *BenchOrderModule) Dependencies() []string {
	return []string{"bench-inventory", "bench-payment", "bench-notification"}
}

// SetDependencyServiceContainer stores the dependency's service container.
func (m *BenchOrderModule) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
	m.depContainers[dep] = container
}

// SetEventBus stores the event bus for publishing events.
func (m *BenchOrderModule) SetEventBus(bus mono.EventBus) {
	m.eventBus = bus
}

// EmitEvents returns the list of event definitions this module emits.
func (m *BenchOrderModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		BenchOrderCreatedV1.ToBase(),
	}
}

// RegisterServices registers the place-order RequestReply service.
func (m *BenchOrderModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container
	return helper.RegisterTypedRequestReplyService(
		container,
		"place-order",
		json.Unmarshal,
		json.Marshal,
		m.placeOrder,
	)
}

// Container returns the module's service container for external access.
func (m *BenchOrderModule) Container() mono.ServiceContainer { return m.container }

// placeOrder handles order placement with full workflow:
// 1. Check stock (RequestReply to inventory)
// 2. Process payment (RequestReply to payment)
// 3. Send notification (QueueGroup to notification)
// 4. Publish event (EventBus)
func (m *BenchOrderModule) placeOrder(
	ctx context.Context,
	req BenchOrderRequest,
	_ *mono.Msg,
) (BenchOrderResponse, error) {
	orderID := req.ProductID // Use product ID as order ID for simplicity

	// Step 1: Check stock
	var stockResp BenchCheckStockResponse
	if err := helper.CallRequestReplyService(
		ctx,
		m.depContainers["bench-inventory"],
		"check-stock",
		json.Marshal,
		json.Unmarshal,
		BenchCheckStockRequest{ProductID: req.ProductID, Quantity: req.Quantity},
		&stockResp,
	); err != nil {
		return BenchOrderResponse{}, err
	}

	// Step 2: Process payment
	var paymentResp BenchPaymentResponse
	if err := helper.CallRequestReplyService(
		ctx,
		m.depContainers["bench-payment"],
		"process",
		json.Marshal,
		json.Unmarshal,
		BenchPaymentRequest{OrderID: orderID, Amount: req.Amount, Currency: req.Currency},
		&paymentResp,
	); err != nil {
		return BenchOrderResponse{}, err
	}

	// Step 3: Send notification (fire-and-forget)
	if err := helper.SendQueueGroupService(
		ctx,
		m.depContainers["bench-notification"],
		"on-order-created",
		json.Marshal,
		BenchNotificationRequest{OrderID: orderID, ProductID: req.ProductID, Amount: req.Amount},
	); err != nil {
		return BenchOrderResponse{}, err
	}

	// Step 4: Publish event
	if err := BenchOrderCreatedV1.Publish(m.eventBus, BenchOrderCreatedEvent{
		OrderID:   orderID,
		ProductID: req.ProductID,
		Amount:    req.Amount,
		Timestamp: time.Now(),
	}, nil); err != nil {
		return BenchOrderResponse{}, err
	}

	return BenchOrderResponse{
		OrderID: orderID,
		Status:  "success",
	}, nil
}

// =============================================================================
// Setup Helper
// =============================================================================

// MultiModuleBenchSetup contains all components for multi-module benchmarks.
type MultiModuleBenchSetup struct {
	App          mono.MonoApplication
	Inventory    *BenchInventoryModule
	Payment      *BenchPaymentModule
	Notification *BenchNotificationModule
	Order        *BenchOrderModule
}

// NewMultiModuleBenchSetup creates a complete multi-module benchmark environment.
func NewMultiModuleBenchSetup() (*MultiModuleBenchSetup, error) {
	app, err := NewBenchApp()
	if err != nil {
		return nil, err
	}

	inventory := NewBenchInventoryModule()
	payment := NewBenchPaymentModule()
	notification := NewBenchNotificationModule()
	order := NewBenchOrderModule()

	// Register modules in dependency order
	for _, module := range []mono.Module{inventory, payment, notification, order} {
		if err := app.Register(module); err != nil {
			return nil, err
		}
	}

	return &MultiModuleBenchSetup{
		App:          app,
		Inventory:    inventory,
		Payment:      payment,
		Notification: notification,
		Order:        order,
	}, nil
}

// Start initializes the benchmark environment.
func (s *MultiModuleBenchSetup) Start(ctx context.Context) error {
	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return s.App.Start(startCtx)
}

// Stop gracefully shuts down the benchmark environment.
func (s *MultiModuleBenchSetup) Stop(ctx context.Context) error {
	return s.App.Stop(ctx)
}
