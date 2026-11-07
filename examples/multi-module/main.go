// Package main demonstrates the multi-module pattern in mono-framework.
//
// This example shows:
//   - Module dependencies and service orchestration
//   - Typed service contracts (<module>/contract packages) used by both sides
//   - Ports and adapters: consumer-owned ports, adapters over contracts,
//     ConsumeServices() validated by the framework at startup
//   - RequestReply and QueueGroup service patterns
//   - EventEmitterModule for event publishing (order module)
//   - EventRegistryAwareModule for event consuming (notification module)
//   - Request ID tracking and access logging middleware
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/middleware/accesslog"
	"github.com/go-monolith/mono/middleware/requestid"

	"github.com/go-monolith/mono/examples/multi-module/inventory"
	"github.com/go-monolith/mono/examples/multi-module/notification"
	"github.com/go-monolith/mono/examples/multi-module/order"
	ordercontract "github.com/go-monolith/mono/examples/multi-module/order/contract"
	"github.com/go-monolith/mono/examples/multi-module/payment"
)

func main() {
	fmt.Println("=== Mono-Framework Multi-Module Example ===")
	fmt.Println("Demonstrates: Module dependencies, RequestReply, QueueGroup, EventEmitterModule, EventRegistryAwareModule")
	fmt.Println()

	// Step 1: Create app with configuration
	app, err := mono.NewMonoApplication(
		mono.WithLogLevel(mono.LogLevelDebug),
		mono.WithLogFormat(mono.LogFormatText),
		mono.WithNATSLogging(true, false, false), // Enable NATS server debug logging
		mono.WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to create app: %v", err)
	}

	fmt.Println("✓ App created successfully")

	// Step 2: Setup access logging middleware
	// Create access log file with restrictive permissions (owner read/write only)
	accessFile, err := os.OpenFile("access.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatalf("Failed to create access log file: %v", err)
	}
	// File will be closed by accesslog module on app shutdown

	// Create access log middleware with JSON format
	accessModule, err := accesslog.New(
		accesslog.WithOutput(accessFile),
		accesslog.WithFormat(accesslog.FormatJSON),
		// Log all fields for complete visibility
		accesslog.WithFields(accesslog.AllFields()),
	)
	if err != nil {
		log.Fatalf("Failed to create access log module: %v", err)
	}

	// Register access log middleware (after requestid, before business modules)
	if err := app.Register(accessModule); err != nil {
		log.Fatalf("Failed to register access log module: %v", err)
	}
	fmt.Println("✓ Access log middleware registered (output: access.log)")

	// Step 3: Setup request ID middleware (must be after accesslog)
	// Request ID middleware extracts/generates X-Request-ID and propagates it
	// to downstream services via the OnOutgoingMessage hook. It must be registered
	// AFTER accesslog so that accesslog can extract the request ID from the
	// message headers.
	requestIDModule, err := requestid.New()
	if err != nil {
		log.Fatalf("Failed to create request ID module: %v", err)
	}

	if err := app.Register(requestIDModule); err != nil {
		log.Fatalf("Failed to register request ID module: %v", err)
	}
	fmt.Println("✓ Request ID middleware registered")

	// Step 4: Register business modules in order (dependencies first, then dependents)
	// - Order module implements EventEmitterModule to declare events it emits
	// - Notification module implements EventRegistryAwareModule to consume events
	// During startup, the framework:
	//   1. Collects event definitions from EventEmitterModules (order.EmitEvents())
	//   2. Provides EventRegistry to EventRegistryAwareModules (notification.SetEventRegistry())
	//   3. Sets up NATS subscriptions for event consumers
	modules := []mono.Module{
		inventory.NewModule(),
		payment.NewModule(),
		notification.NewModule(),
		order.NewModule(),
	}

	for _, module := range modules {
		if err := app.Register(module); err != nil {
			log.Fatalf("Failed to register module %s: %v", module.Name(), err)
		}
	}

	fmt.Printf("✓ Business modules registered: %v\n", app.Modules())

	// Step 5: Start the app
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatalf("Failed to start app: %v", err)
	}

	fmt.Println("✓ App started (request ID tracking and access logging active)")
	fmt.Println()

	// Step 6: Check app health
	health := app.Health(ctx)
	fmt.Printf("App Health: healthy=%v, nats_healthy=%v\n", health.Healthy, health.NATSHealthy)
	fmt.Println()

	// Step 7: Wait for all services to be fully ready
	time.Sleep(500 * time.Millisecond)

	// Step 8: Run example scenarios
	fmt.Println("Running example scenarios...")
	fmt.Println("(All service calls are being logged to access.log)")
	fmt.Println()

	runScenarios(app)

	// Step 9: Wait for interrupt signal (Ctrl+C)
	fmt.Println("\nPress Ctrl+C to shutdown...")
	fmt.Println("Tip: Check 'access.log' to see all service calls logged in JSON format with X-Request-ID")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n\nShutdown signal received...")

	// Step 10: Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := app.Stop(shutdownCtx); err != nil {
		log.Fatalf("Failed to stop app: %v", err)
	}

	fmt.Println("✓ App stopped successfully")
	fmt.Println("Example completed!")
}

func runScenarios(app mono.MonoApplication) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Inside the monolith, modules reach the order module through its contract
	// package (ordercontract.PlaceOrder) and the container they receive in
	// SetDependencyServiceContainer. Here in main.go, the host program does the
	// same with the container the framework exposes via app.Services(name);
	// app.Services is a host-level escape hatch, not for module code.
	orderContainer := app.Services("order")
	if orderContainer == nil {
		fmt.Println("⚠ Order service container not available")
		return
	}

	// Scenario 1: Successful order
	fmt.Println("[Scenario 1] Successful Order")
	placeOrder(ctx, orderContainer, "laptop", 1, 999.99, "USD")
	fmt.Println()

	time.Sleep(500 * time.Millisecond)

	// Scenario 2: Out of stock
	fmt.Println("[Scenario 2] Out of Stock")
	placeOrder(ctx, orderContainer, "rare-item", 1, 1999.99, "USD")
	fmt.Println()

	time.Sleep(500 * time.Millisecond)

	// Scenario 3: Another successful order (or payment might fail due to 90% success rate)
	fmt.Println("[Scenario 3] Another Order")
	placeOrder(ctx, orderContainer, "mouse", 2, 29.99, "USD")
	fmt.Println()

	time.Sleep(500 * time.Millisecond)

	// Scenario 4: Third order attempt
	fmt.Println("[Scenario 4] Third Order")
	placeOrder(ctx, orderContainer, "keyboard", 1, 79.99, "USD")
	fmt.Println()
}

func placeOrder(ctx context.Context, orderContainer mono.ServiceContainer, productID string, quantity int, amount float64, currency string) {
	fmt.Printf("  → Placing order for product '%s' (qty: %d, $%.2f %s)\n", productID, quantity, amount, currency)

	// Call the order service through its typed contract: request and response
	// types, service name and serialization all come from the contract.
	result, err := ordercontract.PlaceOrder.Call(ctx, orderContainer, ordercontract.CreateOrderRequest{
		ProductID: productID,
		Quantity:  quantity,
		Amount:    amount,
		Currency:  currency,
	})
	if err != nil {
		fmt.Printf("  ✗ Order failed: %v\n", err)
		return
	}

	switch result.Status {
	case "success":
		fmt.Printf("  ✓ Order created successfully: %s\n", result.OrderID)
	case "failed_out_of_stock":
		fmt.Printf("  ✗ Order failed: out of stock\n")
	case "payment_failed":
		fmt.Printf("  ✗ Order failed: payment declined\n")
	default:
		fmt.Printf("  ? Order status: %s - %s\n", result.Status, result.Message)
	}
}
