// Package main demonstrates the EventEmitterModule pattern in mono-framework.
//
// This example shows:
//   - How to implement EventEmitterModule to declare events
//   - How to implement EventConsumerModule to consume events
//   - Event discovery via EventRegistry.GetEventByName()
//   - Using Publish/PublishRaw helper for event publishing
//   - Multiple versions of events (subject-based versioning)
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-monolith/mono/v1"
	fsjetstream "github.com/go-monolith/mono/v1/plugin/fs-jetstream"

	"github.com/go-monolith/mono/v1/examples/event-emitter/notification"
	"github.com/go-monolith/mono/v1/examples/event-emitter/tracking"
)

func main() {
	fmt.Println("=== Mono-Framework Event Emitter Example ===")
	fmt.Println("Demonstrates: EventEmitterModule, EventConsumerModule, Event Discovery")
	fmt.Println()

	// Step 1: Create app with configuration (JetStream enabled for fs-jetstream plugin)
	jsStorageDir := filepath.Join(os.TempDir(), "mono-event-emitter-jetstream")
	app, err := mono.NewMonoApplication(
		mono.WithLogLevel(mono.LogLevelInfo),
		mono.WithLogFormat(mono.LogFormatText),
		mono.WithShutdownTimeout(10*time.Second),
		mono.WithJetStreamStorageDir(jsStorageDir),
	)
	if err != nil {
		log.Fatalf("Failed to create app: %v", err)
	}

	fmt.Println("App created successfully")

	// Step 2: Create and register the file storage plugin
	storagePlugin, err := fsjetstream.New(fsjetstream.Config{
		Buckets: []fsjetstream.BucketConfig{
			{
				Name:        "order-created-events",
				Description: "Stores OrderCreated event notifications as JSON",
				Storage:     fsjetstream.MemoryStorage,
			},
			{
				Name:        "order-shipped-events",
				Description: "Stores OrderShipped event notifications as JSON",
				Storage:     fsjetstream.MemoryStorage,
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to create storage plugin: %v", err)
	}

	if err := app.RegisterPlugin(storagePlugin, "storage"); err != nil {
		log.Fatalf("Failed to register storage plugin: %v", err)
	}
	fmt.Println("Storage plugin registered (fs-jetstream)")

	// Step 3: Create modules
	trackingModule := tracking.NewModule()
	notificationModule := notification.NewModule()

	// Step 4: Register modules
	// Tracking module must be registered first as it's the event emitter
	// Notification module will discover events from tracking module
	if err := app.Register(trackingModule); err != nil {
		log.Fatalf("Failed to register tracking module: %v", err)
	}
	fmt.Println("Tracking module registered (EventEmitterModule)")

	if err := app.Register(notificationModule); err != nil {
		log.Fatalf("Failed to register notification module: %v", err)
	}
	fmt.Println("Notification module registered (EventConsumerModule + UsePluginModule)")

	fmt.Printf("Registered modules: %v\n", app.Modules())
	fmt.Println()

	// Step 5: Start the app
	// During startup, the framework:
	//   1. Starts plugins (fs-jetstream creates object store buckets)
	//   2. Injects plugins into UsePluginModules (notification.SetPlugin())
	//   3. Collects event definitions from EventEmitterModules (tracking.EmitEvents())
	//   4. Provides EventRegistry to EventConsumerModules (notification.RegisterEventConsumers())
	//   5. Sets up NATS subscriptions for event consumers
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatalf("Failed to start app: %v", err)
	}

	fmt.Println("App started successfully")
	fmt.Println()

	// Step 6: Check app health
	health := app.Health(ctx)
	fmt.Printf("App Health: healthy=%v, nats_healthy=%v\n", health.Healthy, health.NATSHealthy)
	fmt.Println()

	// Step 7: Wait for all subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Step 8: Run example scenarios
	fmt.Println("Running example scenarios...")
	fmt.Println()

	runScenarios(trackingModule)

	// Step 9: Show notification summary and stored files
	fmt.Println()
	fmt.Println("=== Notification Summary ===")
	showNotificationSummary(notificationModule)

	// Step 10: Show stored files in storage plugin
	fmt.Println()
	fmt.Println("=== Stored Event Files ===")
	showStoredFiles(storagePlugin)

	// Step 11: Wait for interrupt signal (Ctrl+C)
	fmt.Println("\nPress Ctrl+C to shutdown...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n\nShutdown signal received...")

	// Step 12: Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := app.Stop(shutdownCtx); err != nil {
		log.Fatalf("Failed to stop app: %v", err)
	}

	fmt.Println("App stopped successfully")
	fmt.Println("Example completed!")
}

func runScenarios(trackingModule *tracking.TrackingModule) {
	// Scenario 1: Create an order
	fmt.Println("[Scenario 1] Creating an order...")
	orderID1, err := trackingModule.CreateOrder("CUST-001", "laptop", 1, 999.99, "USD")
	if err != nil {
		fmt.Printf("Failed to create order: %v\n", err)
	} else {
		fmt.Printf("Order created: %s\n", orderID1)
	}
	fmt.Println()

	// Wait for event processing
	time.Sleep(200 * time.Millisecond)

	// Scenario 2: Ship the order
	fmt.Println("[Scenario 2] Shipping the order...")
	if err := trackingModule.ShipOrder(orderID1, "TRK-12345", "FedEx"); err != nil {
		fmt.Printf("Failed to ship order: %v\n", err)
	} else {
		fmt.Println("Order shipped successfully")
	}
	fmt.Println()

	// Wait for event processing
	time.Sleep(200 * time.Millisecond)

	// Scenario 3: Create another order
	fmt.Println("[Scenario 3] Creating another order...")
	orderID2, err := trackingModule.CreateOrder("CUST-002", "keyboard", 2, 79.99, "USD")
	if err != nil {
		fmt.Printf("Failed to create order: %v\n", err)
	} else {
		fmt.Printf("Order created: %s\n", orderID2)
	}
	fmt.Println()

	// Wait for event processing
	time.Sleep(200 * time.Millisecond)
}

func showNotificationSummary(notificationModule *notification.NotificationModule) {
	orderNotifications := notificationModule.GetOrderCreatedNotifications()
	shippingNotifications := notificationModule.GetOrderShippedNotifications()

	fmt.Printf("Total order notifications sent: %d\n", len(orderNotifications))
	for i, n := range orderNotifications {
		fmt.Printf("  %d. Order %s - Customer %s - $%.2f\n", i+1, n.OrderID, n.CustomerID, n.Amount)
	}

	fmt.Printf("Total shipping notifications sent: %d\n", len(shippingNotifications))
	for i, n := range shippingNotifications {
		fmt.Printf("  %d. Order %s - Tracking %s (%s)\n", i+1, n.OrderID, n.TrackingNum, n.Carrier)
	}
}

func showStoredFiles(storagePlugin *fsjetstream.PluginModule) {
	ctx := context.Background()

	// Show files in order-created-events bucket
	orderCreatedBucket := storagePlugin.Bucket("order-created-events")
	if orderCreatedBucket != nil {
		files, err := orderCreatedBucket.ListWithContext(ctx)
		if err != nil {
			fmt.Printf("Failed to list order-created-events: %v\n", err)
		} else {
			fmt.Printf("Files in 'order-created-events' bucket: %d\n", len(files))
			for _, f := range files {
				fmt.Printf("  - %s (%d bytes)\n", f.Name, f.Size)
			}
		}
	}

	// Show files in order-shipped-events bucket
	orderShippedBucket := storagePlugin.Bucket("order-shipped-events")
	if orderShippedBucket != nil {
		files, err := orderShippedBucket.ListWithContext(ctx)
		if err != nil {
			fmt.Printf("Failed to list order-shipped-events: %v\n", err)
		} else {
			fmt.Printf("Files in 'order-shipped-events' bucket: %d\n", len(files))
			for _, f := range files {
				fmt.Printf("  - %s (%d bytes)\n", f.Name, f.Size)
			}
		}
	}
}
