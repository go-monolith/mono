package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-monolith/mono"
)

func main() {
	fmt.Println("=== Mono-Framework Basic Example ===")
	fmt.Println("Demonstrates: Application initialization, module registration, and lifecycle management")
	fmt.Println()

	// Step 1: Create app with configuration
	app, err := mono.NewMonoApplication(
		mono.WithLogLevel(mono.LogLevelInfo),      // Set log level
		mono.WithLogFormat(mono.LogFormatText),    // Use text format (human-readable)
		mono.WithNATSLogging(false, false, false), // Also enable logs from NATS server
		mono.WithShutdownTimeout(10*time.Second),  // 10 second graceful shutdown
	)
	if err != nil {
		log.Fatalf("Failed to create app: %v", err)
	}

	fmt.Println("✓ App created successfully")

	// Step 2: Create and register the Hello World module
	helloModule := NewHelloModule()
	if err := app.Register(helloModule); err != nil {
		log.Fatalf("Failed to register module: %v", err)
	}

	fmt.Println("✓ Module registered successfully")

	// Step 3: Start the app
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatalf("Failed to start app: %v", err)
	}

	fmt.Println("✓ App started - module is running")
	fmt.Println()

	// Step 4: Check app health
	health := app.Health(ctx)
	// convert to JSON for pretty printing
	healthJSON, err := json.MarshalIndent(health, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal health status: %v", err)
	}
	fmt.Printf("App Health:\n%s\n", healthJSON)

	// List registered modules
	modules := app.Modules()
	fmt.Printf("Registered modules: %v\n", modules)
	fmt.Println()

	// Step 5: Wait for interrupt signal (Ctrl+C)
	fmt.Println("Press Ctrl+C to shutdown...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n\nShutdown signal received...")

	// Step 6: Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := app.Stop(shutdownCtx); err != nil {
		log.Fatalf("Failed to stop app: %v", err)
	}

	fmt.Println("✓ App stopped successfully")
	fmt.Println("Example completed!")
}
