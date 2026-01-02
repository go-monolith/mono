package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-monolith/mono/v1"
)

func main() {
	fmt.Println("=== Mono-Framework NATS Config File Example ===")
	fmt.Println("Demonstrates: Loading NATS configuration from a file with programmatic overrides")
	fmt.Println()

	// Find the config file relative to the example directory
	configFile := findConfigFile()
	fmt.Printf("Config file: %s\n", configFile)

	// Step 1: Create app with config file and programmatic overrides
	// The config file provides base settings (port 4222, host 127.0.0.1, JetStream enabled)
	// Programmatic options override specific settings from the config file
	app, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile(configFile), // Load base config from file
		mono.WithNATSPort(4333),             // Override: port 4222 -> 4333
		mono.WithNATSHost("0.0.0.0"),        // Override: host 127.0.0.1 -> 0.0.0.0
		mono.WithLogLevel(mono.LogLevelInfo),
		mono.WithLogFormat(mono.LogFormatText),
		mono.WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to create app: %v", err)
	}

	fmt.Println("✓ App created with config file + overrides")
	fmt.Println("  - Base config loaded from: server.conf")
	fmt.Println("  - Port overridden: 4222 -> 4333")
	fmt.Println("  - Host overridden: 127.0.0.1 -> 0.0.0.0")
	fmt.Println()

	// Step 2: Register a simple module
	helloModule := NewHelloModule()
	if err := app.Register(helloModule); err != nil {
		log.Fatalf("Failed to register module: %v", err)
	}

	fmt.Println("✓ Module registered")

	// Step 3: Start the app
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatalf("Failed to start app: %v", err)
	}

	fmt.Println("✓ App started - NATS server is running with merged configuration")
	fmt.Println()

	// Step 4: Check app health
	health := app.Health(ctx)
	fmt.Printf("Health Status: Healthy=%v, NATSHealthy=%v\n", health.Healthy, health.NATSHealthy)
	fmt.Println()

	// Step 5: Wait for interrupt signal
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

// findConfigFile locates server.conf relative to the executable or working directory
func findConfigFile() string {
	// Try working directory first (for go run .)
	if _, err := os.Stat("server.conf"); err == nil {
		return "server.conf"
	}

	// Try executable directory
	execPath, err := os.Executable()
	if err == nil {
		configPath := filepath.Join(filepath.Dir(execPath), "server.conf")
		if _, err := os.Stat(configPath); err == nil {
			return configPath
		}
	}

	// Try examples/nats-config directory (for running from repo root)
	configPath := "examples/nats-config/server.conf"
	if _, err := os.Stat(configPath); !errors.Is(err, fs.ErrNotExist) {
		return configPath
	}

	// Fall back to current directory
	return "server.conf"
}
