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

	"github.com/go-monolith/mono"
)

func main() {
	fmt.Println("=== Mono-Framework Cron Scheduler Example ===")
	fmt.Println("Demonstrates: server-side cron-scheduled services (JetStream message scheduler)")
	fmt.Println()

	// Cron services require JetStream, so a storage directory must be configured.
	storageDir := filepath.Join(os.TempDir(), "mono-cron-example")

	app, err := mono.NewMonoApplication(
		mono.WithLogLevel(mono.LogLevelInfo),
		mono.WithLogFormat(mono.LogFormatText),
		mono.WithJetStreamStorageDir(storageDir),
		mono.WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to create app: %v", err)
	}
	fmt.Println("✓ App created (JetStream enabled)")

	if err := app.Register(NewHeartbeatModule()); err != nil {
		log.Fatalf("Failed to register module: %v", err)
	}
	fmt.Println("✓ Module registered")

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatalf("Failed to start app: %v", err)
	}
	fmt.Println("✓ App started — the cron service fires every 2 seconds")
	fmt.Println("Press Ctrl+C to stop.")
	fmt.Println()

	// Block until interrupted.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Stop(shutdownCtx); err != nil {
		log.Fatalf("Failed to stop app: %v", err)
	}
	fmt.Println("✓ App stopped cleanly")
}
