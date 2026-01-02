// Package bench provides multi-module benchmarks for mono-framework.
//
// These benchmarks measure the performance of complex inter-module communication:
//   - BenchmarkMultiModuleOrderOrchestration: Direct RequestReply service calls with full workflow
//   - BenchmarkMultiModuleOrderOrchestrationJson: Complete order workflow with typed JSON helpers
//
// All benchmarks involve at least 4 modules communicating:
// Order → Inventory (check-stock) → Payment (process) → Notification (queue + event)
//
// Run multi-module benchmarks with: go test -bench='BenchmarkMultiModule' -benchmem ./bench/
package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1/pkg/helper"
)

// BenchmarkMultiModuleOrderOrchestration benchmarks the multi-module order workflow
// using low-level raw RequestReply calls.
//
// The workflow involves 4 modules communicating:
// 1. Order module receives place-order request
// 2. Order → Inventory: check-stock (RequestReply)
// 3. Order → Payment: process (RequestReply)
// 4. Order → Notification: on-order-created (QueueGroup)
// 5. Order → EventBus: BenchOrderCreatedV1 (pub/sub event)
func BenchmarkMultiModuleOrderOrchestration(b *testing.B) {
	for _, size := range PayloadSizes {
		b.Run(fmt.Sprintf("payload_%dB", size), func(b *testing.B) {
			benchmarkMultiModuleOrderOrchestration(b, size)
		})
	}
}

func benchmarkMultiModuleOrderOrchestration(b *testing.B, payloadSize int) {
	ctx := context.Background()

	setup, err := NewMultiModuleBenchSetup()
	if err != nil {
		b.Fatalf("Failed to create setup: %v", err)
	}

	if err := setup.Start(ctx); err != nil {
		b.Fatalf("Failed to start: %v", err)
	}
	defer func() {
		if err := setup.Stop(ctx); err != nil {
			b.Errorf("Failed to stop setup: %v", err)
		}
	}()

	// Wait for subscriptions to be ready (100ms is sufficient for in-process connections)
	time.Sleep(100 * time.Millisecond)

	// Get the order service container
	container := setup.Order.Container()
	if container == nil {
		b.Fatal("Failed to get order container")
	}

	// Get the place-order service client
	client, err := container.GetRequestReplyService("place-order")
	if err != nil {
		b.Fatalf("Failed to get place-order service: %v", err)
	}

	// Create request with variable payload size
	// ProductID will be padded to reach desired payload size
	productID := string(GeneratePayload(payloadSize - 50)) // Account for JSON overhead
	if len(productID) < 1 {
		productID = "x"
	}
	request := BenchOrderRequest{
		ProductID: productID,
		Quantity:  1,
		Amount:    99.99,
		Currency:  "USD",
	}
	payload, err := json.Marshal(request)
	if err != nil {
		b.Fatalf("Failed to marshal request: %v", err)
	}

	benchCtx, benchCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer benchCancel()

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		resp, err := client.Call(benchCtx, payload)
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		result = resp
	}
}

// BenchmarkMultiModuleOrderOrchestrationJson benchmarks the complete order workflow
// using typed JSON helper functions and verifies async message delivery.
//
// The workflow involves 4 modules communicating:
// 1. Order module receives place-order request
// 2. Order → Inventory: check-stock (RequestReply)
// 3. Order → Payment: process (RequestReply)
// 4. Order → Notification: on-order-created (QueueGroup)
// 5. Order → EventBus: BenchOrderCreatedV1 (pub/sub event)
//
// This benchmark also verifies that all async notifications and events are delivered.
func BenchmarkMultiModuleOrderOrchestrationJSON(b *testing.B) {
	for _, size := range PayloadSizes {
		b.Run(fmt.Sprintf("payload_%dB", size), func(b *testing.B) {
			benchmarkMultiModuleOrderOrchestrationJSON(b, size)
		})
	}
}

func benchmarkMultiModuleOrderOrchestrationJSON(b *testing.B, payloadSize int) {
	ctx := context.Background()

	setup, err := NewMultiModuleBenchSetup()
	if err != nil {
		b.Fatalf("Failed to create setup: %v", err)
	}

	if err := setup.Start(ctx); err != nil {
		b.Fatalf("Failed to start: %v", err)
	}
	defer func() {
		if err := setup.Stop(ctx); err != nil {
			b.Errorf("Failed to stop setup: %v", err)
		}
	}()

	// Wait for subscriptions to be ready (100ms is sufficient for in-process connections)
	time.Sleep(100 * time.Millisecond)

	// Get the order service container
	container := setup.Order.Container()
	if container == nil {
		b.Fatal("Failed to get order container")
	}

	// Create request with variable payload size
	productID := string(GeneratePayload(payloadSize - 50))
	if len(productID) < 1 {
		productID = "x"
	}
	request := BenchOrderRequest{
		ProductID: productID,
		Quantity:  1,
		Amount:    99.99,
		Currency:  "USD",
	}

	benchCtx, benchCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer benchCancel()

	// Reset async counters
	setup.Notification.NotifCount.Store(0)
	setup.Notification.EventCount.Store(0)

	// Calculate approximate bytes per operation (request + all intermediate messages)
	// This is an estimate since multiple messages flow through the system
	payload, _ := json.Marshal(request)
	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		var resp BenchOrderResponse
		if err := helper.CallRequestReplyService(
			benchCtx,
			container,
			"place-order",
			json.Marshal,
			json.Unmarshal,
			request,
			&resp,
		); err != nil {
			b.Fatalf("Workflow failed: %v", err)
		}
		result = resp
	}

	b.StopTimer()

	// Wait for async operations (notifications and events) to complete
	deadline := time.Now().Add(10 * time.Second)
	for setup.Notification.NotifCount.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	for setup.Notification.EventCount.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	// Report async completion rates
	notifComplete := setup.Notification.NotifCount.Load()
	eventComplete := setup.Notification.EventCount.Load()
	if notifComplete < int64(b.N) {
		b.Logf("Warning: only %d/%d notifications completed", notifComplete, b.N)
	}
	if eventComplete < int64(b.N) {
		b.Logf("Warning: only %d/%d events completed", eventComplete, b.N)
	}
}
