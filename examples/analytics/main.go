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

	"github.com/go-monolith/mono/v1"
	kvjetstream "github.com/go-monolith/mono/v1/plugin/kv-jetstream"

	analytics "github.com/go-monolith/mono/v1/examples/analytics/analytics-module"
)

func main() {
	fmt.Println("=== Mono-Framework Channel Services Example ===")
	fmt.Println("Demonstrates: Channel-based services with kv-jetstream for persistent storage")
	fmt.Println()

	// Step 1: Create app with configuration
	// Create a temp directory for JetStream storage (enables JetStream)
	jsStorageDir, err := os.MkdirTemp("", "analytics-jetstream-*")
	if err != nil {
		log.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(jsStorageDir) }() //nolint:errcheck // Clean up on exit, error is not actionable

	app, err := mono.NewMonoApplication(
		mono.WithLogLevel(mono.LogLevelDebug),
		mono.WithLogFormat(mono.LogFormatText),
		mono.WithNATSLogging(true, true, false), // Also enable logs from NATS server
		mono.WithShutdownTimeout(10*time.Second),
		mono.WithJetStreamStorageDir(jsStorageDir), // Enable JetStream with storage dir
	)
	if err != nil {
		log.Fatalf("Failed to create app: %v", err)
	}

	fmt.Println("✓ App created successfully")

	// Step 2: Create and register the kv-jetstream plugin
	kvPlugin, err := kvjetstream.New(kvjetstream.Config{
		Buckets: []kvjetstream.BucketConfig{
			{
				Name:        analytics.BucketName,
				Description: "Analytics events storage",
				Storage:     kvjetstream.MemoryStorage, // Use memory storage for demo
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to create kv plugin: %v", err)
	}

	if err := app.RegisterPlugin(kvPlugin, "kv"); err != nil {
		log.Fatalf("Failed to register kv plugin: %v", err)
	}

	fmt.Println("✓ KV plugin registered")

	// Step 3: Register analytics module
	analyticsModule := analytics.NewModule()
	if err := app.Register(analyticsModule); err != nil {
		log.Fatalf("Failed to register module: %v", err)
	}

	fmt.Printf("✓ Modules registered: %v\n", app.Modules())

	// Step 4: Start the app
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatalf("Failed to start app: %v", err)
	}

	fmt.Println("✓ App started")
	fmt.Println()

	// Step 5: Check app health
	health := app.Health(ctx)
	fmt.Printf("App Health: healthy=%v, nats_healthy=%v\n", health.Healthy, health.NATSHealthy)
	fmt.Println()

	// Wait for services to be fully ready
	time.Sleep(500 * time.Millisecond)

	// Step 6: Get channel service
	// Note: In a real application, you would access services after the app is fully started
	services := app.Services("analytics")
	if services == nil {
		fmt.Println("⚠ Note: Channel services are registered but Services() API returns nil")
		fmt.Println("   This is expected behavior - channel services are accessible only within the same process")
		fmt.Println("   For cross-module communication, use RequestReply or QueueGroup services instead")
		fmt.Println()
		fmt.Println("Demonstration: Channel service handler is running in the background")
		fmt.Println("In production, other modules would access this via the ServiceContainer")
		fmt.Println()

		// Wait for signal
		fmt.Println("Press Ctrl+C to shutdown...")
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		fmt.Println("\n\nShutdown signal received...")

		// Graceful shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := app.Stop(shutdownCtx); err != nil {
			log.Fatalf("Failed to stop app: %v", err)
		}

		fmt.Println("✓ App stopped successfully")
		fmt.Println("Example completed!")
		return
	}

	inChan, outChan, err := services.GetChannelService("track-event", "example-client")
	if err != nil {
		log.Fatalf("Failed to get channel service: %v", err)
	}

	fmt.Println("Running example scenarios...")
	fmt.Println()

	// Track event IDs for later retrieval demonstration
	// totalCount reflects the server-side cumulative event count from the last response
	var trackedEventIDs []string
	var totalCount int

	// Scenario 1: Basic event tracking
	fmt.Println("[Scenario 1] Basic Event Tracking")
	// Note: ignoring count here, totalCount will be set by final scenario
	eventID, _ := trackBasicEvent(inChan, outChan)
	if eventID != "" {
		trackedEventIDs = append(trackedEventIDs, eventID)
	}
	fmt.Println()

	time.Sleep(200 * time.Millisecond)

	// Scenario 2: Multiple events
	fmt.Println("[Scenario 2] Multiple Events")
	// Note: ignoring count here, totalCount will be set by final scenario
	ids, _ := trackMultipleEvents(inChan, outChan)
	trackedEventIDs = append(trackedEventIDs, ids...)
	fmt.Println()

	time.Sleep(200 * time.Millisecond)

	// Scenario 3: Concurrent event tracking
	fmt.Println("[Scenario 3] Concurrent Event Tracking")
	ids, totalCount = trackConcurrentEvents(inChan, outChan)
	trackedEventIDs = append(trackedEventIDs, ids...)
	fmt.Println()

	time.Sleep(200 * time.Millisecond)

	// Scenario 4: Error handling
	fmt.Println("[Scenario 4] Error Handling")
	trackInvalidEvent(inChan, outChan)
	fmt.Println()

	time.Sleep(200 * time.Millisecond)

	// Scenario 5: Retrieve events using adapter pattern
	fmt.Println("[Scenario 5] Retrieve Events via Adapter Pattern")
	retrieveEventsViaAdapter(ctx, services, trackedEventIDs)
	fmt.Println()

	// Display statistics
	fmt.Printf("Total events tracked: %d\n", totalCount)
	fmt.Println()

	// Step 7: Wait for interrupt signal (Ctrl+C)
	fmt.Println("Press Ctrl+C to shutdown...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n\nShutdown signal received...")

	// Step 8: Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := app.Stop(shutdownCtx); err != nil {
		log.Fatalf("Failed to stop app: %v", err)
	}

	fmt.Println("✓ App stopped successfully")
	fmt.Println("Example completed!")
}

func trackBasicEvent(inChan, outChan chan *mono.Msg) (string, int) {
	request := analytics.TrackEventRequest{
		EventType: "page_view",
		UserID:    "user123",
		Properties: map[string]any{
			"page":     "/home",
			"referrer": "google.com",
		},
		Timestamp: time.Now(),
	}

	response, err := sendTrackEventRequest(inChan, outChan, request)
	if err != nil {
		fmt.Printf("  ✗ Failed to track event: %v\n", err)
		return "", 0
	}

	if response.Success {
		fmt.Printf("  ✓ Event tracked successfully: %s (total: %d)\n", response.EventID, response.TotalCount)
		return response.EventID, response.TotalCount
	}

	fmt.Printf("  ✗ Event tracking failed\n")
	return "", 0
}

func trackMultipleEvents(inChan, outChan chan *mono.Msg) ([]string, int) {
	events := []analytics.TrackEventRequest{
		{
			EventType: "button_click",
			UserID:    "user456",
			Properties: map[string]any{
				"button": "subscribe",
				"page":   "/pricing",
			},
			Timestamp: time.Now(),
		},
		{
			EventType: "form_submit",
			UserID:    "user789",
			Properties: map[string]any{
				"form": "contact",
			},
			Timestamp: time.Now(),
		},
		{
			EventType: "video_play",
			UserID:    "user123",
			Properties: map[string]any{
				"video_id": "intro-tutorial",
			},
			Timestamp: time.Now(),
		},
	}

	var eventIDs []string
	var totalCount int

	for i, request := range events {
		response, err := sendTrackEventRequest(inChan, outChan, request)
		if err != nil {
			fmt.Printf("  ✗ Event %d failed: %v\n", i+1, err)
			continue
		}

		if response.Success {
			fmt.Printf("  ✓ Event %d tracked: %s (type=%s, user=%s, total=%d)\n",
				i+1, response.EventID, request.EventType, request.UserID, response.TotalCount)
			eventIDs = append(eventIDs, response.EventID)
			totalCount = response.TotalCount
		}
	}

	return eventIDs, totalCount
}

func trackConcurrentEvents(inChan, outChan chan *mono.Msg) ([]string, int) {
	const numEvents = 10

	type result struct {
		msg        string
		eventID    string
		totalCount int
	}
	results := make(chan result, numEvents)

	for i := 0; i < numEvents; i++ {
		go func(id int) {
			request := analytics.TrackEventRequest{
				EventType: "concurrent_event",
				UserID:    fmt.Sprintf("user%d", id),
				Properties: map[string]any{
					"worker_id": id,
				},
				Timestamp: time.Now(),
			}

			response, err := sendTrackEventRequest(inChan, outChan, request)
			if err != nil {
				results <- result{msg: fmt.Sprintf("Worker %d: failed (%v)", id, err)}
			} else if response.Success {
				results <- result{
					msg:        fmt.Sprintf("Worker %d: success (%s, total=%d)", id, response.EventID, response.TotalCount),
					eventID:    response.EventID,
					totalCount: response.TotalCount,
				}
			} else {
				results <- result{msg: fmt.Sprintf("Worker %d: failed", id)}
			}
		}(i)
	}

	// Collect results
	var eventIDs []string
	var totalCount int
	for i := 0; i < numEvents; i++ {
		res := <-results
		fmt.Printf("  %s\n", res.msg)
		if res.eventID != "" {
			eventIDs = append(eventIDs, res.eventID)
			// Keep track of the highest total count (server-side cumulative count)
			if res.totalCount > totalCount {
				totalCount = res.totalCount
			}
		}
	}

	return eventIDs, totalCount
}

func trackInvalidEvent(inChan, outChan chan *mono.Msg) {
	// Send event with missing required fields
	request := analytics.TrackEventRequest{
		EventType: "", // Missing event_type
		UserID:    "user999",
		Properties: map[string]any{
			"test": true,
		},
		Timestamp: time.Now(),
	}

	response, err := sendTrackEventRequest(inChan, outChan, request)
	if err != nil {
		fmt.Printf("  → Invalid event rejected (error: %v)\n", err)
		return
	}

	if !response.Success {
		fmt.Printf("  → Invalid event rejected gracefully\n")
	}
}

// retrieveEventsViaAdapter demonstrates using the adapter pattern to retrieve
// events via the request-reply service. It retrieves up to 3 events from the
// provided eventIDs slice and tests retrieval of a non-existent event.
func retrieveEventsViaAdapter(ctx context.Context, services mono.ServiceContainer, eventIDs []string) {
	// Create adapter using the analytics service container
	adapter := analytics.NewAnalyticsAdapter(services)

	// Retrieve a few events to demonstrate the adapter pattern
	numToRetrieve := 3
	if len(eventIDs) < numToRetrieve {
		numToRetrieve = len(eventIDs)
	}

	if numToRetrieve == 0 {
		fmt.Println("  No events available to retrieve")
		return
	}

	fmt.Printf("  Retrieving %d events using adapter pattern...\n", numToRetrieve)
	for i := 0; i < numToRetrieve; i++ {
		eventID := eventIDs[i]

		// Use the adapter to get the event
		req := &analytics.GetEventRequest{
			EventID: eventID,
		}

		resp, err := adapter.GetEvent(ctx, req)
		if err != nil {
			fmt.Printf("  ✗ Failed to retrieve event %s: %v\n", eventID, err)
			continue
		}

		if resp.Found {
			fmt.Printf("  ✓ Event %s retrieved: type=%s, user=%s, recorded=%s\n",
				resp.EventID, resp.EventType, resp.UserID, resp.RecordedAt.Format(time.RFC3339))
		} else {
			fmt.Printf("  ✗ Event %s not found\n", eventID)
		}
	}

	// Try to retrieve a non-existent event
	fmt.Println("\n  Testing retrieval of non-existent event...")
	resp, err := adapter.GetEvent(ctx, &analytics.GetEventRequest{
		EventID: "non-existent-event-id",
	})
	if err != nil {
		fmt.Printf("  ✗ Error: %v\n", err)
	} else if !resp.Found {
		fmt.Printf("  ✓ Non-existent event correctly returned as not found\n")
	}
}

func sendTrackEventRequest(inChan, outChan chan *mono.Msg, request analytics.TrackEventRequest) (*analytics.TrackEventResponse, error) {
	// Marshal request
	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create request message
	reqMsg := &mono.Msg{
		Subject: "analytics.track_event",
		Reply:   "response.inbox",
		Data:    requestData,
	}

	// Send request via channel
	select {
	case inChan <- reqMsg:
		// Request sent successfully
	case <-time.After(1 * time.Second):
		return nil, fmt.Errorf("timeout sending request")
	}

	// Receive response via channel
	var responseMsg *mono.Msg
	select {
	case responseMsg = <-outChan:
		// Response received successfully
	case <-time.After(2 * time.Second):
		return nil, fmt.Errorf("timeout receiving response")
	}

	// Parse response
	var response analytics.TrackEventResponse
	if err := json.Unmarshal(responseMsg.Data, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &response, nil
}
