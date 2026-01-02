//go:build integration
// +build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1"
	"github.com/nats-io/nats.go"
)

// clusterTestModule implements a module for cluster integration testing
type clusterTestModule struct {
	name                 string
	eventBus             mono.EventBus
	container            mono.ServiceContainer
	registerServicesFunc func(mono.ServiceContainer) error
	mu                   sync.Mutex
}

func (m *clusterTestModule) Name() string {
	return m.name
}

func (m *clusterTestModule) Dependencies() []string {
	return nil
}

func (m *clusterTestModule) Start(ctx context.Context) error {
	return nil
}

func (m *clusterTestModule) Stop(ctx context.Context) error {
	return nil
}

func (m *clusterTestModule) SetEventBus(eventBus mono.EventBus) {
	m.eventBus = eventBus
}

func (m *clusterTestModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container
	if m.registerServicesFunc != nil {
		return m.registerServicesFunc(container)
	}
	return nil
}

// TestNATSCluster_TwoNodes tests a 2-node NATS cluster setup
func TestNATSCluster_TwoNodes(t *testing.T) {
	ctx := context.Background()

	// Setup 2-node cluster
	framework1, framework2, cleanup := setupTwoNodeCluster(t, "test-2node-cluster", 14222, 16222)
	defer cleanup()

	// Start first node
	if err := framework1.Start(ctx); err != nil {
		t.Fatalf("failed to start framework1: %v", err)
	}

	// Wait for seed node to be ready
	time.Sleep(500 * time.Millisecond)

	// Start second node
	if err := framework2.Start(ctx); err != nil {
		t.Fatalf("failed to start framework2: %v", err)
	}

	// Wait for cluster to form
	time.Sleep(1 * time.Second)

	// Get NATS connections from both frameworks
	conn1, err := getNATSConnection(14222)
	if err != nil {
		t.Fatalf("failed to get connection from framework1: %v", err)
	}
	defer conn1.Close()

	conn2, err := getNATSConnection(14223)
	if err != nil {
		t.Fatalf("failed to get connection from framework2: %v", err)
	}
	defer conn2.Close()

	// Verify both connections are working
	if !conn1.IsConnected() {
		t.Fatal("application1 NATS connection is not connected")
	}
	if !conn2.IsConnected() {
		t.Fatal("application2 NATS connection is not connected")
	}

	t.Log("2-node cluster formed successfully")
}

// TestNATSCluster_ThreeNodes tests a 3-node NATS cluster setup
func TestNATSCluster_ThreeNodes(t *testing.T) {
	ctx := context.Background()

	// Setup 3-node cluster
	framework1, framework2, framework3, cleanup := setupThreeNodeCluster(t, "test-3node-cluster", 14322, 16322)
	defer cleanup()

	// Start first node
	if err := framework1.Start(ctx); err != nil {
		t.Fatalf("failed to start framework1: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Start second node
	if err := framework2.Start(ctx); err != nil {
		t.Fatalf("failed to start framework2: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Start third node
	if err := framework3.Start(ctx); err != nil {
		t.Fatalf("failed to start framework3: %v", err)
	}

	// Wait for cluster to form
	time.Sleep(1 * time.Second)

	// Verify all nodes are connected
	for i, port := range []int{14322, 14323, 14324} {
		conn, err := getNATSConnection(port)
		if err != nil {
			t.Fatalf("failed to get connection from framework%d: %v", i+1, err)
		}
		if !conn.IsConnected() {
			t.Fatalf("application%d NATS connection is not connected", i+1)
		}
		conn.Close()
	}

	t.Log("3-node cluster formed successfully")
}

// TestNATSCluster_PubSubAcrossNodes tests pub/sub communication across cluster nodes using EventBus
func TestNATSCluster_PubSubAcrossNodes(t *testing.T) {
	ctx := context.Background()

	// Shared state for message verification
	var receivedMsg atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)

	// Create module for node 1 (publisher)
	module1 := &clusterTestModule{
		name: "publisher-module",
	}

	// Create module for node 2 (subscriber) - will subscribe in Start hook
	module2 := &clusterTestModule{
		name: "subscriber-module",
	}

	// Setup 2-node cluster
	framework1, framework2, cleanup := setupTwoNodeCluster(t, "pubsub-cluster", 14422, 16422)
	defer cleanup()

	// Register modules
	if err := framework1.Register(module1); err != nil {
		t.Fatalf("failed to register module1: %v", err)
	}
	if err := framework2.Register(module2); err != nil {
		t.Fatalf("failed to register module2: %v", err)
	}

	// Start both frameworks
	if err := framework1.Start(ctx); err != nil {
		t.Fatalf("failed to start framework1: %v", err)
	}
	defer framework1.Stop(ctx)

	if err := framework2.Start(ctx); err != nil {
		t.Fatalf("failed to start framework2: %v", err)
	}
	defer framework2.Stop(ctx)

	// Wait for cluster to form
	time.Sleep(1 * time.Second)

	// Subscribe on node 2 using EventBus
	_, err := module2.eventBus.Subscribe("test.subject", func(_ context.Context, msg *mono.Msg) {
		receivedMsg.Store(string(msg.Data))
		wg.Done()
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Wait for subscription to propagate across cluster
	time.Sleep(500 * time.Millisecond)

	// Publish from node 1 using EventBus
	testMessage := []byte("Hello from node1 to node2")
	if err := module1.eventBus.Publish("test.subject", testMessage); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Wait for message to be received (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message across cluster")
	}

	// Verify message content
	received := receivedMsg.Load()
	if received == nil {
		t.Fatal("no message received")
	}
	if received.(string) != string(testMessage) {
		t.Errorf("expected message %q, got %q", string(testMessage), received.(string))
	}

	t.Log("Pub/sub across cluster nodes successful")
}

// TestNATSCluster_RequestReplyAcrossNodes tests request-reply across cluster nodes using RequestReply services
func TestNATSCluster_RequestReplyAcrossNodes(t *testing.T) {
	ctx := context.Background()

	// Shared counter to track which node handled the request
	var node1Handled, node2Handled atomic.Bool

	// Create modules with SAME name on both nodes so they share the same service namespace
	// Both nodes register the service to test cluster-wide load balancing
	module1 := &clusterTestModule{
		name: "echo-module",
		registerServicesFunc: func(container mono.ServiceContainer) error {
			// Register a RequestReply service that will respond to requests
			return container.RegisterRequestReplyService("echo-service", func(_ context.Context, req *mono.Msg) ([]byte, error) {
				node1Handled.Store(true)
				response := fmt.Sprintf("Echo from node1: %s", string(req.Data))
				return []byte(response), nil
			})
		},
	}

	module2 := &clusterTestModule{
		name: "echo-module",
		registerServicesFunc: func(container mono.ServiceContainer) error {
			// Register a RequestReply service that will respond to requests
			return container.RegisterRequestReplyService("echo-service", func(_ context.Context, req *mono.Msg) ([]byte, error) {
				node2Handled.Store(true)
				response := fmt.Sprintf("Echo from node2: %s", string(req.Data))
				return []byte(response), nil
			})
		},
	}

	// Setup 2-node cluster
	framework1, framework2, cleanup := setupTwoNodeCluster(t, "reqreply-cluster", 14522, 16522)
	defer cleanup()

	// Register modules
	if err := framework1.Register(module1); err != nil {
		t.Fatalf("failed to register module1: %v", err)
	}
	if err := framework2.Register(module2); err != nil {
		t.Fatalf("failed to register module2: %v", err)
	}

	// Start both frameworks
	if err := framework1.Start(ctx); err != nil {
		t.Fatalf("failed to start framework1: %v", err)
	}
	defer framework1.Stop(ctx)

	if err := framework2.Start(ctx); err != nil {
		t.Fatalf("failed to start framework2: %v", err)
	}
	defer framework2.Stop(ctx)

	// Wait for cluster and subscriptions to stabilize
	time.Sleep(1 * time.Second)

	// Get the service client from module1's container to send request
	serviceClient, err := module1.container.GetRequestReplyService("echo-service")
	if err != nil {
		t.Fatalf("failed to get request-reply service: %v", err)
	}

	// Send request from node 1
	testRequest := []byte("ping")
	response, err := serviceClient.Call(ctx, testRequest)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	// Verify response came from one of the nodes
	responseStr := string(response.Data)
	if !strings.Contains(responseStr, "Echo from node") {
		t.Errorf("unexpected response format: %q", responseStr)
	}

	// At least one node should have handled the request
	if !node1Handled.Load() && !node2Handled.Load() {
		t.Error("neither node handled the request")
	}

	t.Logf("Request-reply across cluster successful: %s", responseStr)
}

// TestNATSCluster_MultipleSubscribers tests that multiple subscribers across nodes all receive messages using EventBus
func TestNATSCluster_MultipleSubscribers(t *testing.T) {
	ctx := context.Background()

	// Shared state for message verification
	var node1Received, node2Received, node3Received atomic.Bool
	var wg sync.WaitGroup
	wg.Add(3)

	// Create modules for all nodes
	module1 := &clusterTestModule{
		name: "node1-module",
	}

	module2 := &clusterTestModule{
		name: "node2-module",
	}

	module3 := &clusterTestModule{
		name: "node3-module",
	}

	// Setup 3-node cluster
	framework1, framework2, framework3, cleanup := setupThreeNodeCluster(t, "multi-sub-cluster", 14622, 16622)
	defer cleanup()

	// Register modules
	if err := framework1.Register(module1); err != nil {
		t.Fatalf("failed to register module1: %v", err)
	}
	if err := framework2.Register(module2); err != nil {
		t.Fatalf("failed to register module2: %v", err)
	}
	if err := framework3.Register(module3); err != nil {
		t.Fatalf("failed to register module3: %v", err)
	}

	// Start all frameworks
	if err := framework1.Start(ctx); err != nil {
		t.Fatalf("failed to start framework1: %v", err)
	}
	defer framework1.Stop(ctx)

	if err := framework2.Start(ctx); err != nil {
		t.Fatalf("failed to start framework2: %v", err)
	}
	defer framework2.Stop(ctx)

	if err := framework3.Start(ctx); err != nil {
		t.Fatalf("failed to start framework3: %v", err)
	}
	defer framework3.Stop(ctx)

	// Wait for cluster to stabilize
	time.Sleep(1 * time.Second)

	// Subscribe on all three nodes using EventBus
	_, err := module1.eventBus.Subscribe("broadcast.event", func(_ context.Context, msg *mono.Msg) {
		node1Received.Store(true)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("failed to subscribe on node1: %v", err)
	}

	_, err = module2.eventBus.Subscribe("broadcast.event", func(_ context.Context, msg *mono.Msg) {
		node2Received.Store(true)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("failed to subscribe on node2: %v", err)
	}

	_, err = module3.eventBus.Subscribe("broadcast.event", func(_ context.Context, msg *mono.Msg) {
		node3Received.Store(true)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("failed to subscribe on node3: %v", err)
	}

	// Wait for subscriptions to propagate across cluster
	time.Sleep(500 * time.Millisecond)

	// Publish broadcast event from node 1
	if err := module1.eventBus.Publish("broadcast.event", []byte("broadcast message")); err != nil {
		t.Fatalf("failed to publish broadcast event: %v", err)
	}

	// Wait for all subscribers to receive (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for broadcast to all nodes")
	}

	// Verify all nodes received the message
	if !node1Received.Load() {
		t.Error("node1 did not receive message")
	}
	if !node2Received.Load() {
		t.Error("node2 did not receive message")
	}
	if !node3Received.Load() {
		t.Error("node3 did not receive message")
	}

	t.Log("Broadcast to multiple subscribers across cluster successful")
}

// TestNATSCluster_QueueGroupAcrossNodes tests queue group load balancing across cluster using QueueGroup services
func TestNATSCluster_QueueGroupAcrossNodes(t *testing.T) {
	ctx := context.Background()

	// Shared state for message counting
	var node1Count, node2Count atomic.Int32

	// Create modules with SAME name on both nodes so they share the same service namespace
	// Both register the same service with the same queue group for load balancing
	module1 := &clusterTestModule{
		name: "worker-module",
		registerServicesFunc: func(container mono.ServiceContainer) error {
			return container.RegisterQueueGroupService("work-queue", mono.QGHP{
				QueueGroup: "workers",
				Handler: func(_ context.Context, msg *mono.Msg) error {
					node1Count.Add(1)
					return nil
				},
			})
		},
	}

	// Create module for node 2 (worker)
	module2 := &clusterTestModule{
		name: "worker-module",
		registerServicesFunc: func(container mono.ServiceContainer) error {
			return container.RegisterQueueGroupService("work-queue", mono.QGHP{
				QueueGroup: "workers",
				Handler: func(_ context.Context, msg *mono.Msg) error {
					node2Count.Add(1)
					return nil
				},
			})
		},
	}

	// Setup 2-node cluster
	framework1, framework2, cleanup := setupTwoNodeCluster(t, "queue-cluster", 14722, 16722)
	defer cleanup()

	// Register modules
	if err := framework1.Register(module1); err != nil {
		t.Fatalf("failed to register module1: %v", err)
	}
	if err := framework2.Register(module2); err != nil {
		t.Fatalf("failed to register module2: %v", err)
	}

	// Start both frameworks
	if err := framework1.Start(ctx); err != nil {
		t.Fatalf("failed to start framework1: %v", err)
	}
	defer framework1.Stop(ctx)

	if err := framework2.Start(ctx); err != nil {
		t.Fatalf("failed to start framework2: %v", err)
	}
	defer framework2.Stop(ctx)

	// Wait for cluster and subscriptions to stabilize
	// Give extra time for queue group subscriptions to propagate across cluster
	time.Sleep(2 * time.Second)

	// Get the queue group service client from module1's container to send work
	queueClient, err := module1.container.GetQueueGroupService("work-queue")
	if err != nil {
		t.Fatalf("failed to get queue group service: %v", err)
	}

	// Publish multiple work items
	numMessages := 20
	for i := 0; i < numMessages; i++ {
		if err := queueClient.Send(ctx, []byte(fmt.Sprintf("work-%d", i))); err != nil {
			t.Fatalf("failed to send message %d: %v", i, err)
		}
	}

	// Wait for messages to be processed
	time.Sleep(2 * time.Second)

	// Verify messages were distributed (both nodes should receive some)
	n1Count := node1Count.Load()
	n2Count := node2Count.Load()
	total := n1Count + n2Count

	if total != int32(numMessages) {
		t.Errorf("expected %d messages processed, got %d (node1: %d, node2: %d)",
			numMessages, total, n1Count, n2Count)
	}

	// Both nodes should have received at least one message (load balancing)
	if n1Count == 0 {
		t.Error("node1 did not receive any messages (no load balancing)")
	}
	if n2Count == 0 {
		t.Error("node2 did not receive any messages (no load balancing)")
	}

	t.Logf("Queue group load balancing successful: node1=%d, node2=%d", n1Count, n2Count)
}

// Helper functions

// getNATSConnection creates a connection to a framework's embedded NATS server
// using the port information passed during setup
func getNATSConnection(clientPort int) (*nats.Conn, error) {
	clientURL := fmt.Sprintf("nats://127.0.0.1:%d", clientPort)
	conn, err := nats.Connect(clientURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server at %s: %w", clientURL, err)
	}
	return conn, nil
}

// setupTwoNodeCluster creates a 2-node cluster for testing (frameworks not started)
func setupTwoNodeCluster(t *testing.T, clusterName string, clientPortBase, clusterPortBase int) (mono.MonoApplication, mono.MonoApplication, func()) {
	t.Helper()
	ctx := context.Background()

	// Create seed node
	framework1, err := mono.NewMonoApplication(
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(clientPortBase),
		mono.WithNATSClustering(clusterName, "127.0.0.1", clusterPortBase, nil),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework1: %v", err)
	}

	// Create second node
	framework2, err := mono.NewMonoApplication(
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(clientPortBase+1),
		mono.WithNATSClustering(clusterName, "127.0.0.1", clusterPortBase+1, []string{fmt.Sprintf("nats://127.0.0.1:%d", clusterPortBase)}),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework2: %v", err)
	}

	cleanup := func() {
		framework2.Stop(ctx)
		framework1.Stop(ctx)
	}

	return framework1, framework2, cleanup
}

// setupThreeNodeCluster creates a 3-node cluster for testing (frameworks not started)
func setupThreeNodeCluster(t *testing.T, clusterName string, clientPortBase, clusterPortBase int) (mono.MonoApplication, mono.MonoApplication, mono.MonoApplication, func()) {
	t.Helper()
	ctx := context.Background()

	// Create seed node
	framework1, err := mono.NewMonoApplication(
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(clientPortBase),
		mono.WithNATSClustering(clusterName, "127.0.0.1", clusterPortBase, nil),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework1: %v", err)
	}

	// Create second node
	framework2, err := mono.NewMonoApplication(
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(clientPortBase+1),
		mono.WithNATSClustering(clusterName, "127.0.0.1", clusterPortBase+1, []string{fmt.Sprintf("nats://127.0.0.1:%d", clusterPortBase)}),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework2: %v", err)
	}

	// Create third node
	framework3, err := mono.NewMonoApplication(
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(clientPortBase+2),
		mono.WithNATSClustering(clusterName, "127.0.0.1", clusterPortBase+2, []string{fmt.Sprintf("nats://127.0.0.1:%d", clusterPortBase)}),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework3: %v", err)
	}

	cleanup := func() {
		framework3.Stop(ctx)
		framework2.Stop(ctx)
		framework1.Stop(ctx)
	}

	return framework1, framework2, framework3, cleanup
}

// createNATSConfigFile creates a NATS server config file in the given directory
func createNATSConfigFile(dir, filename string, clientPort, clusterPort int, clusterName string, routes []string) (string, error) {
	configPath := filepath.Join(dir, filename)

	// Build routes section
	routesSection := ""
	if len(routes) > 0 {
		routesSection = "\n    routes = [\n"
		for _, route := range routes {
			routesSection += fmt.Sprintf("      \"%s\"\n", route)
		}
		routesSection += "    ]"
	}

	configContent := fmt.Sprintf(`# NATS Server Config for testing
host: "127.0.0.1"
port: %d

cluster {
    name: "%s"
    host: "127.0.0.1"
    port: %d%s
}
`, clientPort, clusterName, clusterPort, routesSection)

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		return "", fmt.Errorf("failed to write config file: %w", err)
	}

	return configPath, nil
}

// setupTwoNodeClusterWithConfigFile creates a 2-node cluster using NATS config files
func setupTwoNodeClusterWithConfigFile(t *testing.T, clusterName string, clientPortBase, clusterPortBase int) (mono.MonoApplication, mono.MonoApplication, func()) {
	t.Helper()
	ctx := context.Background()

	// Create temp directory for config files
	tempDir, err := os.MkdirTemp("", "nats-config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}

	// Create config file for seed node
	config1Path, err := createNATSConfigFile(tempDir, "node1.conf", clientPortBase, clusterPortBase, clusterName, nil)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create config file 1: %v", err)
	}

	// Create config file for second node with route to seed
	config2Path, err := createNATSConfigFile(tempDir, "node2.conf", clientPortBase+1, clusterPortBase+1, clusterName,
		[]string{fmt.Sprintf("nats://127.0.0.1:%d", clusterPortBase)})
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create config file 2: %v", err)
	}

	// Create seed node using config file
	framework1, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile(config1Path),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create framework1: %v", err)
	}

	// Create second node using config file
	framework2, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile(config2Path),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create framework2: %v", err)
	}

	cleanup := func() {
		framework2.Stop(ctx)
		framework1.Stop(ctx)
		os.RemoveAll(tempDir)
	}

	return framework1, framework2, cleanup
}

// TestNATSCluster_WithConfigFile tests cluster setup using NATS config files
func TestNATSCluster_WithConfigFile(t *testing.T) {
	ctx := context.Background()

	// Shared state for message verification
	var receivedMsg atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)

	// Create module for node 1 (publisher)
	module1 := &clusterTestModule{
		name: "publisher-module",
	}

	// Create module for node 2 (subscriber)
	module2 := &clusterTestModule{
		name: "subscriber-module",
	}

	// Setup 2-node cluster using config files
	framework1, framework2, cleanup := setupTwoNodeClusterWithConfigFile(t, "config-file-cluster", 14822, 16822)
	defer cleanup()

	// Register modules
	if err := framework1.Register(module1); err != nil {
		t.Fatalf("failed to register module1: %v", err)
	}
	if err := framework2.Register(module2); err != nil {
		t.Fatalf("failed to register module2: %v", err)
	}

	// Start seed node
	// Note: cleanup() handles stopping frameworks, no need for explicit defer Stop()
	if err := framework1.Start(ctx); err != nil {
		t.Fatalf("failed to start framework1: %v", err)
	}

	// Wait for seed node to be ready
	time.Sleep(500 * time.Millisecond)

	// Start second node
	if err := framework2.Start(ctx); err != nil {
		t.Fatalf("failed to start framework2: %v", err)
	}

	// Wait for cluster to form
	time.Sleep(1 * time.Second)

	// Subscribe on node 2 using EventBus
	_, err := module2.eventBus.Subscribe("test.config.subject", func(_ context.Context, msg *mono.Msg) {
		receivedMsg.Store(string(msg.Data))
		wg.Done()
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Wait for subscription to propagate across cluster
	time.Sleep(500 * time.Millisecond)

	// Publish from node 1 using EventBus
	testMessage := []byte("Hello from config-file cluster")
	if err := module1.eventBus.Publish("test.config.subject", testMessage); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Wait for message to be received (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message across config-file cluster")
	}

	// Verify message content
	received := receivedMsg.Load()
	if received == nil {
		t.Fatal("no message received")
	}
	if received.(string) != string(testMessage) {
		t.Errorf("expected message %q, got %q", string(testMessage), received.(string))
	}

	t.Log("Config file cluster pub/sub successful")
}

// TestNATSCluster_ConfigFileWithOverride tests that programmatic options override config file settings
func TestNATSCluster_ConfigFileWithOverride(t *testing.T) {
	ctx := context.Background()

	// Create temp directory for config files
	tempDir, err := os.MkdirTemp("", "nats-override-test-*")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create config file with port 14900 (will be overridden to 14922)
	configPath := filepath.Join(tempDir, "override-test.conf")
	configContent := `# Config file with port that will be overridden
host: "127.0.0.1"
port: 14900
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	// Create framework with config file AND port override
	framework, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile(configPath),
		mono.WithNATSPort(14922), // Override the port from config file
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework: %v", err)
	}

	// Start framework
	if err := framework.Start(ctx); err != nil {
		t.Fatalf("failed to start framework: %v", err)
	}
	defer framework.Stop(ctx)

	// Try to connect to the overridden port (14922), not the config file port (14900)
	conn, err := getNATSConnection(14922)
	if err != nil {
		t.Fatalf("failed to connect to overridden port 14922: %v", err)
	}
	defer conn.Close()

	if !conn.IsConnected() {
		t.Fatal("NATS connection is not connected on overridden port")
	}

	// Verify we cannot connect to the original config file port
	_, err = getNATSConnection(14900)
	if err == nil {
		t.Fatal("should not be able to connect to original config file port 14900")
	}

	t.Log("Config file with programmatic override successful - port correctly overridden")
}

// TestNATSCluster_ConfigFileWithJetStream tests JetStream enabled via config file
func TestNATSCluster_ConfigFileWithJetStream(t *testing.T) {
	ctx := context.Background()

	// Create temp directory for config and JetStream data
	tempDir, err := os.MkdirTemp("", "nats-jetstream-config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	jsDir := filepath.Join(tempDir, "jetstream")
	if err := os.MkdirAll(jsDir, 0755); err != nil {
		t.Fatalf("failed to create jetstream directory: %v", err)
	}

	// Create config file with JetStream enabled
	configPath := filepath.Join(tempDir, "jetstream-test.conf")
	configContent := fmt.Sprintf(`# Config file with JetStream
host: "127.0.0.1"
port: 15022

jetstream {
    store_dir: "%s"
    max_mem: 64MB
    max_file: 128MB
}
`, jsDir)
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	// Create framework with config file
	framework, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile(configPath),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework: %v", err)
	}

	// Start framework
	if err := framework.Start(ctx); err != nil {
		t.Fatalf("failed to start framework: %v", err)
	}
	defer framework.Stop(ctx)

	// Connect and verify JetStream is available
	conn, err := getNATSConnection(15022)
	if err != nil {
		t.Fatalf("failed to connect to NATS: %v", err)
	}
	defer conn.Close()

	// Create JetStream context
	js, err := conn.JetStream()
	if err != nil {
		t.Fatalf("failed to get JetStream context: %v", err)
	}

	// Try to create a stream to verify JetStream is working
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST_STREAM",
		Subjects: []string{"test.>"},
		Storage:  nats.FileStorage,
	})
	if err != nil {
		t.Fatalf("failed to create stream (JetStream not working): %v", err)
	}

	// Clean up stream
	if err := js.DeleteStream("TEST_STREAM"); err != nil {
		t.Logf("warning: failed to delete test stream: %v", err)
	}

	t.Log("Config file with JetStream successful")
}

// TestNATSCluster_ConfigFileWithMaxPayload tests MaxPayload setting from config file
func TestNATSCluster_ConfigFileWithMaxPayload(t *testing.T) {
	ctx := context.Background()

	// Create temp directory for config files
	tempDir, err := os.MkdirTemp("", "nats-maxpayload-config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create config file with custom max_payload (2MB)
	configPath := filepath.Join(tempDir, "maxpayload-test.conf")
	configContent := `# Config file with custom max_payload
host: "127.0.0.1"
port: 15122
max_payload: 2097152
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	// Create framework with config file (no programmatic MaxPayload override)
	framework, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile(configPath),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework: %v", err)
	}

	// Start framework
	if err := framework.Start(ctx); err != nil {
		t.Fatalf("failed to start framework: %v", err)
	}
	defer framework.Stop(ctx)

	// Connect to NATS
	conn, err := getNATSConnection(15122)
	if err != nil {
		t.Fatalf("failed to connect to NATS: %v", err)
	}
	defer conn.Close()

	// Create a message larger than 1MB but smaller than 2MB
	// This should succeed if max_payload from config file is applied
	largePayload := make([]byte, 1500000) // 1.5MB
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	// Subscribe to receive the message
	received := make(chan []byte, 1)
	sub, err := conn.Subscribe("test.large.payload", func(msg *nats.Msg) {
		received <- msg.Data
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Publish large message
	if err := conn.Publish("test.large.payload", largePayload); err != nil {
		t.Fatalf("failed to publish large message (max_payload from config file not applied?): %v", err)
	}
	conn.Flush()

	// Wait for message
	select {
	case data := <-received:
		if len(data) != len(largePayload) {
			t.Errorf("received message size mismatch: got %d, want %d", len(data), len(largePayload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for large message")
	}

	t.Log("Config file with MaxPayload successful - large message sent/received")
}

// TestNATSCluster_ThreeNodesWithConfigFile tests a 3-node cluster setup using config files
func TestNATSCluster_ThreeNodesWithConfigFile(t *testing.T) {
	ctx := context.Background()

	// Create temp directory for config files
	tempDir, err := os.MkdirTemp("", "nats-3node-config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	clusterName := "three-node-config-cluster"
	clientPortBase := 15222
	clusterPortBase := 17222

	// Create config files for all three nodes
	config1Path, err := createNATSConfigFile(tempDir, "node1.conf", clientPortBase, clusterPortBase, clusterName, nil)
	if err != nil {
		t.Fatalf("failed to create config file 1: %v", err)
	}

	config2Path, err := createNATSConfigFile(tempDir, "node2.conf", clientPortBase+1, clusterPortBase+1, clusterName,
		[]string{fmt.Sprintf("nats://127.0.0.1:%d", clusterPortBase)})
	if err != nil {
		t.Fatalf("failed to create config file 2: %v", err)
	}

	config3Path, err := createNATSConfigFile(tempDir, "node3.conf", clientPortBase+2, clusterPortBase+2, clusterName,
		[]string{fmt.Sprintf("nats://127.0.0.1:%d", clusterPortBase)})
	if err != nil {
		t.Fatalf("failed to create config file 3: %v", err)
	}

	// Create all three frameworks
	framework1, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile(config1Path),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework1: %v", err)
	}

	framework2, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile(config2Path),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework2: %v", err)
	}

	framework3, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile(config3Path),
		mono.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create framework3: %v", err)
	}

	// Start all nodes sequentially
	// Note: Test cleans up frameworks on exit; no explicit defer Stop() needed
	if err := framework1.Start(ctx); err != nil {
		t.Fatalf("failed to start framework1: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := framework2.Start(ctx); err != nil {
		framework1.Stop(ctx)
		t.Fatalf("failed to start framework2: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := framework3.Start(ctx); err != nil {
		framework2.Stop(ctx)
		framework1.Stop(ctx)
		t.Fatalf("failed to start framework3: %v", err)
	}

	// Cleanup at test end
	defer func() {
		framework3.Stop(ctx)
		framework2.Stop(ctx)
		framework1.Stop(ctx)
	}()

	// Wait for cluster to form
	time.Sleep(1 * time.Second)

	// Verify all nodes are connected
	for i, port := range []int{clientPortBase, clientPortBase + 1, clientPortBase + 2} {
		conn, err := getNATSConnection(port)
		if err != nil {
			t.Fatalf("failed to connect to node%d on port %d: %v", i+1, port, err)
		}
		if !conn.IsConnected() {
			t.Fatalf("node%d connection is not connected", i+1)
		}
		conn.Close()
	}

	// Test pub/sub across all three nodes
	var receivedCount atomic.Int32
	var wg sync.WaitGroup
	wg.Add(3)

	// Subscribe on all three nodes
	for i, port := range []int{clientPortBase, clientPortBase + 1, clientPortBase + 2} {
		conn, err := getNATSConnection(port)
		if err != nil {
			t.Fatalf("failed to connect to node%d: %v", i+1, err)
		}
		defer conn.Close()

		_, err = conn.Subscribe("test.broadcast.3node", func(msg *nats.Msg) {
			receivedCount.Add(1)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("failed to subscribe on node%d: %v", i+1, err)
		}
	}

	// Wait for subscriptions to propagate
	time.Sleep(500 * time.Millisecond)

	// Publish from node 1
	conn1, err := getNATSConnection(clientPortBase)
	if err != nil {
		t.Fatalf("failed to connect to node1 for publish: %v", err)
	}
	defer conn1.Close()

	if err := conn1.Publish("test.broadcast.3node", []byte("broadcast to 3 nodes")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
	conn1.Flush()

	// Wait for all messages
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout: only received %d/3 messages", receivedCount.Load())
	}

	t.Log("3-node cluster with config files successful")
}

// TestNATSCluster_ConfigFileNotFound tests error handling for missing config file
func TestNATSCluster_ConfigFileNotFound(t *testing.T) {
	// Create framework with non-existent config file
	// Note: NATS server starts during framework creation, so config file errors
	// are caught during NewMonoApplication(), not during Start()
	_, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile("/nonexistent/path/to/config.conf"),
		mono.WithShutdownTimeout(5*time.Second),
	)

	// Framework creation should fail with config file error
	if err == nil {
		t.Fatal("expected error when creating framework with non-existent config file")
	}

	// Verify error message mentions config file
	if !strings.Contains(err.Error(), "config") {
		t.Errorf("error should mention config file, got: %v", err)
	}

	t.Logf("Config file not found error handled correctly: %v", err)
}

// TestNATSCluster_ConfigFileInvalid tests error handling for invalid config file
func TestNATSCluster_ConfigFileInvalid(t *testing.T) {
	// Create temp directory for config files
	tempDir, err := os.MkdirTemp("", "nats-invalid-config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create invalid config file
	configPath := filepath.Join(tempDir, "invalid.conf")
	invalidContent := `# Invalid NATS config
this is not valid: {{{ syntax
port: not_a_number
`
	if err := os.WriteFile(configPath, []byte(invalidContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	// Create framework with invalid config file
	// Note: NATS server starts during framework creation, so config file errors
	// are caught during NewMonoApplication(), not during Start()
	_, err = mono.NewMonoApplication(
		mono.WithNATSConfigFile(configPath),
		mono.WithShutdownTimeout(5*time.Second),
	)

	// Framework creation should fail with config parse error
	if err == nil {
		t.Fatal("expected error when creating framework with invalid config file")
	}

	t.Logf("Invalid config file error handled correctly: %v", err)
}

// TestNATSCluster_ConfigFileQueueGroupAcrossNodes tests queue group load balancing with config file cluster
func TestNATSCluster_ConfigFileQueueGroupAcrossNodes(t *testing.T) {
	ctx := context.Background()

	// Shared state for message counting
	var node1Count, node2Count atomic.Int32

	// Create modules with SAME name for queue group load balancing
	module1 := &clusterTestModule{
		name: "worker-module",
		registerServicesFunc: func(container mono.ServiceContainer) error {
			return container.RegisterQueueGroupService("config-work-queue", mono.QGHP{
				QueueGroup: "config-workers",
				Handler: func(_ context.Context, msg *mono.Msg) error {
					node1Count.Add(1)
					return nil
				},
			})
		},
	}

	module2 := &clusterTestModule{
		name: "worker-module",
		registerServicesFunc: func(container mono.ServiceContainer) error {
			return container.RegisterQueueGroupService("config-work-queue", mono.QGHP{
				QueueGroup: "config-workers",
				Handler: func(_ context.Context, msg *mono.Msg) error {
					node2Count.Add(1)
					return nil
				},
			})
		},
	}

	// Setup 2-node cluster using config files
	framework1, framework2, cleanup := setupTwoNodeClusterWithConfigFile(t, "config-queue-cluster", 15322, 17322)
	defer cleanup()

	// Register modules
	if err := framework1.Register(module1); err != nil {
		t.Fatalf("failed to register module1: %v", err)
	}
	if err := framework2.Register(module2); err != nil {
		t.Fatalf("failed to register module2: %v", err)
	}

	// Start both frameworks
	// Note: cleanup() handles stopping frameworks, no need for explicit defer Stop()
	if err := framework1.Start(ctx); err != nil {
		t.Fatalf("failed to start framework1: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := framework2.Start(ctx); err != nil {
		t.Fatalf("failed to start framework2: %v", err)
	}

	// Wait for cluster and subscriptions to stabilize
	time.Sleep(2 * time.Second)

	// Get queue group service client
	queueClient, err := module1.container.GetQueueGroupService("config-work-queue")
	if err != nil {
		t.Fatalf("failed to get queue group service: %v", err)
	}

	// Publish multiple work items
	numMessages := 20
	for i := 0; i < numMessages; i++ {
		if err := queueClient.Send(ctx, []byte(fmt.Sprintf("config-work-%d", i))); err != nil {
			t.Fatalf("failed to send message %d: %v", i, err)
		}
	}

	// Wait for messages to be processed
	time.Sleep(2 * time.Second)

	// Verify messages were distributed
	n1Count := node1Count.Load()
	n2Count := node2Count.Load()
	total := n1Count + n2Count

	if total != int32(numMessages) {
		t.Errorf("expected %d messages processed, got %d (node1: %d, node2: %d)",
			numMessages, total, n1Count, n2Count)
	}

	// Both nodes should have received at least one message
	if n1Count == 0 {
		t.Error("node1 did not receive any messages (no load balancing)")
	}
	if n2Count == 0 {
		t.Error("node2 did not receive any messages (no load balancing)")
	}

	t.Logf("Config file queue group load balancing successful: node1=%d, node2=%d", n1Count, n2Count)
}
