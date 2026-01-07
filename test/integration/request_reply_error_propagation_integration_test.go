//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
)

// randomPort returns a random port in the valid NATS port range (1024-65535).
// Using high ports (49152-65535) to avoid conflicts with well-known services.
func randomPort() int {
	return 49152 + rand.Intn(65535-49152)
}

// TestRequestReplyService_HandlerErrorPropagation tests that handler errors
// are properly propagated back to the client instead of causing timeouts.
func TestRequestReplyService_HandlerErrorPropagation(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that returns an error from its handler
	responder := &testModule{name: "error-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("failing-service", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return nil, errors.New("intentional test error: database connection failed")
		})
	}

	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register responder: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give the subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Verify container is available
	if responder.container == nil {
		t.Fatal("Responder container is nil")
	}

	// Get the request-reply service client
	client, err := responder.container.GetRequestReplyService("failing-service")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request - should receive error, not timeout
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	_, err = client.Call(reqCtx, []byte("test request"))

	// Verify we got an error
	if err == nil {
		t.Fatal("Expected error from handler, got nil")
	}

	// Verify it's a RemoteError
	if !monoerrors.IsRemoteError(err) {
		t.Errorf("Expected RemoteError, got %T: %v", err, err)
	}

	// Verify error message is preserved
	remoteErr, ok := monoerrors.GetRemoteError(err)
	if !ok {
		t.Fatal("Failed to extract RemoteError")
	}

	expectedMsg := "intentional test error: database connection failed"
	if remoteErr.Message != expectedMsg {
		t.Errorf("Expected message %q, got %q", expectedMsg, remoteErr.Message)
	}

	// Verify service metadata
	if remoteErr.ServiceName != "failing-service" {
		t.Errorf("Expected ServiceName 'failing-service', got %q", remoteErr.ServiceName)
	}
	if remoteErr.ModuleName != "error-responder" {
		t.Errorf("Expected ModuleName 'error-responder', got %q", remoteErr.ModuleName)
	}
}

// TestRequestReplyService_NormalResponseStillWorks tests that normal (non-error)
// responses still work correctly after adding error propagation support.
func TestRequestReplyService_NormalResponseStillWorks(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that returns a successful response
	responder := &testModule{name: "success-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("echo-service", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return []byte("echo: " + string(req.Data)), nil
		})
	}

	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register responder: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give the subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the request-reply service client
	client, err := responder.container.GetRequestReplyService("echo-service")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	response, err := client.Call(reqCtx, []byte("hello world"))

	// Verify we got a successful response
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "echo: hello world"
	if string(response.Data) != expected {
		t.Errorf("Expected %q, got %q", expected, string(response.Data))
	}
}

// TestRequestReplyService_ErrorWithCallMsg tests error propagation with CallMsg method.
func TestRequestReplyService_ErrorWithCallMsg(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that returns an error
	responder := &testModule{name: "error-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("validate-service", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return nil, errors.New("validation failed: missing required field")
		})
	}

	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register responder: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give the subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the request-reply service client
	client, err := responder.container.GetRequestReplyService("validate-service")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request using CallMsg
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	inputMsg := &mono.Msg{
		Data:   []byte("test data"),
		Header: map[string][]string{"X-Custom-Header": {"test-value"}},
	}

	_, err = client.CallMsg(reqCtx, inputMsg)

	// Verify we got an error
	if err == nil {
		t.Fatal("Expected error from handler, got nil")
	}

	// Verify it's a RemoteError
	if !monoerrors.IsRemoteError(err) {
		t.Errorf("Expected RemoteError, got %T: %v", err, err)
	}

	// Verify error message
	remoteErr, ok := monoerrors.GetRemoteError(err)
	if !ok {
		t.Fatal("Failed to extract RemoteError")
	}

	if remoteErr.Message != "validation failed: missing required field" {
		t.Errorf("Expected specific error message, got %q", remoteErr.Message)
	}
}

// TestRequestReplyService_ErrorDoesNotTimeout tests that errors are returned
// quickly instead of waiting for timeout.
func TestRequestReplyService_ErrorDoesNotTimeout(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that returns an error immediately
	responder := &testModule{name: "fast-error-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("fast-fail", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return nil, errors.New("immediate failure")
		})
	}

	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register responder: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give the subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the request-reply service client
	client, err := responder.container.GetRequestReplyService("fast-fail")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request with a long timeout - if error propagation works,
	// we should get the error back quickly (not wait for timeout)
	reqCtx, reqCancel := context.WithTimeout(ctx, 30*time.Second)
	defer reqCancel()

	start := time.Now()
	_, err = client.Call(reqCtx, []byte("test"))
	elapsed := time.Since(start)

	// Verify we got an error
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Verify error was returned quickly (much less than timeout)
	if elapsed > 5*time.Second {
		t.Errorf("Error took too long to propagate: %v (should be < 5s)", elapsed)
	}

	// Verify it's a RemoteError (not a timeout error)
	if !monoerrors.IsRemoteError(err) {
		t.Errorf("Expected RemoteError, got %T: %v", err, err)
	}

	// Ensure it's NOT a timeout error
	if monoerrors.IsTimeoutError(err) {
		t.Error("Error should be RemoteError, not TimeoutError")
	}
}

// =============================================================================
// TCP Connection Tests
// These tests verify error propagation over actual TCP connections.
// =============================================================================

// TestRequestReplyService_HandlerErrorPropagation_OverTCP tests that handler errors
// are properly propagated back to the client over TCP connections.
func TestRequestReplyService_HandlerErrorPropagation_OverTCP(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithNATSPort(randomPort()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that returns an error from its handler
	responder := &testModule{name: "error-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("failing-service", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return nil, errors.New("intentional test error: database connection failed")
		})
	}

	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register responder: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give the subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Verify container is available
	if responder.container == nil {
		t.Fatal("Responder container is nil")
	}

	// Get the request-reply service client
	client, err := responder.container.GetRequestReplyService("failing-service")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request - should receive error, not timeout
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	_, err = client.Call(reqCtx, []byte("test request"))

	// Verify we got an error
	if err == nil {
		t.Fatal("Expected error from handler, got nil")
	}

	// Verify it's a RemoteError
	if !monoerrors.IsRemoteError(err) {
		t.Errorf("Expected RemoteError, got %T: %v", err, err)
	}

	// Verify error message is preserved
	remoteErr, ok := monoerrors.GetRemoteError(err)
	if !ok {
		t.Fatal("Failed to extract RemoteError")
	}

	expectedMsg := "intentional test error: database connection failed"
	if remoteErr.Message != expectedMsg {
		t.Errorf("Expected message %q, got %q", expectedMsg, remoteErr.Message)
	}

	// Verify service metadata
	if remoteErr.ServiceName != "failing-service" {
		t.Errorf("Expected ServiceName 'failing-service', got %q", remoteErr.ServiceName)
	}
	if remoteErr.ModuleName != "error-responder" {
		t.Errorf("Expected ModuleName 'error-responder', got %q", remoteErr.ModuleName)
	}
}

// TestRequestReplyService_NormalResponseStillWorks_OverTCP tests that normal (non-error)
// responses still work correctly over TCP connections.
func TestRequestReplyService_NormalResponseStillWorks_OverTCP(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithNATSPort(randomPort()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that returns a successful response
	responder := &testModule{name: "success-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("echo-service", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return []byte("echo: " + string(req.Data)), nil
		})
	}

	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register responder: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give the subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the request-reply service client
	client, err := responder.container.GetRequestReplyService("echo-service")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	response, err := client.Call(reqCtx, []byte("hello world"))

	// Verify we got a successful response
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "echo: hello world"
	if string(response.Data) != expected {
		t.Errorf("Expected %q, got %q", expected, string(response.Data))
	}
}

// TestRequestReplyService_ErrorWithCallMsg_OverTCP tests error propagation with CallMsg method over TCP.
func TestRequestReplyService_ErrorWithCallMsg_OverTCP(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithNATSPort(randomPort()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that returns an error
	responder := &testModule{name: "error-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("validate-service", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return nil, errors.New("validation failed: missing required field")
		})
	}

	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register responder: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give the subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the request-reply service client
	client, err := responder.container.GetRequestReplyService("validate-service")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request using CallMsg
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	inputMsg := &mono.Msg{
		Data:   []byte("test data"),
		Header: map[string][]string{"X-Custom-Header": {"test-value"}},
	}

	_, err = client.CallMsg(reqCtx, inputMsg)

	// Verify we got an error
	if err == nil {
		t.Fatal("Expected error from handler, got nil")
	}

	// Verify it's a RemoteError
	if !monoerrors.IsRemoteError(err) {
		t.Errorf("Expected RemoteError, got %T: %v", err, err)
	}

	// Verify error message
	remoteErr, ok := monoerrors.GetRemoteError(err)
	if !ok {
		t.Fatal("Failed to extract RemoteError")
	}

	if remoteErr.Message != "validation failed: missing required field" {
		t.Errorf("Expected specific error message, got %q", remoteErr.Message)
	}
}

// TestRequestReplyService_ErrorDoesNotTimeout_OverTCP tests that errors are returned
// quickly instead of waiting for timeout over TCP connections.
func TestRequestReplyService_ErrorDoesNotTimeout_OverTCP(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithNATSPort(randomPort()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that returns an error immediately
	responder := &testModule{name: "fast-error-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("fast-fail", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return nil, errors.New("immediate failure")
		})
	}

	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register responder: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give the subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the request-reply service client
	client, err := responder.container.GetRequestReplyService("fast-fail")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request with a long timeout - if error propagation works,
	// we should get the error back quickly (not wait for timeout)
	reqCtx, reqCancel := context.WithTimeout(ctx, 30*time.Second)
	defer reqCancel()

	start := time.Now()
	_, err = client.Call(reqCtx, []byte("test"))
	elapsed := time.Since(start)

	// Verify we got an error
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Verify error was returned quickly (much less than timeout)
	if elapsed > 5*time.Second {
		t.Errorf("Error took too long to propagate: %v (should be < 5s)", elapsed)
	}

	// Verify it's a RemoteError (not a timeout error)
	if !monoerrors.IsRemoteError(err) {
		t.Errorf("Expected RemoteError, got %T: %v", err, err)
	}

	// Ensure it's NOT a timeout error
	if monoerrors.IsTimeoutError(err) {
		t.Error("Error should be RemoteError, not TimeoutError")
	}
}
