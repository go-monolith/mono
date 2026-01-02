package types

import (
	"errors"
	"testing"
	"time"
)

// =============================================================================
// Mock Types for Msg Acknowledgment Methods
// =============================================================================

// mockAcker implements the acker interface for testing Msg.Ack()
type mockAcker struct {
	ackCalled bool
	ackErr    error
}

func (m *mockAcker) Ack() error {
	m.ackCalled = true
	return m.ackErr
}

// mockNaker implements the naker interface for testing Msg.Nak()
type mockNaker struct {
	nakCalled bool
	nakErr    error
}

func (m *mockNaker) Nak() error {
	m.nakCalled = true
	return m.nakErr
}

// mockNakDelayer implements the nakDelayer interface for testing Msg.NakWithDelay()
type mockNakDelayer struct {
	nakWithDelayCalled bool
	capturedDelay      time.Duration
	nakWithDelayErr    error
}

func (m *mockNakDelayer) NakWithDelay(delay time.Duration) error {
	m.nakWithDelayCalled = true
	m.capturedDelay = delay
	return m.nakWithDelayErr
}

// mockTermer implements the termer interface for testing Msg.Term()
type mockTermer struct {
	termCalled bool
	termErr    error
}

func (m *mockTermer) Term() error {
	m.termCalled = true
	return m.termErr
}

// mockInProgresser implements the inProgresser interface for testing Msg.InProgress()
type mockInProgresser struct {
	inProgressCalled bool
	inProgressErr    error
}

func (m *mockInProgresser) InProgress() error {
	m.inProgressCalled = true
	return m.inProgressErr
}

// nonAckableNatsMsg is a type that doesn't implement any ack interfaces
type nonAckableNatsMsg struct{}

// =============================================================================
// Msg.Ack() Tests
// =============================================================================

func TestMsg_Ack(t *testing.T) {
	tests := []struct {
		name        string
		natsMsg     any
		expectError bool
		expectAck   bool
	}{
		{
			name:        "nil NatsMsg returns nil (no-op)",
			natsMsg:     nil,
			expectError: false,
			expectAck:   false,
		},
		{
			name:        "valid acker interface - success",
			natsMsg:     &mockAcker{},
			expectError: false,
			expectAck:   true,
		},
		{
			name:        "valid acker interface - returns error",
			natsMsg:     &mockAcker{ackErr: errors.New("ack failed")},
			expectError: true,
			expectAck:   true,
		},
		{
			name:        "non-acker NatsMsg returns nil (no-op)",
			natsMsg:     &nonAckableNatsMsg{},
			expectError: false,
			expectAck:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Msg{
				Subject: "test.subject",
				Data:    []byte("test data"),
				NatsMsg: tt.natsMsg,
			}

			err := msg.Ack()

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.expectAck {
				if acker, ok := tt.natsMsg.(*mockAcker); ok {
					if !acker.ackCalled {
						t.Error("expected Ack() to be called on acker")
					}
				}
			}
		})
	}
}

// =============================================================================
// Msg.Nak() Tests
// =============================================================================

func TestMsg_Nak(t *testing.T) {
	tests := []struct {
		name        string
		natsMsg     any
		expectError bool
		expectNak   bool
	}{
		{
			name:        "nil NatsMsg returns nil (no-op)",
			natsMsg:     nil,
			expectError: false,
			expectNak:   false,
		},
		{
			name:        "valid naker interface - success",
			natsMsg:     &mockNaker{},
			expectError: false,
			expectNak:   true,
		},
		{
			name:        "valid naker interface - returns error",
			natsMsg:     &mockNaker{nakErr: errors.New("nak failed")},
			expectError: true,
			expectNak:   true,
		},
		{
			name:        "non-naker NatsMsg returns nil (no-op)",
			natsMsg:     &nonAckableNatsMsg{},
			expectError: false,
			expectNak:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Msg{
				Subject: "test.subject",
				Data:    []byte("test data"),
				NatsMsg: tt.natsMsg,
			}

			err := msg.Nak()

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.expectNak {
				if naker, ok := tt.natsMsg.(*mockNaker); ok {
					if !naker.nakCalled {
						t.Error("expected Nak() to be called on naker")
					}
				}
			}
		})
	}
}

// =============================================================================
// Msg.NakWithDelay() Tests
// =============================================================================

func TestMsg_NakWithDelay(t *testing.T) {
	tests := []struct {
		name               string
		natsMsg            any
		delay              time.Duration
		expectError        bool
		expectNakWithDelay bool
	}{
		{
			name:               "nil NatsMsg returns nil (no-op)",
			natsMsg:            nil,
			delay:              5 * time.Second,
			expectError:        false,
			expectNakWithDelay: false,
		},
		{
			name:               "valid nakDelayer interface - success",
			natsMsg:            &mockNakDelayer{},
			delay:              10 * time.Second,
			expectError:        false,
			expectNakWithDelay: true,
		},
		{
			name:               "valid nakDelayer interface - returns error",
			natsMsg:            &mockNakDelayer{nakWithDelayErr: errors.New("nak with delay failed")},
			delay:              5 * time.Second,
			expectError:        true,
			expectNakWithDelay: true,
		},
		{
			name:               "non-nakDelayer NatsMsg returns nil (no-op)",
			natsMsg:            &nonAckableNatsMsg{},
			delay:              5 * time.Second,
			expectError:        false,
			expectNakWithDelay: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Msg{
				Subject: "test.subject",
				Data:    []byte("test data"),
				NatsMsg: tt.natsMsg,
			}

			err := msg.NakWithDelay(tt.delay)

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.expectNakWithDelay {
				if nakDelayer, ok := tt.natsMsg.(*mockNakDelayer); ok {
					if !nakDelayer.nakWithDelayCalled {
						t.Error("expected NakWithDelay() to be called on nakDelayer")
					}
					if nakDelayer.capturedDelay != tt.delay {
						t.Errorf("expected delay %v, got %v", tt.delay, nakDelayer.capturedDelay)
					}
				}
			}
		})
	}
}

// =============================================================================
// Msg.Term() Tests
// =============================================================================

func TestMsg_Term(t *testing.T) {
	tests := []struct {
		name        string
		natsMsg     any
		expectError bool
		expectTerm  bool
	}{
		{
			name:        "nil NatsMsg returns nil (no-op)",
			natsMsg:     nil,
			expectError: false,
			expectTerm:  false,
		},
		{
			name:        "valid termer interface - success",
			natsMsg:     &mockTermer{},
			expectError: false,
			expectTerm:  true,
		},
		{
			name:        "valid termer interface - returns error",
			natsMsg:     &mockTermer{termErr: errors.New("term failed")},
			expectError: true,
			expectTerm:  true,
		},
		{
			name:        "non-termer NatsMsg returns nil (no-op)",
			natsMsg:     &nonAckableNatsMsg{},
			expectError: false,
			expectTerm:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Msg{
				Subject: "test.subject",
				Data:    []byte("test data"),
				NatsMsg: tt.natsMsg,
			}

			err := msg.Term()

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.expectTerm {
				if termer, ok := tt.natsMsg.(*mockTermer); ok {
					if !termer.termCalled {
						t.Error("expected Term() to be called on termer")
					}
				}
			}
		})
	}
}

// =============================================================================
// Msg.InProgress() Tests
// =============================================================================

func TestMsg_InProgress(t *testing.T) {
	tests := []struct {
		name             string
		natsMsg          any
		expectError      bool
		expectInProgress bool
	}{
		{
			name:             "nil NatsMsg returns nil (no-op)",
			natsMsg:          nil,
			expectError:      false,
			expectInProgress: false,
		},
		{
			name:             "valid inProgresser interface - success",
			natsMsg:          &mockInProgresser{},
			expectError:      false,
			expectInProgress: true,
		},
		{
			name:             "valid inProgresser interface - returns error",
			natsMsg:          &mockInProgresser{inProgressErr: errors.New("in progress failed")},
			expectError:      true,
			expectInProgress: true,
		},
		{
			name:             "non-inProgresser NatsMsg returns nil (no-op)",
			natsMsg:          &nonAckableNatsMsg{},
			expectError:      false,
			expectInProgress: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Msg{
				Subject: "test.subject",
				Data:    []byte("test data"),
				NatsMsg: tt.natsMsg,
			}

			err := msg.InProgress()

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.expectInProgress {
				if inProgresser, ok := tt.natsMsg.(*mockInProgresser); ok {
					if !inProgresser.inProgressCalled {
						t.Error("expected InProgress() to be called on inProgresser")
					}
				}
			}
		})
	}
}
