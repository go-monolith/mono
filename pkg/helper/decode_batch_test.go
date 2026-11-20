package helper_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// BatchOf builds a batch of messages with the given payloads.
func BatchOf(payloads ...string) []*types.Msg {
	msgs := make([]*types.Msg, len(payloads))
	for i, p := range payloads {
		msgs[i] = &types.Msg{Subject: "services.orders.process", Data: []byte(p)}
	}
	return msgs
}

func DecodeTestRequest(msg *types.Msg) (testRequest, error) {
	var req testRequest
	err := json.Unmarshal(msg.Data, &req)
	return req, err
}

func TestDecodeBatch(t *testing.T) {
	tests := []struct {
		name        string
		payloads    []string
		wantNames   []string
		wantDecoded []int // indexes into the batch
		wantErr     bool
	}{
		{name: "empty batch"},
		{
			name:        "all decodable",
			payloads:    []string{`{"name":"a"}`, `{"name":"b"}`},
			wantNames:   []string{"a", "b"},
			wantDecoded: []int{0, 1},
		},
		{
			name:        "one undecodable in the middle",
			payloads:    []string{`{"name":"a"}`, `not json`, `{"name":"c"}`},
			wantNames:   []string{"a", "c"},
			wantDecoded: []int{0, 2},
			wantErr:     true,
		},
		{
			name:     "none decodable",
			payloads: []string{`not json`, `{`},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgs := BatchOf(tt.payloads...)
			payloads, decoded, err := helper.DecodeBatch(msgs, DecodeTestRequest)

			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, want error %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if !errors.Is(err, helper.ErrUndecodableMessage) {
					t.Errorf("error %v does not wrap ErrUndecodableMessage", err)
				}
				var syntaxErr *json.SyntaxError
				if !errors.As(err, &syntaxErr) {
					t.Errorf("error %v does not wrap the first decode error", err)
				}
			}
			if len(payloads) != len(tt.wantNames) || len(decoded) != len(tt.wantDecoded) {
				t.Fatalf("got %d payloads and %d messages, want %d and %d", len(payloads), len(decoded), len(tt.wantNames), len(tt.wantDecoded))
			}
			for i, name := range tt.wantNames {
				if payloads[i].Name != name {
					t.Errorf("payload %d = %q, want %q", i, payloads[i].Name, name)
				}
				if decoded[i] != msgs[tt.wantDecoded[i]] {
					t.Errorf("message %d is not the one payload %d was decoded from", i, i)
				}
			}
		})
	}
}

// One undecodable message must not cost the others of its batch: the handler
// gets them, index-aligned with their payloads, and the error reports the
// message left out.
func TestRegisterTypedStreamConsumerService_UndecodableMessageLeftOut(t *testing.T) {
	handlerErr := errors.New("handler failed")
	tests := []struct {
		name        string
		payloads    []string
		handlerErr  error
		wantCalls   int
		wantHandled []string
	}{
		{name: "the rest of the batch is handled", payloads: []string{`{"name":"a"}`, `not json`, `{"name":"c"}`}, wantCalls: 1, wantHandled: []string{"a", "c"}},
		{name: "handler is not called with an empty batch", payloads: []string{`not json`}, wantCalls: 0},
		{name: "handler error is kept beside it", payloads: []string{`{"name":"a"}`, `not json`}, handlerErr: handlerErr, wantCalls: 1, wantHandled: []string{"a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockServiceContainer{}
			calls := 0
			var handled []string
			handler := func(_ context.Context, reqs []testRequest, msgs []*types.Msg) error {
				calls++
				if len(reqs) != len(msgs) {
					t.Errorf("%d payloads for %d messages", len(reqs), len(msgs))
				}
				for i, req := range reqs {
					var fromMsg testRequest
					if err := json.Unmarshal(msgs[i].Data, &fromMsg); err != nil || fromMsg.Name != req.Name {
						t.Errorf("payload %d (%q) does not belong to message %d (%s)", i, req.Name, i, msgs[i].Data)
					}
					handled = append(handled, req.Name)
				}
				return tt.handlerErr
			}
			config := types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "test-stream"}}
			if err := helper.RegisterTypedStreamConsumerService(mock, "orders", config, json.Unmarshal, handler); err != nil {
				t.Fatalf("registration failed: %v", err)
			}

			err := mock.streamConsumerHandler(context.Background(), BatchOf(tt.payloads...))

			if !errors.Is(err, helper.ErrUndecodableMessage) {
				t.Errorf("error %v does not wrap ErrUndecodableMessage", err)
			}
			if tt.handlerErr != nil && !errors.Is(err, tt.handlerErr) {
				t.Errorf("error %v does not wrap the handler error", err)
			}
			if calls != tt.wantCalls {
				t.Errorf("handler called %d times, want %d", calls, tt.wantCalls)
			}
			if len(handled) != len(tt.wantHandled) {
				t.Fatalf("handled %v, want %v", handled, tt.wantHandled)
			}
			for i := range handled {
				if handled[i] != tt.wantHandled[i] {
					t.Errorf("handled %v, want %v", handled, tt.wantHandled)
				}
			}
		})
	}
}

func TestRegisterTypedEventStreamConsumer_UndecodableMessageLeftOut(t *testing.T) {
	registry := newMockEventRegistry()
	def := helper.EventDefinition[testRequest]("order", "OrderCreated", "v1")
	var handled []string
	handler := func(_ context.Context, events []testRequest, msgs []*types.Msg) error {
		if len(events) != len(msgs) {
			t.Errorf("%d events for %d messages", len(events), len(msgs))
		}
		for _, event := range events {
			handled = append(handled, event.Name)
		}
		return nil
	}
	config := types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "test-stream"}}
	if err := helper.RegisterTypedEventStreamConsumer(registry, def, config, handler, &mockModule{}); err != nil {
		t.Fatalf("registration failed: %v", err)
	}
	wrapped := registry.registeredStreams[def.ToBase().Subject]

	err := wrapped(context.Background(), BatchOf(`not json`, `{"name":"a"}`, `{"name":"b"}`))
	if !errors.Is(err, helper.ErrUndecodableMessage) {
		t.Errorf("error %v does not wrap ErrUndecodableMessage", err)
	}
	if len(handled) != 2 || handled[0] != "a" || handled[1] != "b" {
		t.Errorf("handled %v, want [a b]", handled)
	}

	handled = nil
	if err := wrapped(context.Background(), BatchOf(`not json`)); !errors.Is(err, helper.ErrUndecodableMessage) {
		t.Errorf("error %v does not wrap ErrUndecodableMessage", err)
	}
	if len(handled) != 0 {
		t.Errorf("handler was called for a batch without a decodable message: %v", handled)
	}
}
