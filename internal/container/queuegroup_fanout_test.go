package container_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/container"
	"github.com/go-monolith/mono/internal/logger"
	"github.com/go-monolith/mono/monotest"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// FanOutModule is the module the fan-out tests bind their container to.
type FanOutModule struct{}

// Name returns the module name.
func (FanOutModule) Name() string { return "orders" }

// Start does nothing.
func (FanOutModule) Start(context.Context) error { return nil }

// Stop does nothing.
func (FanOutModule) Stop(context.Context) error { return nil }

// Answer is how a stand-in queue group answers a send: an ACK, a rejection
// of the given framework error type, or nothing.
type Answer struct {
	ErrorType string        // "" = ACK
	Silent    bool          // send no answer
	Delay     time.Duration // wait before answering
}

// FanOutClient registers a queue group service with one pair per answer on
// a container over a monotest bus, subscribes a stand-in for each group that
// answers the way its answer says (the container's own handlers are not
// dispatched), and returns the service's client.
func FanOutClient(t *testing.T, answers ...Answer) types.QueueGroupServiceClient {
	t.Helper()
	bus := monotest.NewEventBus()
	t.Cleanup(bus.Close) // waits for stand-ins still answering
	c := container.NewServiceContainer(logger.NewDefaultLogger())
	if err := c.BindModule(FanOutModule{}); err != nil {
		t.Fatal(err)
	}
	c.SetEventBus(bus)
	pairs := make([]types.QGHP, 0, len(answers))
	for i, answer := range answers {
		group := "group-" + string(rune('a'+i))
		pairs = append(pairs, types.QGHP{QueueGroup: group, Handler: func(context.Context, *types.Msg) error { return nil }})
		if _, err := bus.QueueSubscribe("services.orders.placed", group, func(_ context.Context, msg *types.Msg) {
			if answer.Silent {
				return
			}
			time.Sleep(answer.Delay)
			reply := &types.Msg{Subject: msg.Reply}
			if answer.ErrorType != "" {
				reply.Header = types.Header{
					types.HeaderError:        []string{"true"},
					types.HeaderErrorType:    []string{answer.ErrorType},
					types.HeaderErrorMessage: []string{"rejected by " + group},
				}
			}
			// A stand-in still answering when the test ends finds the bus
			// closed; its answer is not waited for any more.
			if err := bus.PublishMsg(reply); err != nil && !errors.Is(err, monotest.ErrEventBusClosed) {
				t.Errorf("answer of %s: %v", group, err)
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.RegisterQueueGroupService("placed", pairs...); err != nil {
		t.Fatal(err)
	}
	client, err := c.GetQueueGroupService("placed")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// A send to a service with several queue groups is delivered when any group
// accepted it, whichever group answered first, and fails only when every
// group rejected it or no group exists (issue #89).
func TestQueueGroupFanOutSend(t *testing.T) {
	tests := []struct {
		name    string
		answers []Answer
		want    error // nil, a sentinel, or context.DeadlineExceeded
	}{
		{"all groups accept", []Answer{{}, {}}, nil},
		{"rejection first, then an ACK", []Answer{{ErrorType: types.ErrorTypeOverloaded}, {Delay: 50 * time.Millisecond}}, nil},
		{"ACK first, then a rejection", []Answer{{}, {ErrorType: types.ErrorTypeOverloaded, Delay: 50 * time.Millisecond}}, nil},
		{"every group rejects", []Answer{{ErrorType: types.ErrorTypeOverloaded}, {ErrorType: types.ErrorTypeOverloaded}}, monoerrors.ErrServiceOverloaded},
		{"one group shutting down, the other silent", []Answer{{ErrorType: types.ErrorTypeShuttingDown}, {Silent: true}}, monoerrors.ErrServiceUnavailable},
		{"no group answers", []Answer{{Silent: true}, {Silent: true}}, context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := FanOutClient(t, tt.answers...)
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			err := client.Send(ctx, []byte("order"))
			switch {
			case tt.want == nil && err != nil:
				t.Fatalf("Send = %v, want nil", err)
			case tt.want != nil && !errors.Is(err, tt.want):
				t.Fatalf("Send = %v, want %v", err, tt.want)
			}
		})
	}
}

// A rejection is returned once the other groups have not answered within
// FanOutRejectionGrace (no instance of them online), not when the context
// ends.
func TestQueueGroupFanOutSendRejectionGrace(t *testing.T) {
	client := FanOutClient(t, Answer{ErrorType: types.ErrorTypeOverloaded}, Answer{Silent: true})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	err := client.Send(ctx, []byte("order"))
	if !errors.Is(err, monoerrors.ErrServiceOverloaded) {
		t.Fatalf("Send = %v, want ErrServiceOverloaded", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("Send waited %v, want about %v", waited, container.FanOutRejectionGrace)
	}
}

// A send to a service with several queue groups that no instance subscribes
// to fails with ErrServiceUnavailable at once, as for one group.
func TestQueueGroupFanOutSendNoResponders(t *testing.T) {
	bus := monotest.NewEventBus()
	c := container.NewServiceContainer(logger.NewDefaultLogger())
	if err := c.BindModule(FanOutModule{}); err != nil {
		t.Fatal(err)
	}
	c.SetEventBus(bus)
	handler := func(context.Context, *types.Msg) error { return nil }
	if err := c.RegisterQueueGroupService("placed",
		types.QGHP{QueueGroup: "a", Handler: handler},
		types.QGHP{QueueGroup: "b", Handler: handler},
	); err != nil {
		t.Fatal(err)
	}
	client, err := c.GetQueueGroupService("placed")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = client.SendMsg(context.Background(), &types.Msg{Data: []byte("order")})
	if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
		t.Fatalf("SendMsg = %v, want ErrServiceUnavailable", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("SendMsg waited %v, want an immediate answer", waited)
	}
}
