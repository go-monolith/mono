package container

import (
	"context"
	"errors"
	"testing"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

func noopStreamHandler(_ context.Context, _ []*types.Msg) error { return nil }

// registerJetStreamServices registers one stream consumer and one cron service
// and returns their errors.
func registerJetStreamServices(c *serviceContainer) (streamErr, cronErr error) {
	streamErr = c.RegisterStreamConsumerService("orders",
		types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "ORDERS"}}, noopStreamHandler)
	cronErr = c.RegisterCronService("rollup",
		types.CronServiceConfig{Schedule: "@every 1m"}, noopCronHandler)
	return streamErr, cronErr
}

func TestJetStreamRequiredServices_FailWhenDisabled(t *testing.T) {
	c := newBoundCronContainer(t, "reports")
	c.SetJetStreamEnabled(false)

	streamErr, cronErr := registerJetStreamServices(c)
	for name, err := range map[string]error{"stream consumer": streamErr, "cron": cronErr} {
		if !errors.Is(err, monoerrors.ErrJetStreamNotEnabled) {
			t.Errorf("%s: err = %v, want ErrJetStreamNotEnabled", name, err)
		}
	}
	if c.Has("orders") || c.Has("rollup") {
		t.Error("a rejected service was stored")
	}

	// Services that do not need JetStream are unaffected.
	if err := c.RegisterRequestReplyService("lookup",
		func(_ context.Context, _ *types.Msg) ([]byte, error) { return nil, nil }); err != nil {
		t.Errorf("request-reply registration failed: %v", err)
	}
}

func TestJetStreamRequiredServices_SucceedWhenEnabledOrUnknown(t *testing.T) {
	for name, set := range map[string]func(*serviceContainer){
		"enabled": func(c *serviceContainer) { c.SetJetStreamEnabled(true) },
		"unknown": func(*serviceContainer) {},
	} {
		t.Run(name, func(t *testing.T) {
			c := newBoundCronContainer(t, "reports")
			set(c)
			streamErr, cronErr := registerJetStreamServices(c)
			if streamErr != nil || cronErr != nil {
				t.Fatalf("stream consumer err = %v, cron err = %v", streamErr, cronErr)
			}
		})
	}
}

func TestServiceContainer_ImplementsJetStreamAvailabilitySetter(t *testing.T) {
	if _, ok := NewServiceContainer(&mockLogger{}).(JetStreamAvailabilitySetter); !ok {
		t.Fatal("serviceContainer does not implement JetStreamAvailabilitySetter")
	}
}
