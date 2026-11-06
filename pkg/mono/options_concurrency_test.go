package mono_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

func TestWithShutdownDrainTimeout(t *testing.T) {
	t.Run("default is derived from the shutdown timeout", func(t *testing.T) {
		if got := mono.DefaultConfig().ShutdownDrainTimeout; got != 0 {
			t.Fatalf("DefaultConfig().ShutdownDrainTimeout = %s, want 0 (derived at NewMonoApplication)", got)
		}
		if types.DefaultShutdownDrainTimeout != 30*time.Second {
			t.Fatalf("DefaultShutdownDrainTimeout = %s, want 30s", types.DefaultShutdownDrainTimeout)
		}
		// A short shutdown timeout alone must remain a valid configuration
		// (examples and the quickstart use 5s/10s): the drain follows it.
		app, err := mono.NewMonoApplication(
			mono.WithNATSDontListen(), mono.WithNATSInProcessConn(),
			mono.WithShutdownTimeout(5*time.Second),
		)
		if err != nil {
			t.Fatalf("WithShutdownTimeout(5s) alone must be valid, got %v", err)
		}
		_ = app.Stop(context.Background())
	})
	t.Run("sets the value", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		if err := mono.WithShutdownDrainTimeout(10 * time.Second)(cfg); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ShutdownDrainTimeout != 10*time.Second {
			t.Fatalf("ShutdownDrainTimeout = %s, want 10s", cfg.ShutdownDrainTimeout)
		}
	})
	t.Run("rejects non-positive values", func(t *testing.T) {
		for _, d := range []time.Duration{0, -time.Second} {
			err := mono.WithShutdownDrainTimeout(d)(mono.DefaultConfig())
			if !errors.Is(err, monoerrors.ErrInvalidConfiguration) {
				t.Fatalf("WithShutdownDrainTimeout(%s): got %v, want ErrInvalidConfiguration", d, err)
			}
		}
	})
	t.Run("must not exceed the shutdown timeout", func(t *testing.T) {
		_, err := mono.NewMonoApplication(
			mono.WithShutdownTimeout(5*time.Second),
			mono.WithShutdownDrainTimeout(6*time.Second),
		)
		if !errors.Is(err, monoerrors.ErrInvalidConfiguration) {
			t.Fatalf("got %v, want ErrInvalidConfiguration", err)
		}
		if err == nil || !strings.Contains(err.Error(), "must not exceed the shutdown timeout") {
			t.Fatalf("error should explain the cross-field rule, got %v", err)
		}
	})
}

func TestWithDefaultServiceConcurrency(t *testing.T) {
	t.Run("default is zero (built-in GOMAXPROCS*4)", func(t *testing.T) {
		if got := mono.DefaultConfig().DefaultServiceConcurrency; got != 0 {
			t.Fatalf("DefaultServiceConcurrency = %d, want 0", got)
		}
	})

	t.Run("sets the value", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		if err := mono.WithDefaultServiceConcurrency(1)(cfg); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.DefaultServiceConcurrency != 1 {
			t.Fatalf("DefaultServiceConcurrency = %d, want 1", cfg.DefaultServiceConcurrency)
		}
	})

	t.Run("rejects values below 1", func(t *testing.T) {
		for _, n := range []int{0, -1} {
			cfg := mono.DefaultConfig()
			err := mono.WithDefaultServiceConcurrency(n)(cfg)
			if err == nil {
				t.Fatalf("WithDefaultServiceConcurrency(%d): expected error", n)
			}
			if !errors.Is(err, monoerrors.ErrInvalidConfiguration) {
				t.Fatalf("WithDefaultServiceConcurrency(%d): got %v, want ErrInvalidConfiguration", n, err)
			}
		}
	})

	t.Run("root aliases are wired to pkg/types", func(t *testing.T) {
		var opts mono.ServiceOptions
		for _, opt := range []mono.ServiceOption{mono.WithMaxConcurrency(4), mono.WithMaxPending(9)} {
			opt(&opts)
		}
		if opts.MaxConcurrency != 4 || opts.MaxPending != 9 {
			t.Fatalf("ServiceOptions = %+v, want {4 9}", opts)
		}
		eventOpts := mono.EventConsumerOptions{QueueGroup: "g", ServiceOptions: opts}
		if eventOpts.MaxConcurrency != 4 {
			t.Fatalf("EventConsumerOptions does not embed ServiceOptions: %+v", eventOpts)
		}
	})
}
