package mono_test

import (
	"errors"
	"testing"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
)

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
