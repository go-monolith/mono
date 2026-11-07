package mono_test

import (
	"testing"

	"github.com/go-monolith/mono"
)

func TestWithStrictModuleBoundaries(t *testing.T) {
	t.Run("default is off", func(t *testing.T) {
		if mono.DefaultConfig().StrictModuleBoundaries {
			t.Fatal("StrictModuleBoundaries must default to false")
		}
	})
	t.Run("enables strict mode", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		if err := mono.WithStrictModuleBoundaries()(cfg); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !cfg.StrictModuleBoundaries {
			t.Fatal("StrictModuleBoundaries not set")
		}
	})
}
